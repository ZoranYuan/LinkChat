package ws

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	eventport "IM_backend/core/application/ports/events"
	"IM_backend/core/infra/metrics"
)

var ErrGatewayClosed = errors.New("实时通道网关已关闭")

type Gateway struct {
	mu                sync.RWMutex
	connections       map[string]*Connection
	userConnections   map[string]map[string]struct{} //  用户当前有哪些在线连接
	groupConnections   map[string]map[string]struct{} //  群聊当前有哪些在线成员连接
	connectionGroups   map[string]map[string]struct{} //  连接所属哪些在线群聊，断线时反向清理
	networkCtx        context.Context
	cancelNetworkCtx  context.CancelFunc
	businessCtx       context.Context
	cancelBusinessCtx context.CancelFunc
	dispatchWg        sync.WaitGroup
	closing           bool
}

func NewGateway(serviceCtx context.Context) *Gateway {
	networkCtx, cancelNetworkCtx := context.WithCancel(serviceCtx)
	businessCtx, cancelBusinessCtx := context.WithCancel(serviceCtx)
	return &Gateway{
		networkCtx:        networkCtx,
		cancelNetworkCtx:  cancelNetworkCtx,
		businessCtx:       businessCtx,
		cancelBusinessCtx: cancelBusinessCtx,
		connections:       make(map[string]*Connection),
		userConnections:   make(map[string]map[string]struct{}),
		groupConnections:   make(map[string]map[string]struct{}),
		connectionGroups:   make(map[string]map[string]struct{}),
	}
}

func (g *Gateway) NetworkContext() context.Context {
	return g.networkCtx
}

func (g *Gateway) SweepHiddenConnections(ctx context.Context, interval, hiddenTimeout time.Duration) {
	if interval <= 0 || hiddenTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				for _, connection := range g.snapshot() {
					if connection.CloseIfPageHiddenExpired(now, hiddenTimeout) {
						log.Printf("回收页面长期隐藏的 WebSocket 连接：user=%s connection=%s hidden_timeout=%s",
							connection.UserID(), connection.ConnectionID(), hiddenTimeout)
					}
				}
			}
		}
	}()
}

// 服务器退出
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	g.closing = true
	g.mu.Unlock()

	// 网络先停止接收，业务分发正常排空后取消；超时则提前通知业务中断。
	g.cancelNetworkCtx()
	defer g.cancelBusinessCtx()

	done := make(chan struct{})
	go func() {
		g.dispatchWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterAndStart 在同一把锁内检查停机状态并计数，只有入站分发结束才扣减计数。
func (g *Gateway) RegisterAndStart(
	connection *Connection,
	handler MessageHandler,
	onClose func(*Connection),
) error {
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		metrics.WSConnectionsTotal.WithLabelValues("rejected").Inc()
		connection.Close()
		return ErrGatewayClosed
	}

	g.registerLocked(connection)
	g.dispatchWg.Add(1)
	connection.Start(g.businessCtx, handler, onClose, func(_ *Connection) {
		g.dispatchWg.Done()
	})
	g.mu.Unlock()
	return nil
}

func (g *Gateway) registerLocked(connection *Connection) {
	connectionID := connection.ConnectionID()
	if _, exists := g.connections[connectionID]; !exists {
		metrics.WSActiveConnections.Inc()
		metrics.WSConnectionsTotal.WithLabelValues("opened").Inc()
	}
	g.connections[connectionID] = connection
	ids := g.userConnections[connection.UserID()]
	if ids == nil {
		ids = make(map[string]struct{})
		g.userConnections[connection.UserID()] = ids
	}
	ids[connection.ConnectionID()] = struct{}{}
}

func (g *Gateway) Unregister(connection *Connection) {
	g.mu.Lock()
	connectionID := connection.ConnectionID()
	// 只允许当前索引指向的连接清理自身，避免过期清理误删新连接的索引。
	if current := g.connections[connectionID]; current != connection {
		g.mu.Unlock()
		return
	}
	delete(g.connections, connectionID)
	metrics.WSActiveConnections.Dec()
	metrics.WSConnectionsTotal.WithLabelValues("closed").Inc()
	ids := g.userConnections[connection.UserID()]
	delete(ids, connectionID)
	if len(ids) == 0 {
		delete(g.userConnections, connection.UserID())
	}
	for groupID := range g.connectionGroups[connectionID] {
		connections := g.groupConnections[groupID]
		delete(connections, connectionID)
		if len(connections) == 0 {
			delete(g.groupConnections, groupID)
		}
	}
	delete(g.connectionGroups, connectionID)
	g.mu.Unlock()
}

// CloseAuthSession 关闭指定用户认证会话下的全部本机 WebSocket 连接。
func (g *Gateway) CloseAuthSession(userID, sessionID string) int {
	if userID == "" || sessionID == "" {
		return 0
	}
	g.mu.RLock()
	ids := g.userConnections[userID]
	connections := make([]*Connection, 0, len(ids))
	for connectionID := range ids {
		connection := g.connections[connectionID]
		if connection != nil && connection.AuthSessionID() == sessionID {
			connections = append(connections, connection)
		}
	}
	g.mu.RUnlock()

	for _, connection := range connections {
		connection.Close()
	}
	return len(connections)
}

func (g *Gateway) DeliverMessageToUser(userID string, event eventport.MessagePushEvent) error {
	return g.deliverToUser(userID, eventport.TypeMessagePush, messagePushToPB(event))
}

func (g *Gateway) DeliverMessageReadNotice(userID string, event eventport.MessageReadNoticeEvent) error {
	return g.deliverToUser(userID, eventport.TypeMessageReadNotice, messageReadNoticeToPB(event))
}

func (g *Gateway) DeliverFriendRequestNotice(userID string, event eventport.FriendRequestNoticeEvent) error {
	return g.deliverToUser(userID, eventport.TypeFriendRequestNotice, friendRequestNoticeToPB(event))
}

func (g *Gateway) DeliverGroupMessageNotice(groupID string, event eventport.GroupMessageNoticeEvent) error {
	return g.deliverToGroup(groupID, eventport.TypeGroupMessageNotice, groupMessageNoticeToPB(event), "")
}

func (g *Gateway) DeliverMessageToOnlineGroupMembers(groupID string, event eventport.MessagePushEvent, excludeUserID string) error {
	return g.deliverToGroup(groupID, eventport.TypeMessagePush, messagePushToPB(event), excludeUserID)
}

func (g *Gateway) deliverToUser(userID, eventType string, event proto.Message) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	message := Message{Op: eventType, Data: payload}
	for _, connection := range g.connectionsForUser(userID) {
		if err := connection.Enqueue(message); err != nil {
			connection.Close()
			log.Printf("WS 用户推送失败：user=%s connection=%s event=%s error=%v", userID, connection.ConnectionID(), eventType, err)
		}
	}
	return nil
}

func (g *Gateway) BindOnlineGroups(connection *Connection, groupIDs []string) {
	if connection == nil {
		return
	}
	g.mu.Lock()
	if g.closing || g.connections[connection.ConnectionID()] == nil {
		g.mu.Unlock()
		return
	}
	connectionID := connection.ConnectionID()
	groups := g.connectionGroups[connectionID]
	if groups == nil {
		groups = make(map[string]struct{})
		g.connectionGroups[connectionID] = groups
	}
	for _, groupID := range groupIDs {
		if groupID == "" {
			continue
		}
		connections := g.groupConnections[groupID]
		if connections == nil {
			connections = make(map[string]struct{})
			g.groupConnections[groupID] = connections
		}
		if _, exists := connections[connectionID]; !exists {
			connections[connectionID] = struct{}{}
		}
		groups[groupID] = struct{}{}
	}
	g.mu.Unlock()
}

func (g *Gateway) deliverToGroup(groupID, eventType string, event proto.Message, excludeUserID string) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	message := Message{Op: eventType, Data: payload}
	for _, connection := range g.connectionsForGroup(groupID) {
		// 排除发送者自身
		if excludeUserID != "" && connection.UserID() == excludeUserID {
			continue
		}

		if err := connection.Enqueue(message); err != nil {
			// 当前 Connection 投递失败，主动断开，然后让前端重新连接并同步消息。
			connection.Close()

			log.Printf(
				"WS 群聊推送失败：group=%s user=%s connection=%s event=%s error=%v",
				groupID,
				connection.UserID(),
				connection.ConnectionID(),
				eventType,
				err,
			)
			continue
		}
	}
	return nil
}

// OnlineGroupConnectionCount 直接读取当前节点的群聊连接索引。
func (g *Gateway) OnlineGroupConnectionCount(groupID string) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.groupConnections[groupID])
}

func (g *Gateway) connectionsForUser(userID string) []*Connection {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := g.userConnections[userID]
	result := make([]*Connection, 0, len(ids))
	for id := range ids {
		if connection := g.connections[id]; connection != nil {
			result = append(result, connection)
		}
	}
	return result
}

func (g *Gateway) connectionsForGroup(groupID string) []*Connection {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := g.groupConnections[groupID]
	result := make([]*Connection, 0, len(ids))
	for id := range ids {
		if connection := g.connections[id]; connection != nil {
			result = append(result, connection)
		}
	}
	return result
}

func (g *Gateway) snapshot() []*Connection {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]*Connection, 0, len(g.connections))
	for _, connection := range g.connections {
		result = append(result, connection)
	}
	return result
}
