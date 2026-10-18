package websocket

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"IM_backend/internal/shared/diagnostics"
)

var ErrGatewayClosed = errors.New("实时通道网关已关闭")

type Gateway struct {
	mu                sync.RWMutex
	connections       map[string]*Connection
	userConnections   map[string]map[string]struct{} //  用户当前有哪些在线连接
	roomConnections   map[string]map[string]struct{} //  房间当前有哪些在线成员连接
	connectionRooms   map[string]map[string]struct{} //  连接所属哪些在线房间，断线时反向清理
	globalCtx         context.Context
	cancelGlobalCtx   context.CancelFunc
	businessCtx       context.Context
	cancelBusinessCtx context.CancelFunc
	dispatchWg        sync.WaitGroup
	closing           bool
}

func NewGateway(serviceCtx context.Context) *Gateway {
	globalCtx, cancelGlobalCtx := context.WithCancel(serviceCtx)
	businessCtx, cancelBusinessCtx := context.WithCancel(serviceCtx)
	return &Gateway{
		globalCtx:         globalCtx,
		cancelGlobalCtx:   cancelGlobalCtx,
		businessCtx:       businessCtx,
		cancelBusinessCtx: cancelBusinessCtx,
		connections:       make(map[string]*Connection),
		userConnections:   make(map[string]map[string]struct{}),
		roomConnections:   make(map[string]map[string]struct{}),
		connectionRooms:   make(map[string]map[string]struct{}),
	}
}

func (g *Gateway) NetworkContext() context.Context {
	return g.globalCtx
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
	g.cancelGlobalCtx()
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
	pongWait int,
	pingPeriod int,
	writeWait int,
	handler MessageHandler,
	onClose func(*Connection),
) error {
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		connection.Close()
		return ErrGatewayClosed
	}

	g.registerLocked(connection)
	g.dispatchWg.Add(1)
	connection.Start(g.businessCtx, pongWait, pingPeriod, writeWait, handler, onClose, func(_ *Connection) {
		g.dispatchWg.Done()
	})
	g.mu.Unlock()
	return nil
}

func (g *Gateway) registerLocked(connection *Connection) {
	g.connections[connection.ConnectionID()] = connection
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
	ids := g.userConnections[connection.UserID()]
	delete(ids, connectionID)
	if len(ids) == 0 {
		delete(g.userConnections, connection.UserID())
	}
	for roomID := range g.connectionRooms[connectionID] {
		connections := g.roomConnections[roomID]
		if _, exists := connections[connectionID]; exists {
			delete(connections, connectionID)
		}
		if len(connections) == 0 {
			delete(g.roomConnections, roomID)
		}
	}
	delete(g.connectionRooms, connectionID)
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

func (g *Gateway) DeliverToUser(eventType, userID string, payload []byte) error {
	for _, connection := range g.connectionsForUser(userID) {
		if err := connection.PushEvent(eventType, payload); err != nil {
			// 当前 Connection 投递失败，主动断开，然后让前端重新连接并同步消息。
			// 单个 Connection 的失败不能影响同一用户的其他连接。
			connection.Close()

			log.Printf(
				"WS 用户推送失败：user=%s connection=%s event=%s error=%v",
				userID,
				connection.ConnectionID(),
				eventType,
				err,
			)
		}
	}
	return nil
}

func (g *Gateway) BindOnlineRooms(connection *Connection, roomIDs []string) {
	if connection == nil {
		return
	}
	g.mu.Lock()
	if g.closing || g.connections[connection.ConnectionID()] == nil {
		g.mu.Unlock()
		return
	}
	connectionID := connection.ConnectionID()
	rooms := g.connectionRooms[connectionID]
	if rooms == nil {
		rooms = make(map[string]struct{})
		g.connectionRooms[connectionID] = rooms
	}
	for _, roomID := range roomIDs {
		if roomID == "" {
			continue
		}
		connections := g.roomConnections[roomID]
		if connections == nil {
			connections = make(map[string]struct{})
			g.roomConnections[roomID] = connections
		}
		if _, exists := connections[connectionID]; !exists {
			connections[connectionID] = struct{}{}
		}
		rooms[roomID] = struct{}{}
	}
	g.mu.Unlock()
}

func (g *Gateway) BindUserToRoom(userID, roomID string) {
	if userID == "" || roomID == "" {
		return
	}
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		return
	}
	for connectionID := range g.userConnections[userID] {
		connection := g.connections[connectionID]
		if connection == nil {
			continue
		}
		connections := g.roomConnections[roomID]
		if connections == nil {
			connections = make(map[string]struct{})
			g.roomConnections[roomID] = connections
		}
		// 将当前的 connection 加入房间
		if _, exists := connections[connectionID]; !exists {
			connections[connectionID] = struct{}{}
		}
		rooms := g.connectionRooms[connectionID]
		if rooms == nil {
			rooms = make(map[string]struct{})
			g.connectionRooms[connectionID] = rooms
		}
		rooms[roomID] = struct{}{}
	}
	g.mu.Unlock()
}

func (g *Gateway) UnbindUserFromRoom(userID, roomID string) {
	if userID == "" || roomID == "" {
		return
	}
	g.mu.Lock()
	for connectionID := range g.userConnections[userID] {
		connections := g.roomConnections[roomID]
		if _, exists := connections[connectionID]; exists {
			delete(connections, connectionID)
		}
		if len(connections) == 0 {
			delete(g.roomConnections, roomID)
		}
		rooms := g.connectionRooms[connectionID]
		delete(rooms, roomID)
		if len(rooms) == 0 {
			delete(g.connectionRooms, connectionID)
		}
	}
	g.mu.Unlock()
}

func (g *Gateway) DeliverToOnlineRoomMembers(eventType, roomID string, payload []byte, excludeUserID string) error {
	diagnosticEnabled := diagnostics.Enabled()
	localDelivered := 0
	localFailed := 0
	for _, connection := range g.connectionsForRoom(roomID) {
		// 排除发送者自身
		if excludeUserID != "" && connection.UserID() == excludeUserID {
			continue
		}

		if err := connection.PushEvent(eventType, payload); err != nil {
			if diagnosticEnabled {
				localFailed++
			}
			// 当前 Connection 投递失败，主动断开，然后让前端重新连接并同步消息。
			connection.Close()

			log.Printf(
				"WS 房间推送失败：room=%s user=%s connection=%s event=%s error=%v",
				roomID,
				connection.UserID(),
				connection.ConnectionID(),
				eventType,
				err,
			)
			continue
		}
		if diagnosticEnabled {
			localDelivered++
		}
	}
	if diagnosticEnabled {
		messageID, seq := realtimeMessageIdentity(eventType, payload)
		diagnostics.Logf("stage=ws_room_enqueue room_id=%s event_type=%s message_id=%s seq=%d delivered_connections=%d failed_connections=%d outcome=completed",
			roomID, eventType, messageID, seq, localDelivered, localFailed)
	}
	return nil
}

// OnlineRoomConnectionCount 直接读取当前节点的房间连接索引。
func (g *Gateway) OnlineRoomConnectionCount(ctx context.Context, roomID string) (int, error) {
	if g == nil || roomID == "" {
		return 0, errors.New("房间在线连接查询参数无效")
	}
	if ctx != nil && ctx.Err() != nil {
		return 0, ctx.Err()
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.roomConnections[roomID]), nil
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

func (g *Gateway) connectionsForRoom(roomID string) []*Connection {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := g.roomConnections[roomID]
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
