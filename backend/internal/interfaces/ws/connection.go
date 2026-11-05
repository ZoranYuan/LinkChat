package ws

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	gorilla "github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"IM_backend/configs"
	wspb "IM_backend/internal/interfaces/ws/pb"
)

var (
	ErrConnectionClosed          = errors.New("实时通道连接已关闭")
	ErrOutboundQueueFull         = errors.New("实时通道发送队列已满")
	ErrInvalidConnectionIdentity = errors.New("非法的连接身份")
)

type Platform string

const PlatfromWeb Platform = "web"

// ConnectionIdentity 区分认证登录会话与具体的 WebSocket 连接。
type ConnectionIdentity struct {
	Platform      Platform
	UserID        string
	DeviceID      string
	AuthSessionID string
	ConnectionID  string
}

// NewConnectionIdentity 为每次连接生成独立 ID；AuthSessionID 仍来自 JWT 的 sid。
func NewConnectionIdentity(userID, deviceID, platform, authSessionID string) (ConnectionIdentity, error) {
	identity := ConnectionIdentity{
		UserID:        strings.TrimSpace(userID),
		DeviceID:      strings.TrimSpace(deviceID),
		Platform:      Platform(strings.ToLower(strings.TrimSpace(platform))),
		AuthSessionID: strings.TrimSpace(authSessionID),
		ConnectionID:  uuid.NewString(),
	}

	if identity.UserID == "" || identity.DeviceID == "" || identity.AuthSessionID == "" {
		return ConnectionIdentity{}, ErrInvalidConnectionIdentity
	}

	switch identity.Platform {
	case PlatfromWeb:
		return identity, nil
	default:
		return ConnectionIdentity{}, ErrInvalidConnectionIdentity
	}
}

type Message struct {
	Op   string
	Data []byte
}

type MessageHandler func(context.Context, *Connection, string, []byte)

type Connection struct {
	conn *gorilla.Conn
	ctx  context.Context // 网络读写随连接关闭而取消。

	mu sync.RWMutex

	identity ConnectionIdentity

	config         configs.WebSocketConfig
	outbound       chan Message
	outboundClosed bool

	inbound chan Message

	cancel        context.CancelFunc
	closeOnce     sync.Once
	visibilityMu  sync.RWMutex
	hiddenSince   time.Time
	hiddenReclaim bool
}

func NewConnection(networkCtx context.Context, conn *gorilla.Conn, identity ConnectionIdentity, config configs.WebSocketConfig) *Connection {
	ctx, cancel := context.WithCancel(networkCtx)
	return &Connection{conn: conn, ctx: ctx, cancel: cancel, identity: identity, config: config,
		outbound: make(chan Message, config.MaxMessageSendBufferSize), inbound: make(chan Message, config.MaxMessageSendBufferSize)}
}

func (s *Connection) marshalBatch(batch *MessageBatch) ([]byte, error) {
	frames := make([]*wspb.WsFrame, 0, len(batch.Messages))

	for _, message := range batch.Messages {
		frames = append(frames, &wspb.WsFrame{
			Op:   message.Op,
			Data: message.Data,
		})
	}

	batchData, err := proto.Marshal(&wspb.EventBatch{
		Frames: frames,
	})

	if err != nil {
		return nil, fmt.Errorf("序列化消息批次失败: %w", err)
	}

	payload, err := proto.Marshal(&wspb.WsFrame{
		Op:   messaging.TypeEventBatch,
		Data: batchData,
	})

	if err != nil {
		return nil, fmt.Errorf("序列化批量消息帧失败: %w", err)
	}

	return payload, nil
}

func (s *Connection) sendBatch(batch *MessageBatch) error {
	payload, err := s.marshalBatch(batch)
	if err != nil {
		return err
	}
	return s.write(gorilla.BinaryMessage, payload)
}

func (s *Connection) Start(businessCtx context.Context, handler MessageHandler, onClose, onDispatchDone func(*Connection)) {
	go func() {
		if onDispatchDone != nil {
			defer onDispatchDone(s)
		}
		s.dispatchLoop(businessCtx, handler)
	}()
	go s.readLoop(onClose)
	go s.writeLoop()
}

func (s *Connection) Close() {
	s.closeOnce.Do(func() {
		// 停止接受新的出站消息；读写循环退出后分别清理自己的队列。
		s.cancel()
		if s.conn != nil {
			_ = s.conn.WriteControl(
				gorilla.CloseMessage,
				gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, ""),
				time.Now().Add(time.Duration(s.config.CloseTimeoutSeconds)*time.Second),
			)
			_ = s.conn.Close()
		}
	})
}

func (s *Connection) Enqueue(message Message) error {
	select {
	case <-s.ctx.Done():
		return ErrConnectionClosed
	case s.outbound <- message:
		return nil
	default:
		return ErrOutboundQueueFull
	}
}

func (s *Connection) UserID() string        { return s.identity.UserID }
func (s *Connection) DeviceID() string      { return s.identity.DeviceID }
func (s *Connection) AuthSessionID() string { return s.identity.AuthSessionID }
func (s *Connection) ConnectionID() string  { return s.identity.ConnectionID }

func (s *Connection) Platform() Platform {
	return s.identity.Platform
}

// SetPageHidden 记录页面连续隐藏的起点；重复 hidden 不延长回收期限。
func (s *Connection) SetPageHidden(hidden bool, now time.Time) {
	s.visibilityMu.Lock()
	defer s.visibilityMu.Unlock()
	if s.hiddenReclaim {
		return
	}

	if hidden {
		if s.hiddenSince.IsZero() {
			s.hiddenSince = now
		}
		return
	}
	s.hiddenSince = time.Time{}
}

// CloseIfPageHiddenExpired 原子确认隐藏超时并标记回收，避免可见状态与扫描关闭竞态。
func (s *Connection) CloseIfPageHiddenExpired(now time.Time, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	s.visibilityMu.Lock()
	if s.hiddenSince.IsZero() || now.Sub(s.hiddenSince) < timeout || s.hiddenReclaim {
		s.visibilityMu.Unlock()
		return false
	}
	s.hiddenReclaim = true
	s.visibilityMu.Unlock()

	s.Close()
	return true
}

func (s *Connection) read() (*Message, error) {
	messageType, data, err := s.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if messageType != gorilla.BinaryMessage {
		return nil, fmt.Errorf("不支持的实时通道消息类型：%d", messageType)
	}

	var frame wspb.WsFrame
	if err := proto.Unmarshal(data, &frame); err != nil {
		return nil, err
	}

	_ = s.conn.SetReadDeadline(time.Now().Add(time.Duration(s.config.PongWaitSeconds) * time.Second))
	return &Message{Op: frame.GetOp(), Data: frame.GetData()}, nil
}

func (s *Connection) PushProtobufEvent(op string, payload proto.Message) error {
	encoded, err := proto.Marshal(payload)
	if err != nil {
		return err
	}
	return s.Enqueue(Message{Op: op, Data: encoded})
}

// inbound 仅由 readLoop 关闭，正常断线后继续分发已经接收的消息。
func (s *Connection) dispatchLoop(businessCtx context.Context, handler MessageHandler) {
	if handler == nil {
		return
	}
	for message := range s.inbound {
		handler(businessCtx, s, message.Op, message.Data)
	}
}

func (s *Connection) readLoop(onClose func(*Connection)) {
	defer func() {
		s.Close()
		close(s.inbound)
		if onClose != nil {
			// 执行关闭连接的回调函数
			onClose(s)
		}
	}()

	_ = s.conn.SetReadDeadline(time.Now().Add(time.Duration(s.config.PongWaitSeconds) * time.Second))
	if s.config.MaxMessageSize > 0 {
		s.conn.SetReadLimit(int64(s.config.MaxMessageSize))
	}
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(time.Duration(s.config.PongWaitSeconds) * time.Second))
	})

	for {
		message, err := s.read()
		// 慢连接直接关闭
		if err != nil {
			return
		}
		if s.ctx.Err() != nil {
			return
		}

		select {
		case <-s.ctx.Done():
			return
		case s.inbound <- *message:
		default:
			// 客户端请求处理不过来，关闭慢连接
			return
		}
	}
}

func (s *Connection) accumulateLoop(
	accumulator *BatchMessageAccumulator,
	readyQueue chan<- *MessageBatch,
) error {
	if accumulator == nil {
		return errors.New("batch message accumulator is nil")
	}
	defer accumulator.stopTimer()

	emitBatch := func(batch *MessageBatch) error {
		if batch == nil {
			return nil
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}

		select {
		case readyQueue <- batch:
			return nil

		case <-s.ctx.Done():
			// Connection 关闭了，当前批次被 cancel
			return s.ctx.Err()
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()

		case item, ok := <-s.outbound:
			if !ok || s.ctx.Err() != nil {
				return ErrConnectionClosed
			}
			for _, batch := range accumulator.Append(item) {
				if err := emitBatch(batch); err != nil {
					return err
				}
			}

		case <-accumulator.timerC:
			if err := s.ctx.Err(); err != nil {
				return err
			}
			// 一批数据到时间了，依旧封口
			if err := emitBatch(accumulator.Flush()); err != nil {
				return err
			}
		}
	}
}

func (s *Connection) sendLoop(
	readyQueue <-chan *MessageBatch,
) error {
	if s.config.PingPeriodSeconds <= 0 {
		return errors.New("ping period must be greater than zero")
	}

	pingTicker := time.NewTicker(time.Duration(s.config.PingPeriodSeconds) * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()

		case batch, ok := <-readyQueue:
			if !ok {
				return nil
			}
			if err := s.ctx.Err(); err != nil {
				return err
			}

			if batch == nil {
				continue
			}

			if err := s.sendBatch(batch); err != nil {
				// 出现慢连接，直接关闭
				return fmt.Errorf(
					"发送消息批次失败: %w",
					err,
				)
			}

		case <-pingTicker.C:
			if err := s.ctx.Err(); err != nil {
				return err
			}
			if err := s.write(
				gorilla.PingMessage,
				nil,
			); err != nil {
				return fmt.Errorf("发送 WebSocket Ping 失败: %w", err)
			}
		}
	}
}

func (s *Connection) writeLoop() {
	accumulator := NewBatchMessageAccumulator(s.config)
	readyQueue := make(chan *MessageBatch, s.config.BatchReadyQueueSize)
	errCh := make(chan error, 2)

	defer close(readyQueue)

	go func() {
		errCh <- s.accumulateLoop(accumulator, readyQueue)
	}()

	go func() {
		errCh <- s.sendLoop(readyQueue)
	}()

	// 任一协程结束就关闭连接，唤醒另一个协程，再等待它退出后清理队列。
	for range 2 {
		err := <-errCh
		s.Close()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrConnectionClosed) {
			log.Printf("WebSocket 写循环退出：connection=%s err=%v", s.ConnectionID(), err)
		}
	}
}

func (s *Connection) write(messageType int, data []byte) error {
	_ = s.conn.SetWriteDeadline(time.Now().Add(time.Duration(s.config.WriteWaitSeconds) * time.Second))
	return s.conn.WriteMessage(messageType, data)
}
