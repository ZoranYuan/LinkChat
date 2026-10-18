package websocket

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"IM_backend/internal/infrastructure/id/snow"
	"IM_backend/internal/shared/diagnostics"
	"IM_backend/internal/shared/protocol"
	wspb "IM_backend/internal/transport/ws/pb"

	gorilla "github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var (
	ErrConnectionClosed  = errors.New("实时通道连接已关闭")
	ErrOutboundQueueFull = errors.New("实时通道发送队列已满")
)

type Message struct {
	Op                string
	Data              []byte
	inboundEnqueuedAt time.Time
}

type OutboundItem struct {
	Message Message
	Policy  AppendPolicy
}

type MessageHandler func(context.Context, *Connection, string, []byte)

type Connection struct {
	conn *gorilla.Conn
	ctx  context.Context // 网络读写随连接关闭而取消。

	identity ConnectionIdentity

	idGenerator *snow.Generator
	batchConfig MessageBatchConfig
	maxReadSize int64
	outbound    chan OutboundItem
	inbound     chan Message

	cancel        context.CancelFunc
	closeOnce     sync.Once
	visibilityMu  sync.RWMutex
	hiddenSince   time.Time
	hiddenReclaim bool
}

func NewConnection(
	networkCtx context.Context,
	conn *gorilla.Conn,
	identity ConnectionIdentity,
	maxBufferSize int,
	idGenerator *snow.Generator,
	batchConfig MessageBatchConfig,
) *Connection {
	ctx, cancel := context.WithCancel(networkCtx)
	if maxBufferSize <= 0 {
		maxBufferSize = DefaultBatchConfig().ReadyQueueSize
	}
	return &Connection{
		conn:        conn,
		ctx:         ctx,
		cancel:      cancel,
		identity:    identity,
		idGenerator: idGenerator,
		batchConfig: batchConfig.withDefaults(),
		outbound:    make(chan OutboundItem, maxBufferSize),
		inbound:     make(chan Message, maxBufferSize),
	}
}

func (s *Connection) marshalBatch(batch *MessageBatch) ([]byte, error) {
	frames := make([]*wspb.WsFrame, 0, len(batch.Messages))

	for _, message := range batch.Messages {
		frames = append(frames, &wspb.WsFrame{
			Op:   message.Op,
			Data: message.Data,
		})
	}

	batchData, err := proto.Marshal(&wspb.WsBatch{
		Frames: frames,
	})

	if err != nil {
		return nil, fmt.Errorf("序列化消息批次失败: %w", err)
	}

	payload, err := proto.Marshal(&wspb.WsFrame{
		Op:   protocol.EventMessageBatch,
		Data: batchData,
	})

	if err != nil {
		return nil, fmt.Errorf("序列化批量消息帧失败: %w", err)
	}

	return payload, nil
}

func (s *Connection) sendBatch(batch *MessageBatch, writeWait int) error {
	if err := batch.StartSending(); err != nil {
		return err
	}
	firstSeq, lastSeq, messageFrames := outboundMessageSeqRange(batch)

	marshalStartedAt := time.Now()
	payload, err := s.marshalBatch(batch)
	marshalDuration := time.Since(marshalStartedAt)
	if err != nil {
		_ = batch.MarkFailed(err)
		diagnostics.Logf("stage=ws_outbound_batch connection_id=%s batch_id=%s messages=%d message_frames=%d first_seq=%d last_seq=%d bytes=%d marshal_us=%d outcome=marshal_error error=%q",
			s.ConnectionID(), batch.Id, batch.MessageCount(), messageFrames, firstSeq, lastSeq, batch.Bytes, marshalDuration.Microseconds(), err.Error())
		return err
	}

	writeStartedAt := time.Now()
	if err := s.write(gorilla.BinaryMessage, payload, writeWait); err != nil {
		_ = batch.MarkFailed(err)
		diagnostics.Logf("stage=ws_outbound_batch connection_id=%s batch_id=%s messages=%d message_frames=%d first_seq=%d last_seq=%d bytes=%d linger_us=%d queue_wait_us=%d marshal_us=%d socket_write_us=%d outcome=write_error error=%q",
			s.ConnectionID(), batch.Id, batch.MessageCount(), messageFrames, firstSeq, lastSeq, batch.Bytes, batch.SealedAt.Sub(batch.CreateAt).Microseconds(),
			batch.SendingAt.Sub(batch.SealedAt).Microseconds(), marshalDuration.Microseconds(), time.Since(writeStartedAt).Microseconds(), err.Error())
		return err
	}

	markErr := batch.MarkSent()
	diagnostics.Logf("stage=ws_outbound_batch connection_id=%s batch_id=%s messages=%d message_frames=%d first_seq=%d last_seq=%d bytes=%d linger_us=%d queue_wait_us=%d marshal_us=%d socket_write_us=%d total_us=%d outcome=%s",
		s.ConnectionID(), batch.Id, batch.MessageCount(), messageFrames, firstSeq, lastSeq, batch.Bytes, batch.SealedAt.Sub(batch.CreateAt).Microseconds(),
		batch.SendingAt.Sub(batch.SealedAt).Microseconds(), marshalDuration.Microseconds(), time.Since(writeStartedAt).Microseconds(),
		time.Since(batch.SendingAt).Microseconds(), connectionBatchOutcome(markErr))
	return markErr
}

func outboundMessageSeqRange(batch *MessageBatch) (int64, int64, int) {
	if !diagnostics.Enabled() || batch == nil {
		return 0, 0, 0
	}
	var firstSeq, lastSeq int64
	count := 0
	for _, item := range batch.Messages {
		if item.Op != string(protocol.EventTypeSendMessage) {
			continue
		}
		var event wspb.MessageEvent
		if err := proto.Unmarshal(item.Data, &event); err != nil || event.GetSeq() <= 0 {
			continue
		}
		seq := event.GetSeq()
		if count == 0 || seq < firstSeq {
			firstSeq = seq
		}
		if count == 0 || seq > lastSeq {
			lastSeq = seq
		}
		count++
	}
	return firstSeq, lastSeq, count
}

func connectionBatchOutcome(err error) string {
	if err != nil {
		return "state_error"
	}
	return "sent"
}

func (s *Connection) Start(businessCtx context.Context, pongWait, pingPeriod, writeWait int, handler MessageHandler, onClose, onDispatchDone func(*Connection)) {
	go func() {
		if onDispatchDone != nil {
			defer onDispatchDone(s)
		}
		s.dispatchLoop(businessCtx, handler)
	}()
	go s.readLoop(pongWait, onClose)
	go s.writeLoop(pingPeriod, writeWait)
}

func (s *Connection) Close() {
	s.closeOnce.Do(func() {
		// 不等待业务分发，也不关闭队列；inbound 由唯一写入者 readLoop 关闭。
		s.cancel()
		if s.conn != nil {
			_ = s.conn.WriteControl(
				gorilla.CloseMessage,
				gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, ""),
				time.Now().Add(time.Second),
			)
			_ = s.conn.Close()
		}
	})
}

func (s *Connection) Enqueue(message Message, policy AppendPolicy) error {
	select {
	case <-s.ctx.Done():
		return ErrConnectionClosed
	default:
	}

	select {
	case s.outbound <- OutboundItem{Message: message, Policy: policy}:
		return nil
	case <-s.ctx.Done():
		return ErrConnectionClosed
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

func (s *Connection) SetReadLimit(size int64) {
	if size > 0 {
		s.maxReadSize = size
	}
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

func (s *Connection) read(pongWait int) (*Message, error) {
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

	_ = s.conn.SetReadDeadline(time.Now().Add(time.Duration(pongWait) * time.Second))
	return &Message{Op: frame.GetOp(), Data: frame.GetData()}, nil
}

func (s *Connection) PushEvent(
	op string,
	payload []byte,
) error {
	encoded, err := EncodePayload(op, payload)
	if err != nil {
		return err
	}

	return s.Enqueue(
		Message{
			Op:   op,
			Data: encoded,
		},
		AppendPolicyForEvent(op),
	)
}

// inbound 仅由 readLoop 关闭，正常断线后继续分发已经接收的消息。
func (s *Connection) dispatchLoop(businessCtx context.Context, handler MessageHandler) {
	for message := range s.inbound {
		queueWait := time.Since(message.inboundEnqueuedAt)
		queueDepth := len(s.inbound)
		if handler == nil {
			diagnostics.Logf("stage=ws_inbound_dispatch connection_id=%s op=%s queue_wait_us=%d handler_us=0 queue_depth=%d queue_capacity=%d payload_bytes=%d outcome=handler_missing",
				s.ConnectionID(), message.Op, queueWait.Microseconds(), queueDepth, cap(s.inbound), len(message.Data))
			return
		}
		handlerStartedAt := time.Now()
		handler(businessCtx, s, message.Op, message.Data)
		diagnostics.Logf("stage=ws_inbound_dispatch connection_id=%s op=%s queue_wait_us=%d handler_us=%d queue_depth=%d queue_capacity=%d payload_bytes=%d outcome=handled",
			s.ConnectionID(), message.Op, queueWait.Microseconds(), time.Since(handlerStartedAt).Microseconds(), queueDepth, cap(s.inbound), len(message.Data))
	}
}

func (s *Connection) readLoop(pongWait int, onClose func(*Connection)) {
	defer func() {
		s.Close()
		close(s.inbound)
		if onClose != nil {
			// 执行关闭连接的回调函数
			onClose(s)
		}
	}()

	_ = s.conn.SetReadDeadline(time.Now().Add(time.Duration(pongWait) * time.Second))
	if s.maxReadSize > 0 {
		s.conn.SetReadLimit(s.maxReadSize)
	}
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(time.Duration(pongWait) * time.Second))
	})

	for {
		message, err := s.read(pongWait)
		if err != nil {
			return
		}
		message.inboundEnqueuedAt = time.Now()
		if s.ctx.Err() != nil {
			return
		}

		select {
		case <-s.ctx.Done():
			return
		case s.inbound <- *message:
		default:
			// 客户端请求处理不过来，关闭慢连接
			diagnostics.LogfAlways("stage=ws_inbound_queue connection_id=%s op=%s queue_depth=%d queue_capacity=%d payload_bytes=%d outcome=full",
				s.ConnectionID(), message.Op, len(s.inbound), cap(s.inbound), len(message.Data))
			return
		}
	}
}

func (s *Connection) accumulateLoop(
	ctx context.Context,
	accumulator *BatchMessageAccumulator,
	readyQueue chan<- *MessageBatch,
) error {
	if accumulator == nil {
		return errors.New("batch message accumulator is nil")
	}

	defer close(readyQueue)

	emitBatch := func(batch *MessageBatch) error {
		if batch == nil || batch.MessageCount() == 0 {
			return nil
		}

		select {
		case readyQueue <- batch:
			return nil

		case <-ctx.Done():
			// Connection 关闭了，当前批次被 cancel
			_ = batch.Cancel()
			return ctx.Err()
		}
	}

	for {
		select {
		case <-ctx.Done():
			if accumulator.activeBatch != nil {
				accumulator.activeBatch.Cancel()
			}

			return ctx.Err()

		case item := <-s.outbound:
			readyBatches, err := accumulator.Append(item.Message, item.Policy)
			if err != nil {
				return fmt.Errorf("消息加入聚合器失败: %w", err)
			}

			for _, batch := range readyBatches {
				if err := emitBatch(batch); err != nil {
					return err
				}
			}

		case <-accumulator.TimerC():
			// 一批数据到时间了，依旧封口
			batch, err := accumulator.Flush(FlushReasonLinger)
			if err != nil {
				return fmt.Errorf("linger 超时封口失败: %w", err)
			}

			if err := emitBatch(batch); err != nil {
				return err
			}
		}
	}
}

func (s *Connection) sendLoop(
	ctx context.Context,
	readyQueue <-chan *MessageBatch,
	pingPeriod, writeWait int,
) error {
	if pingPeriod <= 0 {
		return errors.New("ping period must be greater than zero")
	}

	pingTicker := time.NewTicker(time.Duration(pingPeriod) * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case batch, ok := <-readyQueue:
			if !ok {
				return nil
			}

			if batch == nil || batch.MessageCount() == 0 {
				continue
			}

			if err := s.sendBatch(batch, writeWait); err != nil {
				return fmt.Errorf(
					"发送消息批次失败, batch_id=%s: %w",
					batch.Id,
					err,
				)
			}

		case <-pingTicker.C:
			if err := s.write(
				gorilla.PingMessage,
				nil,
				writeWait,
			); err != nil {
				return fmt.Errorf("发送 WebSocket Ping 失败: %w", err)
			}
		}
	}
}

func (s *Connection) writeLoop(pingPeriod, writeWait int) {
	defer s.Close()

	err := s.writeBatchLoop(pingPeriod, writeWait)

	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, ErrConnectionClosed) {
		log.Printf("Connection 写循环退出：%v", err)
	}
}

func (s *Connection) writeBatchLoop(pingPeriod, writeWait int) error {
	accumulator := NewBatchMessageAccumulator(s.batchConfig, s.idGenerator)
	readyQueue := make(chan *MessageBatch, s.batchConfig.ReadyQueueSize)
	errCh := make(chan error, 2)

	go func() {
		errCh <- s.accumulateLoop(s.ctx, accumulator, readyQueue)
	}()

	go func() {
		errCh <- s.sendLoop(s.ctx, readyQueue, pingPeriod, writeWait)
	}()

	return <-errCh
}

func (s *Connection) write(messageType int, data []byte, writeWait int) error {
	_ = s.conn.SetWriteDeadline(time.Now().Add(time.Duration(writeWait) * time.Second))
	return s.conn.WriteMessage(messageType, data)
}
