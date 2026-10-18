package websocket

import (
	"IM_backend/internal/shared/protocol"
	wspb "IM_backend/internal/transport/ws/pb"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

type BatchStatus uint8

var (
	ErrInvalidBatchTransition = errors.New("invalid batch status transition")
	ErrBatchNotAccumulating   = errors.New("batch is not accumulating")
	ErrBatchEmpty             = errors.New("batch is empty")
)

const (
	// BatchAccumulating 正在聚合消息。
	// 批次尚未封口，还可以继续追加消息。
	BatchAccumulating BatchStatus = iota

	// BatchReady 批次已经封口，等待发送。
	// 触发原因可能是数量满、字节满或者 linger 超时。
	BatchReady

	// BatchSending 正在调用 WebSocket WriteMessage。
	BatchSending

	// BatchSent 已成功写入服务端 TCP 缓冲区。
	// 注意：不代表客户端已经收到。
	BatchSent

	// BatchFailed 写入失败，当前连接不再重试。
	BatchFailed

	// BatchCancelled Connection 关闭，批次被取消。
	BatchCancelled
)

func (s BatchStatus) String() string {
	switch s {
	case BatchAccumulating:
		return "accumulating"
	case BatchReady:
		return "ready"
	case BatchSending:
		return "sending"
	case BatchSent:
		return "sent"
	case BatchFailed:
		return "failed"
	case BatchCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// 将消息批次封口，准备发送
type FlushReason uint8

const (
	FlushReasonUnknown FlushReason = iota

	// 达到最大消息数量。
	FlushReasonMessageCount

	// 达到最大字节数。
	FlushReasonBytes

	// 达到最大等待时间。
	FlushReasonLinger

	// 遇到需要立即发送的控制消息。
	FlushReasonImmediate

	// Connection 正常关闭前刷新。
	FlushReasonShutdown
)

func (r FlushReason) String() string {
	switch r {
	case FlushReasonMessageCount:
		return "message_count"
	case FlushReasonBytes:
		return "bytes"
	case FlushReasonLinger:
		return "linger"
	case FlushReasonImmediate:
		return "immediate"
	case FlushReasonShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

type IdGenerator interface {
	Generate() (string, error)
}

type MessageBatchConfig struct {
	MaxMessages    int
	MaxBytes       int
	Linger         time.Duration
	ReadyQueueSize int
}

type AppendPolicy uint8

const (
	AppendPolicyBatch AppendPolicy = iota
	AppendPolicyFlush
)

func AppendPolicyForEvent(eventType string) AppendPolicy {
	// 服务端接收到消息后，需要给前端发送 ack 应答，这个需要立即发送
	if eventType == string(protocol.EventTypeMsgAck) {
		return AppendPolicyFlush
	}
	return AppendPolicyBatch
}

func DefaultBatchConfig() MessageBatchConfig {
	return MessageBatchConfig{
		MaxMessages:    64,
		MaxBytes:       8 * 1024,
		Linger:         20 * time.Millisecond,
		ReadyQueueSize: 16,
	}
}

func (c MessageBatchConfig) withDefaults() MessageBatchConfig {
	defaults := DefaultBatchConfig()
	if c.MaxMessages <= 0 {
		c.MaxMessages = defaults.MaxMessages
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = defaults.MaxBytes
	}
	if c.Linger <= 0 {
		c.Linger = defaults.Linger
	}
	if c.ReadyQueueSize <= 0 {
		c.ReadyQueueSize = defaults.ReadyQueueSize
	}
	return c
}

type BatchMessageAccumulator struct {
	config MessageBatchConfig

	activeBatch *MessageBatch

	idGenerator IdGenerator

	lingerTimer *time.Timer
	timerC      <-chan time.Time
}

func NewBatchMessageAccumulator(
	config MessageBatchConfig,
	idGenerator IdGenerator,
) *BatchMessageAccumulator {
	config = config.withDefaults()
	timer := time.NewTimer(time.Hour)

	// 立即重置
	if !timer.Stop() {
		<-timer.C
	}

	return &BatchMessageAccumulator{
		config:      config,
		lingerTimer: timer,
		idGenerator: idGenerator,
	}
}

func (a *BatchMessageAccumulator) newBatch() (*MessageBatch, error) {
	batchID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成消息批次 ID 失败: %w", err)
	}

	return NewMessageBatch(
		batchID,
		a.config.MaxMessages,
	), nil
}

func (a *BatchMessageAccumulator) startTimer() {
	a.lingerTimer.Reset(a.config.Linger)
	a.timerC = a.lingerTimer.C
}

func (a *BatchMessageAccumulator) stopTimer() {
	a.lingerTimer.Stop()
	a.timerC = nil
}

func (a *BatchMessageAccumulator) Flush(reason FlushReason) (*MessageBatch, error) {
	if a.activeBatch == nil || a.activeBatch.MessageCount() == 0 {
		a.stopTimer()
		return nil, nil
	}

	if err := a.activeBatch.Seal(reason); err != nil {
		return nil, err
	}

	batch := a.activeBatch

	a.stopTimer()

	a.activeBatch = nil
	return batch, nil
}

func (a *BatchMessageAccumulator) Append(
	message Message,
	policy AppendPolicy,
) ([]*MessageBatch, error) {
	messageSize := proto.Size(&wspb.WsFrame{
		Op:   message.Op,
		Data: message.Data,
	})
	readyBatches := make([]*MessageBatch, 0, 2)

	// 新消息加入后导致大小超出了，那就先封口
	if a.activeBatch != nil &&
		a.activeBatch.MessageCount() > 0 &&
		a.activeBatch.Bytes+messageSize > a.config.MaxBytes {
		batch, err := a.Flush(FlushReasonBytes)
		if err != nil {
			return nil, err
		}

		if batch != nil {
			readyBatches = append(readyBatches, batch)
		}
	}

	if a.activeBatch == nil {
		batch, err := a.newBatch()
		if err != nil {
			return nil, err
		}

		a.activeBatch = batch
		// 开启 linger 定时器
		a.startTimer()
	}

	if err := a.activeBatch.AppendMessage(message, messageSize); err != nil {
		return nil, err
	}

	var reason FlushReason = FlushReasonUnknown

	switch {
	case policy == AppendPolicyFlush:
		reason = FlushReasonImmediate
	case a.activeBatch.MessageCount() >= a.config.MaxMessages:
		reason = FlushReasonMessageCount
	case a.activeBatch.Bytes >= a.config.MaxBytes:
		reason = FlushReasonBytes
	}

	if reason != FlushReasonUnknown {
		batch, err := a.Flush(reason)

		if err != nil {
			return nil, err
		}

		if batch != nil {
			readyBatches = append(readyBatches, batch)
		}
	}

	return readyBatches, nil
}

func (a *BatchMessageAccumulator) TimerC() <-chan time.Time {
	return a.timerC
}

type MessageBatch struct {
	Id       string
	Messages []Message
	Bytes    int

	Status      BatchStatus
	FlushReason FlushReason

	// 便于调试信息
	CreateAt   time.Time
	SealedAt   time.Time
	SendingAt  time.Time
	FinishedAt time.Time

	LastError error
}

func NewMessageBatch(id string, bufferCap int) *MessageBatch {
	if bufferCap <= 0 {
		bufferCap = 64
	}

	return &MessageBatch{
		Id:       id,
		Messages: make([]Message, 0, bufferCap),
		Status:   BatchAccumulating,
		CreateAt: time.Now(),
	}
}

func (m *MessageBatch) AppendMessage(message Message, size int) error {
	if m.Status != BatchAccumulating {
		// 当前缓冲区已经 flush 了
		return fmt.Errorf(
			"%w: append message while batch status is %s",
			ErrBatchNotAccumulating,
			m.Status,
		)
	}

	m.Messages = append(m.Messages, message)
	m.Bytes += size

	return nil
}

func (m *MessageBatch) Seal(reason FlushReason) error {
	if m.Status != BatchAccumulating {
		return fmt.Errorf(
			"%w: %s -> %s",
			ErrInvalidBatchTransition,
			m.Status,
			BatchReady,
		)
	}

	if len(m.Messages) == 0 {
		return ErrBatchEmpty
	}

	m.SealedAt = time.Now()
	m.FlushReason = reason

	// 准备发送
	m.Status = BatchReady
	return nil
}

func (m *MessageBatch) StartSending() error {
	if m.Status != BatchReady {
		return fmt.Errorf(
			"%w: %s -> %s",
			ErrInvalidBatchTransition,
			m.Status,
			BatchSending,
		)
	}

	m.Status = BatchSending
	m.SendingAt = time.Now()

	return nil
}

func (m *MessageBatch) MarkSent() error {
	if m.Status != BatchSending {
		return fmt.Errorf(
			"%w: %s -> %s",
			ErrInvalidBatchTransition,
			m.Status,
			BatchSent,
		)
	}

	m.Status = BatchSent
	m.FinishedAt = time.Now()
	m.LastError = nil

	return nil
}

func (m *MessageBatch) MarkFailed(err error) error {
	if m.Status != BatchSending {
		return fmt.Errorf(
			"%w: %s -> %s",
			ErrInvalidBatchTransition,
			m.Status,
			BatchFailed,
		)
	}

	m.Status = BatchFailed
	m.FinishedAt = time.Now()
	m.LastError = err

	return nil
}

func (m *MessageBatch) Cancel() error {
	switch m.Status {
	case BatchAccumulating, BatchReady:
		m.Status = BatchCancelled
		m.FinishedAt = time.Now()
		return nil

	case BatchCancelled, BatchSent, BatchFailed:
		return nil

	case BatchSending:
		return fmt.Errorf(
			"%w: cannot cancel sending batch",
			ErrInvalidBatchTransition,
		)
	default:
		return fmt.Errorf(
			"%w: unknown status %d",
			ErrInvalidBatchTransition,
			m.Status,
		)
	}
}

func (m *MessageBatch) MessageCount() int {
	return len(m.Messages)
}
