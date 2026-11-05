package ws

import (
	"time"

	"IM_backend/configs"
	"IM_backend/internal/application/ports/messaging"
	wspb "IM_backend/internal/interfaces/ws/pb"

	"google.golang.org/protobuf/proto"
)

type MessageBatch struct {
	BatchID  string
	Messages []Message
	Bytes    int
}

// BatchMessageAccumulator 仅由 accumulateLoop 操作，Flush 后批次交由 sendLoop 独占。
type BatchMessageAccumulator struct {
	config      configs.WebSocketConfig
	activeBatch *MessageBatch
	timer       *time.Timer
}

func NewBatchMessageAccumulator(config configs.WebSocketConfig) *BatchMessageAccumulator {
	timer := time.NewTimer(time.Duration(config.BatchLingerMilliseconds) * time.Millisecond)
	return &BatchMessageAccumulator{
		config: config,
		timer:  timer,
	}
}

func (a *BatchMessageAccumulator) stop() {
	a.activeBatch = nil
	a.timer.Stop()
}

func (a *BatchMessageAccumulator) flush() *MessageBatch {
	batch := a.activeBatch
	a.activeBatch = nil
	a.timer.Stop()
	return batch
}

func (a *BatchMessageAccumulator) Append(message Message) []*MessageBatch {
	size := proto.Size(&wspb.WsFrame{
		Op:   message.Op,
		Data: message.Data,
	})

	readyQuque := make([]*MessageBatch, 0, 2)

	if a.activeBatch != nil && (len(a.activeBatch.Messages)+1 > a.config.BatchMaxMessages || a.activeBatch.Bytes+size > a.config.BatchMaxBytes) {
		readyQuque = append(readyQuque, a.flush())
	}

	if a.activeBatch == nil {
		a.activeBatch = &MessageBatch{Messages: make([]Message, 0, a.config.BatchMaxMessages)}
		a.timer.Reset(time.Duration(a.config.BatchLingerMilliseconds) * time.Millisecond)
	}

	a.activeBatch.Messages = append(a.activeBatch.Messages, message)
	a.activeBatch.Bytes += size

	if message.Op == messaging.TypeMessageSendResult || len(a.activeBatch.Messages) >= a.config.BatchMaxMessages || a.activeBatch.Bytes >= a.config.BatchMaxBytes {
		readyQuque = append(readyQuque, a.flush())
	}

	return readyQuque
}
