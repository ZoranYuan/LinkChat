package ws

import (
	"IM_backend/internal/application/ports/messaging"
	"time"

	"google.golang.org/protobuf/proto"

	"IM_backend/configs"
	wspb "IM_backend/internal/interfaces/ws/pb"
)

type MessageBatch struct {
	Messages []Message
	Bytes    int
}

// BatchMessageAccumulator 仅由 accumulateLoop 操作，Flush 后批次交由 sendLoop 独占。
type BatchMessageAccumulator struct {
	config      configs.WebSocketConfig
	activeBatch *MessageBatch
	lingerTimer *time.Timer
	timerC      <-chan time.Time
}

func NewBatchMessageAccumulator(config configs.WebSocketConfig) *BatchMessageAccumulator {
	timer := time.NewTimer(time.Duration(config.BatchLingerMilliseconds) * time.Millisecond)
	timer.Stop()
	return &BatchMessageAccumulator{
		config:      config,
		lingerTimer: timer,
	}
}

func (a *BatchMessageAccumulator) stopTimer() {
	a.lingerTimer.Stop()
	a.timerC = nil
}

func (a *BatchMessageAccumulator) Flush() *MessageBatch {
	batch := a.activeBatch
	a.activeBatch = nil
	a.stopTimer()
	return batch
}

func (a *BatchMessageAccumulator) Append(message Message) []*MessageBatch {
	messageSize := proto.Size(&wspb.WsFrame{Op: message.Op, Data: message.Data})
	ready := make([]*MessageBatch, 0, 2)
	if a.activeBatch != nil && a.activeBatch.Bytes+messageSize > a.config.BatchMaxBytes {
		ready = append(ready, a.Flush())
	}
	if a.activeBatch == nil {
		a.activeBatch = &MessageBatch{Messages: make([]Message, 0, a.config.BatchMaxMessages)}
		a.lingerTimer.Reset(time.Duration(a.config.BatchLingerMilliseconds) * time.Millisecond)
		a.timerC = a.lingerTimer.C
	}
	a.activeBatch.Messages = append(a.activeBatch.Messages, message)
	a.activeBatch.Bytes += messageSize
	if message.Op == messaging.TypeMessageSendResult || len(a.activeBatch.Messages) >= a.config.BatchMaxMessages || a.activeBatch.Bytes >= a.config.BatchMaxBytes {
		ready = append(ready, a.Flush())
	}
	return ready
}
