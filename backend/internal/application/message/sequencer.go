package message

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"IM_backend/configs"
)

type sendTask struct {
	dto   SendMessageDTO
	reply func(*MessageAckDTO)
}

type sequencerShard struct {
	tasks   chan sendTask
	ticker  *time.Ticker
	flushMu sync.Mutex
}

// Sequencer 按会话分片聚合消息，持久化完成后逐条返回处理结果。
type Sequencer struct {
	ctx    context.Context
	app    *MessageApplication
	shards []*sequencerShard
}

func NewSequencer(ctx context.Context, app *MessageApplication, config configs.WebSocketConfig) *Sequencer {
	s := &Sequencer{ctx: ctx, app: app, shards: make([]*sequencerShard, config.MessageSequencerShardCount)}
	for i := range s.shards {
		shard := &sequencerShard{tasks: make(chan sendTask, config.MessageBatchSize), ticker: time.NewTicker(time.Duration(config.MessageFlushIntervalMs) * time.Millisecond)}
		s.shards[i] = shard
		go s.run(shard)
	}
	return s
}

func (s *Sequencer) shardFor(conversationID string) *sequencerShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(conversationID))
	return s.shards[h.Sum32()%uint32(len(s.shards))]
}

func (s *Sequencer) Submit(ctx context.Context, dto SendMessageDTO, reply func(*MessageAckDTO)) error {
	shard := s.shardFor(dto.ConversationID)
	task := sendTask{dto: dto, reply: reply}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case shard.tasks <- task:
			return nil
		default:
			s.flush(s.ctx, shard)
		}
	}
}

func (s *Sequencer) run(shard *sequencerShard) {
	defer shard.ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-shard.ticker.C:
			s.flush(s.ctx, shard)
		}
	}
}

func (s *Sequencer) Flush(ctx context.Context) {
	for _, shard := range s.shards {
		shard.ticker.Stop()
		s.flush(ctx, shard)
	}
}

func (s *Sequencer) flush(ctx context.Context, shard *sequencerShard) {
	shard.flushMu.Lock()
	defer shard.flushMu.Unlock()
	count := len(shard.tasks)
	if count == 0 {
		return
	}
	groups := make(map[string][]sendTask)
	conversationIDs := make([]string, 0)
	for i := 0; i < count; i++ {
		task := <-shard.tasks
		id := task.dto.ConversationID
		if _, exists := groups[id]; !exists {
			conversationIDs = append(conversationIDs, id)
		}
		groups[id] = append(groups[id], task)
	}
	for _, id := range conversationIDs {
		tasks := groups[id]
		dtos := make([]SendMessageDTO, len(tasks))
		for i := range tasks {
			dtos[i] = tasks[i].dto
		}
		acks, err := s.app.HandleSendMessages(ctx, id, dtos)
		if err != nil {
			log.Printf("批量处理消息失败：conversation=%s count=%d err=%v", id, len(tasks), err)
		}
		if len(acks) != len(tasks) {
			if err == nil {
				err = fmt.Errorf("批量 ACK 数量不匹配：messages=%d acks=%d", len(tasks), len(acks))
			}
			for _, task := range tasks {
				task.reply(&MessageAckDTO{ClientMessageID: task.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error(), SendTime: time.Now().UnixMilli()})
			}
			continue
		}
		for i, task := range tasks {
			ack := acks[i]
			if ack == nil {
				ack = &MessageAckDTO{ClientMessageID: task.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: "消息处理失败", SendTime: time.Now().UnixMilli()}
			}
			task.reply(ack)
		}
	}
}
