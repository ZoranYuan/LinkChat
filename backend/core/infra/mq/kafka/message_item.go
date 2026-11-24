package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

type MessageItem struct {
	Record   *sarama.ConsumerMessage
	Complete func()
}

type EventHandler interface {
	Topic() string
	Handle(ctx context.Context, record *MessageItem) error
}

type partProgress struct {
	topic     string
	partition int32
	base      int64              // 下一条待完成的 offset
	done      map[int64]struct{} // 已经完成的消息，等待补缺口的 offset
}

type orderedCommitter struct {
	session sarama.ConsumerGroupSession
	mu      sync.Mutex
	parts   map[string]*partProgress
}

func newOrderedCommitter(s sarama.ConsumerGroupSession) *orderedCommitter {
	return &orderedCommitter{session: s, parts: map[string]*partProgress{}}
}

func partKey(topic string, p int32) string { return fmt.Sprintf("%s:%d", topic, p) }

func (oc *orderedCommitter) init(topic string, p int32, initial int64) {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	oc.parts[partKey(topic, p)] = &partProgress{
		topic:     topic,
		partition: p,
		base:      initial,
		done:      map[int64]struct{}{},
	}
}

func (oc *orderedCommitter) complete(r *sarama.ConsumerMessage) {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	pg, ok := oc.parts[partKey(r.Topic, r.Partition)]

	if !ok || r.Offset < pg.base {
		return
	}

	// 记录已经完成的 offset
	pg.done[r.Offset] = struct{}{}

	advanced := false
	// 每次 complete 都需要去推进整体的水位
	for {
		if _, ok := pg.done[pg.base]; !ok {
			break
		}

		delete(pg.done, pg.base)
		pg.base++

		// 本批有需要提交的 offset， 即：pg.base
		advanced = true
	}

	if advanced {
		// MarkOffset 提交的是"下一条要消费的 offset"，正好等于 base
		oc.session.MarkOffset(pg.topic, pg.partition, pg.base, "")
	}
}
