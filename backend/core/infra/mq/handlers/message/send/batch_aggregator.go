package send

import (
	"IM_backend/core/infra/mq/kafka"
	"context"
	"sync"
	"time"
)

type batchAggregator struct {
	mu            sync.Mutex
	batches       map[string][]*kafka.MessageItem
	timers        map[string]*time.Timer
	flushCh       chan string
	batchSize     int
	flushInterval time.Duration
	flush         func(context.Context, string, []*kafka.MessageItem)
	ctx           context.Context
}

func newBatchAggregator(
	ctx context.Context, batchSize int, flushInterval time.Duration,
	flush func(context.Context, string, []*kafka.MessageItem),
) *batchAggregator {
	b := &batchAggregator{
		batches:       map[string][]*kafka.MessageItem{},
		timers:        map[string]*time.Timer{},
		flushCh:       make(chan string, 1024),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		flush:         flush,
		ctx:           ctx,
	}
	go b.run()
	return b
}

func (b *batchAggregator) run() {
	for {
		select {
		case <-b.ctx.Done():
			return
		case convID := <-b.flushCh:
			b.doFlush(convID)
		}
	}
}

func (b *batchAggregator) Submit(item *kafka.MessageItem) {
	convID := string(item.Record.Key) // producer 用 conversationID 作分区键
	b.mu.Lock()
	b.batches[convID] = append(b.batches[convID], item)

	full := len(b.batches[convID]) >= b.batchSize
	if full {
		// 当前批次已满，取消定时器，防止二次刷新
		if t := b.timers[convID]; t != nil {
			t.Stop()
			delete(b.timers, convID)
		}
		b.mu.Unlock()
		b.flushCh <- convID
		return
	}

	if _, ok := b.timers[convID]; !ok {
		// 重新设置定时器
		b.timers[convID] = time.AfterFunc(b.flushInterval, func() {
			b.mu.Lock()
			delete(b.timers, convID)
			b.mu.Unlock()
			b.flushCh <- convID
		})
	}

	b.mu.Unlock()
}

func (b *batchAggregator) doFlush(convID string) {
	b.mu.Lock()
	batch := b.batches[convID]
	delete(b.batches, convID)

	if t := b.timers[convID]; t != nil {
		t.Stop()
		delete(b.timers, convID)
	}

	b.mu.Unlock()
	if len(batch) > 0 {
		b.flush(b.ctx, convID, batch)
	}
}

// Drain 停机时把所有未 flush 的批次推入队列并等待执行完，再返回。
func (b *batchAggregator) Drain() {
	b.mu.Lock()
	convIDs := make([]string, 0, len(b.batches))
	for id := range b.batches {
		convIDs = append(convIDs, id)
	}
	b.mu.Unlock()

	var wg sync.WaitGroup

	for _, convID := range convIDs {
		wg.Add(1)
		go func(convID string) {
			defer wg.Done()
			b.doFlush(convID)
		}(convID)
	}

	wg.Wait()
}
