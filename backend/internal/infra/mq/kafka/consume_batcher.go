package kafka

import (
	"IM_backend/configs"
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type consumeBatchItem struct {
	message *sarama.ConsumerMessage

	// 完成后调用 complete / failed 来通知上层, 目前只是占位
	completed func()

	failed func(error)
}

type consumeBatch struct {
	conversationID string
	items          []*consumeBatchItem
}

type consumeWorker struct {
	input   chan *consumeBatchItem
	batches map[string]*consumeBatch // key 是 conversationID
}

func (c *consumeBatch) flush(
	ctx context.Context,
	processBatchHandler func(context.Context, *consumeBatch),
) {
	if len(c.items) == 0 {
		return
	}

	batch := &consumeBatch{
		conversationID: c.conversationID,
		items:          c.items,
	}
	c.items = nil

	processBatchHandler(ctx, batch)
}

func (w *consumeWorker) run(
	ctx context.Context,
	batchSize int,
	flushEvery time.Duration,
	processBatchHandler func(context.Context, *consumeBatch),
) {
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	flushBatch := func(conversationID string) {
		batch := w.batches[conversationID]
		if batch == nil {
			return
		}
		delete(w.batches, conversationID)
		batch.flush(ctx, processBatchHandler)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-w.input:
			conversationID := string(item.message.Key)
			batch := w.batches[conversationID]

			if batch == nil {
				batch = &consumeBatch{conversationID: conversationID}
				w.batches[conversationID] = batch
			}

			batch.items = append(batch.items, item)
			if len(batch.items) >= batchSize {
				flushBatch(conversationID)
			}

		case <-ticker.C:
			for conversationID := range w.batches {
				flushBatch(conversationID)
			}
		}
	}
}

type consumeBatcher struct {
	workers []*consumeWorker
	// 单个批次的数量阈值
	batchSize  int
	flushEvery time.Duration

	startOnce sync.Once
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	workersWG sync.WaitGroup
}

func newConsumeBatcher(config configs.KafkaConsumerConfig) *consumeBatcher {
	workers := make([]*consumeWorker, config.WorkerCount)

	for i := range workers {
		workers[i] = &consumeWorker{
			input:   make(chan *consumeBatchItem, config.WorkerQueueSize),
			batches: make(map[string]*consumeBatch),
		}
	}

	return &consumeBatcher{
		workers:    workers,
		batchSize:  config.BatchSize,
		flushEvery: time.Duration(config.FlushIntervalMilliseconds) * time.Millisecond,
	}
}

func (cb *consumeBatcher) Submit(ctx context.Context, item *consumeBatchItem) error {
	index := consumeWorkerIndex(item.message.Key, len(cb.workers))

	select {
	case <-ctx.Done():
		return ctx.Err()

	case cb.workers[index].input <- item:
		return nil
	}
}

func (cb *consumeBatcher) Start(ctx context.Context, processBatchHandler func(context.Context, *consumeBatch)) {
	cb.startOnce.Do(func() {
		cb.ctx, cb.cancel = context.WithCancel(ctx)

		for _, worker := range cb.workers {
			cb.workersWG.Add(1)
			go func(worker *consumeWorker) {
				defer cb.workersWG.Done()
				worker.run(cb.ctx, cb.batchSize, cb.flushEvery, processBatchHandler)
			}(worker)
		}
	})
}

func (cb *consumeBatcher) Close() {
	cb.closeOnce.Do(func() {
		if cb.cancel != nil {
			cb.cancel()
		}

		cb.workersWG.Wait()
	})
}

func consumeWorkerIndex(key []byte, workerCount int) int {
	hash := fnv.New32a()
	_, _ = hash.Write(key)
	return int(hash.Sum32() % uint32(workerCount))
}
