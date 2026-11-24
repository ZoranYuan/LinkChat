package ws

import (
	"sync"
	"time"
)

type PendingBatchItem struct {
	batchID     string
	payload     []byte
	nextRetryAt time.Time
	retries     int
}

type PendingBatch struct {
	mu    sync.Mutex
	items map[string]*PendingBatchItem
}

func (p *PendingBatch) handleAck(batchID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.items, batchID)
}

func (p *PendingBatch) addPending(batchID string, payload []byte, batchAckTimeoutSeconds int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.items[batchID] = &PendingBatchItem{
		batchID: batchID,
		payload: payload,
		nextRetryAt: time.Now().Add(
			time.Duration(batchAckTimeoutSeconds) * time.Second,
		),
	}
}
