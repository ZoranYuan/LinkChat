package handlers

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/core/application/conversation"
	eventport "IM_backend/core/application/ports/events"
	domainconversation "IM_backend/core/domain/conversation"
	"IM_backend/core/interfaces/ws"
)

type MessageDelivery struct {
	gateway       *ws.Gateway
	noticeBatcher *groupNoticeBatcher
}

func NewMessageDelivery(
	delivery *ws.Gateway,
	config configs.MessageConfig,
) *MessageDelivery {
	return &MessageDelivery{
		gateway:       delivery,
		noticeBatcher: newGroupNoticeBatcher(delivery, config),
	}
}

func (m *MessageDelivery) OnlineGroupConnectionCount(groupID string) int {
	return m.gateway.OnlineGroupConnectionCount(groupID)
}

func (m *MessageDelivery) Deliver(
	event eventport.MessagePushEvent,
	pull bool,
) error {
	switch event.ConvType {
	case domainconversation.PrivateChat:
		if event.RecvID == event.SenderID {
			return nil
		}
		return m.gateway.DeliverMessageToUser(event.RecvID, event)
	case domainconversation.GroupChat:
		if pull {
			return m.noticeBatcher.enqueue(event.RecvID, eventport.GroupMessageNoticeEvent{
				ConversationID: event.ConversationID,
				Seq:            event.Seq,
			})
		}
		return m.gateway.DeliverMessageToOnlineGroupMembers(event.RecvID, event, event.SenderID)
	default:
		return conversationapp.ErrUnknownConversationType
	}
}

func (m *MessageDelivery) Close(ctx context.Context) {
	m.noticeBatcher.close(ctx)
}

type pendingGroupNotice struct {
	groupID string
	notice eventport.GroupMessageNoticeEvent
}

type groupNoticeBatcher struct {
	delivery *ws.Gateway
	shards   []*groupNoticeShard
	done     chan struct{}
	stopped  chan struct{}
	once     sync.Once
}

type groupNoticeShard struct {
	mu      sync.Mutex
	pending map[string]pendingGroupNotice
	closed  bool
	ticker  *time.Ticker
}

var ErrGroupNoticeBatcherClosed = errors.New("群聊通知合并器已关闭")

func newGroupNoticeBatcher(delivery *ws.Gateway, config configs.MessageConfig) *groupNoticeBatcher {
	batcher := &groupNoticeBatcher{
		delivery: delivery,
		shards:   make([]*groupNoticeShard, config.LargeGroupNoticeShardCount),
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	var workers sync.WaitGroup
	for i := range batcher.shards {
		shard := &groupNoticeShard{
			pending: make(map[string]pendingGroupNotice),
			ticker:  time.NewTicker(time.Duration(config.LargeGroupNoticeLingerMilliseconds) * time.Millisecond),
		}
		batcher.shards[i] = shard
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-batcher.done:
					return
				case <-shard.ticker.C:
					shard.flush(context.Background(), batcher.delivery)
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(batcher.stopped)
	}()
	return batcher
}

func (b *groupNoticeBatcher) enqueue(groupID string, notice eventport.GroupMessageNoticeEvent) error {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(notice.ConversationID))
	shard := b.shards[int(hash.Sum32())%len(b.shards)]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.closed {
		return ErrGroupNoticeBatcherClosed
	}
	current, exists := shard.pending[notice.ConversationID]
	if !exists || notice.Seq > current.notice.Seq {
		shard.pending[notice.ConversationID] = pendingGroupNotice{groupID: groupID, notice: notice}
	}
	return nil
}

func (b *groupNoticeBatcher) close(ctx context.Context) {
	b.once.Do(func() {
		for _, shard := range b.shards {
			shard.mu.Lock()
			shard.closed = true
			shard.mu.Unlock()
			shard.ticker.Stop()
		}
		close(b.done)
	})

	select {
	case <-b.stopped:
		for _, shard := range b.shards {
			shard.flush(ctx, b.delivery)
		}
	case <-ctx.Done():
		log.Printf("等待群聊通知合并器关闭超时：%v", ctx.Err())
	}
}

func (s *groupNoticeShard) flush(ctx context.Context, delivery *ws.Gateway) {
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[string]pendingGroupNotice)
	s.mu.Unlock()

	for _, item := range pending {
		if ctx.Err() != nil {
			return
		}
		err := delivery.DeliverGroupMessageNotice(item.groupID, item.notice)
		if err != nil {
			log.Printf("推送群聊消息通知失败：conversation=%s seq=%d error=%v", item.notice.ConversationID, item.notice.Seq, err)
		}
	}
}
