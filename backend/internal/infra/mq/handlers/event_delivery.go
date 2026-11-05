package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	"IM_backend/internal/application/ports/messaging"
	roomapp "IM_backend/internal/application/room"
	"IM_backend/internal/interfaces/ws"
)

type MessageDelivery struct {
	gateway         *ws.Gateway
	roomApplication *roomapp.RoomApplication
	config          configs.MessageConfig
	noticeBatcher   *roomNoticeBatcher
}

func NewMessageDelivery(
	delivery *ws.Gateway,
	roomApplication *roomapp.RoomApplication,
	config configs.MessageConfig,
) *MessageDelivery {
	return &MessageDelivery{
		gateway:         delivery,
		roomApplication: roomApplication,
		config:          config,
		noticeBatcher:   newRoomNoticeBatcher(delivery, config),
	}
}

func (m *MessageDelivery) OnlineRoomConnectionCount(roomID string) int {
	return m.gateway.OnlineRoomConnectionCount(roomID)
}

func (m *MessageDelivery) Deliver(
	ctx context.Context,
	eventType string,
	event messaging.MessagePushEvent,
	payload []byte,
	active bool,
) error {
	switch event.ConvType {
	case messaging.PrivateChat:
		// 消息入库后把带 seq 的消息回推给发送者，用 clientMsgId 更新本地占位消息。
		if err := m.gateway.DeliverToUser(eventType, event.SenderID, payload); err != nil {
			return err
		}
		if event.RecvID == event.SenderID {
			return nil
		}
		return m.gateway.DeliverToUser(eventType, event.RecvID, payload)
	case messaging.RoomChat:
		// 大群也给发送者推完整消息；群内其他成员仍按原推拉策略接收。
		if err := m.gateway.DeliverToUser(eventType, event.SenderID, payload); err != nil {
			return err
		}
		return m.deliverRoomMessage(ctx, eventType, event, payload, active)
	default:
		return conversationapp.ErrUnknownConversationType
	}
}

func (m *MessageDelivery) deliverRoomMessage(
	ctx context.Context,
	eventType string,
	event messaging.MessagePushEvent,
	payload []byte,
	active bool,
) error {
	roomID := event.RecvID
	room, err := m.roomApplication.GetActiveRoom(ctx, roomID)
	if err != nil {
		return err
	}

	large := room.MemberCount > m.config.RoomRealtimeFanoutLimit
	if large || active {
		return m.noticeBatcher.enqueue(roomID, messaging.RoomMessageNoticeEvent{
			ConversationID: event.ConversationID,
			MessageID:      event.MessageID,
			Seq:            event.Seq,
		})
	}

	return m.gateway.DeliverToOnlineRoomMembers(eventType, roomID, payload, event.SenderID)
}

func (m *MessageDelivery) Close(ctx context.Context) {
	m.noticeBatcher.close(ctx)
}

type pendingRoomNotice struct {
	roomID string
	notice messaging.RoomMessageNoticeEvent
}

type roomNoticeBatcher struct {
	delivery *ws.Gateway
	shards   []*roomNoticeShard
	done     chan struct{}
	stopped  chan struct{}
	once     sync.Once
}

type roomNoticeShard struct {
	mu      sync.Mutex
	pending map[string]pendingRoomNotice
	closed  bool
	ticker  *time.Ticker
}

var ErrRoomNoticeBatcherClosed = errors.New("群聊通知合并器已关闭")

func newRoomNoticeBatcher(delivery *ws.Gateway, config configs.MessageConfig) *roomNoticeBatcher {
	batcher := &roomNoticeBatcher{
		delivery: delivery,
		shards:   make([]*roomNoticeShard, config.LargeRoomNoticeShardCount),
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	var workers sync.WaitGroup
	for i := range batcher.shards {
		shard := &roomNoticeShard{
			pending: make(map[string]pendingRoomNotice),
			ticker:  time.NewTicker(time.Duration(config.LargeRoomNoticeLingerMilliseconds) * time.Millisecond),
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

func (b *roomNoticeBatcher) enqueue(roomID string, notice messaging.RoomMessageNoticeEvent) error {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(notice.ConversationID))
	shard := b.shards[int(hash.Sum32())%len(b.shards)]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.closed {
		return ErrRoomNoticeBatcherClosed
	}
	current, exists := shard.pending[notice.ConversationID]
	if !exists || notice.Seq > current.notice.Seq {
		shard.pending[notice.ConversationID] = pendingRoomNotice{roomID: roomID, notice: notice}
	}
	return nil
}

func (b *roomNoticeBatcher) close(ctx context.Context) {
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

func (s *roomNoticeShard) flush(ctx context.Context, delivery *ws.Gateway) {
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[string]pendingRoomNotice)
	s.mu.Unlock()

	for _, item := range pending {
		if ctx.Err() != nil {
			return
		}
		payload, err := json.Marshal(item.notice)
		if err == nil {
			err = delivery.DeliverToOnlineRoomMembers(messaging.TypeRoomMessageNotice, item.roomID, payload, "")
		}
		if err != nil {
			log.Printf("推送群聊消息通知失败：conversation=%s seq=%d error=%v", item.notice.ConversationID, item.notice.Seq, err)
		}
	}
}
