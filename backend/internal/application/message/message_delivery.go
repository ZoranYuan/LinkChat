package message

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/cache/room"
	"IM_backend/internal/application/ports/realtime"
	roomapp "IM_backend/internal/application/room"
)

type MessageDelivery struct {
	realtime          realtime.RealtimeDelivery
	roomActivityState roomcache.ActivityState
	roomApplication   *roomapp.RoomApplication
	config            configs.MessageConfig
	messageBatcher    *largeRoomMessageBatcher
}

func NewMessageDelivery(
	delivery realtime.RealtimeDelivery,
	roomApplication *roomapp.RoomApplication,
	roomActivityState roomcache.ActivityState,
	config configs.MessageConfig,
) *MessageDelivery {
	if roomActivityState == nil {
		panic("房间 ACTIVE 状态未配置")
	}
	if config.RoomRealtimeFanoutLimit <= 0 ||
		config.LargeRoomNoticeLingerMilliseconds <= 0 ||
		config.LargeRoomNoticeShardCount <= 0 {
		panic("消息投递配置无效：请检查 message.room_realtime_fanout_limit、large_room_notice_linger_milliseconds 和 large_room_notice_shard_count")
	}

	return &MessageDelivery{
		realtime:          delivery,
		roomApplication:   roomApplication,
		roomActivityState: roomActivityState,
		config:            config,
		messageBatcher:    newLargeRoomMessageBatcher(delivery, config),
	}
}

func (delivery *MessageDelivery) Deliver(
	ctx context.Context,
	eventType string,
	event messaging.MessageEvent,
	payload []byte,
) error {
	switch event.ConvType {
	case messaging.PrivateChat:
		return delivery.realtime.DeliverToUser(eventType, event.RecvID, payload)
	case messaging.Room:
		return delivery.deliverRoomMessage(ctx, eventType, event, payload)
	default:
		return ErrUnknownConversationType
	}
}

func (delivery *MessageDelivery) deliverRoomMessage(
	ctx context.Context,
	eventType string,
	event messaging.MessageEvent,
	payload []byte,
) error {
	roomID := event.RecvID
	room, err := delivery.roomApplication.GetActiveRoom(ctx, roomID)
	if err != nil {
		return err
	}

	activePull := delivery.roomActivityState.IsActive(roomID, time.Now())
	// Consumer 只读本地 ACTIVE 标识，不查 Redis、不续期；大房间成员数仍是硬阈值。
	hardPull := room.MemberCount > delivery.config.RoomRealtimeFanoutLimit
	if hardPull || activePull {
		return delivery.messageBatcher.enqueue(roomID, messaging.MessageNotifyEvent{
			ConversationID: event.ConversationID, MessageID: event.MessageID, Seq: event.Seq,
		})
	}

	return delivery.realtime.DeliverToOnlineRoomMembers(
		eventType,
		roomID,
		payload,
		event.SenderID,
	)
}

func (delivery *MessageDelivery) Close(ctx context.Context) {
	delivery.messageBatcher.close(ctx)
}

type pendingRoomNotice struct {
	roomID string
	notice messaging.MessageNotifyEvent
}

type largeRoomMessageBatcher struct {
	delivery realtime.RoomMemberDelivery
	shards   []*largeRoomMessageBatchShard
	done     chan struct{}
	stopped  chan struct{}
	once     sync.Once
}

type largeRoomMessageBatchShard struct {
	mu      sync.Mutex
	pending map[string]pendingRoomNotice
	closed  bool
	ticker  *time.Ticker
	parent  *largeRoomMessageBatcher
}

var ErrLargeRoomMessageBatchClosed = errors.New("大型房间通知合并器已关闭")

func newLargeRoomMessageBatcher(delivery realtime.RoomMemberDelivery, config configs.MessageConfig) *largeRoomMessageBatcher {
	if time.Duration(config.LargeRoomNoticeLingerMilliseconds)*time.Millisecond <= 0 || config.LargeRoomNoticeShardCount <= 0 {
		panic("大型房间通知合并配置无效")
	}
	batcher := &largeRoomMessageBatcher{
		delivery: delivery, shards: make([]*largeRoomMessageBatchShard, config.LargeRoomNoticeShardCount),
		done: make(chan struct{}), stopped: make(chan struct{}),
	}
	var workers sync.WaitGroup
	for i := range batcher.shards {
		shard := &largeRoomMessageBatchShard{
			pending: make(map[string]pendingRoomNotice),
			ticker:  time.NewTicker(time.Duration(config.LargeRoomNoticeLingerMilliseconds) * time.Millisecond), parent: batcher,
		}
		batcher.shards[i] = shard
		workers.Add(1)
		go func() { defer workers.Done(); shard.run() }()
	}
	go func() { workers.Wait(); close(batcher.stopped) }()
	return batcher
}

func (batcher *largeRoomMessageBatcher) enqueue(roomID string, notice messaging.MessageNotifyEvent) error {
	return batcher.shardFor(notice.ConversationID).put(pendingRoomNotice{roomID: roomID, notice: notice})
}

func (batcher *largeRoomMessageBatcher) close(ctx context.Context) {
	batcher.once.Do(func() {
		for _, shard := range batcher.shards {
			shard.mu.Lock()
			shard.closed = true
			shard.mu.Unlock()
			shard.ticker.Stop()
		}
		close(batcher.done)
		select {
		case <-batcher.stopped:
			for _, shard := range batcher.shards {
				shard.flush(ctx)
			}
		case <-ctx.Done():
			log.Printf("等待大型房间通知合并器关闭超时：%v", ctx.Err())
		}
	})
}

func (batcher *largeRoomMessageBatcher) shardFor(key string) *largeRoomMessageBatchShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return batcher.shards[int(h.Sum32())%len(batcher.shards)]
}

func (shard *largeRoomMessageBatchShard) put(item pendingRoomNotice) error {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.closed {
		return ErrLargeRoomMessageBatchClosed
	}
	key := item.notice.ConversationID
	current, exists := shard.pending[key]
	if exists {
		if current.notice.Seq > item.notice.Seq {
			item.notice = current.notice
		}
	}
	shard.pending[key] = item
	return nil
}

func (shard *largeRoomMessageBatchShard) run() {
	for {
		select {
		case <-shard.parent.done:
			return
		case <-shard.ticker.C:
			shard.flush(context.Background())
		}
	}
}

func (shard *largeRoomMessageBatchShard) flush(ctx context.Context) {
	shard.mu.Lock()
	pending := shard.pending
	shard.pending = make(map[string]pendingRoomNotice)
	shard.mu.Unlock()
	for _, item := range pending {
		if ctx.Err() != nil {
			return
		}
		payload, err := json.Marshal(item.notice)
		if err == nil {
			// 合并批次可能包含多个发送者，所有在线成员都需要获知最新游标。
			err = shard.parent.delivery.DeliverToOnlineRoomMembers(messaging.TypeRoomMessageNotice, item.roomID, payload, "")
		}
		if err != nil {
			log.Printf("投递大型房间轻量提醒失败：room=%s seq=%d error=%v", item.notice.ConversationID, item.notice.Seq, err)
		}
	}
}
