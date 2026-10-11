package message

import (
	"IM_backend/configs"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	"IM_backend/internal/application/ports/realtime"
	roomapp "IM_backend/internal/application/room"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"IM_backend/internal/shared/diagnostics"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"sync"
	"time"
)

type LargeRoomMessageBatchOptions struct {
	Linger     time.Duration
	ShardCount int
}

type MessageDelivery struct {
	realtime          realtime.RealtimeDelivery
	roomActivityState *roomapp.RoomActivityState
	roomApplication   *roomapp.RoomApplication
	roomRepository    roomrepo.RoomRepository
	config            configs.MessageConfig
	messageBatcher    *largeRoomMessageBatcher
}

func (delivery *MessageDelivery) SetRoomApplication(application *roomapp.RoomApplication) {
	if delivery != nil {
		delivery.roomApplication = application
	}
}

func NewMessageDelivery(
	delivery realtime.RealtimeDelivery,
	roomRepository roomrepo.RoomRepository,
	roomActivityState *roomapp.RoomActivityState,
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
		roomRepository:    roomRepository,
		roomActivityState: roomActivityState,
		config:            config,
		messageBatcher: newLargeRoomMessageBatcher(delivery, LargeRoomMessageBatchOptions{
			Linger:     time.Duration(config.LargeRoomNoticeLingerMilliseconds) * time.Millisecond,
			ShardCount: config.LargeRoomNoticeShardCount,
		}),
	}
}

func (delivery *MessageDelivery) Deliver(
	ctx context.Context,
	eventType string,
	conversationID string,
	envelope protocol.Envelope,
	event protocol.MessageEvent,
) error {
	switch event.ConvType {
	case protocol.PrivateChat:
		return delivery.deliverPrivateMessage(eventType, envelope)
	case protocol.RoomChat:
		return delivery.deliverRoomMessage(ctx, eventType, conversationID, envelope, event)
	default:
		return ErrUnknownConversationType
	}
}

func (delivery *MessageDelivery) deliverPrivateMessage(
	eventType string,
	envelope protocol.Envelope,
) error {
	return delivery.realtime.DeliverToUser(eventType, envelope.To, envelope.Payload)
}

func (delivery *MessageDelivery) deliverRoomMessage(
	ctx context.Context,
	eventType string,
	roomID string,
	envelope protocol.Envelope,
	event protocol.MessageEvent,
) error {
	startedAt := time.Now()
	roomLookupStartedAt := time.Now()
	var room *roomentity.Room
	var err error
	if delivery.roomApplication != nil {
		room, err = delivery.roomApplication.GetRoomInfo(ctx, roomID)
		if err == nil && room != nil && room.Status != roomvo.Normal {
			err = roomentity.ErrRoomNotFound
		}
	} else {
		room, err = delivery.roomRepository.FindActiveRoom(roomID, int(roomvo.Normal))
	}
	roomLookupDuration := time.Since(roomLookupStartedAt)
	if err != nil {
		diagnostics.Logf("stage=room_delivery message_id=%s seq=%d room_id=%s room_lookup_us=%d outcome=room_lookup_error error=%q",
			event.MessageId, event.Seq, roomID, roomLookupDuration.Microseconds(), err.Error())
		return err
	}
	if room == nil {
		diagnostics.Logf("stage=room_delivery message_id=%s seq=%d room_id=%s room_lookup_us=%d outcome=room_missing",
			event.MessageId, event.Seq, roomID, roomLookupDuration.Microseconds())
		return roomentity.ErrRoomNotFound
	}

	policyStartedAt := time.Now()
	activePull := delivery.roomActivityState.IsActive(roomID, policyStartedAt)
	policyDuration := time.Since(policyStartedAt)
	// Consumer 只读本地 ACTIVE 标识，不查 Redis、不续期；大群成员数仍是硬阈值。
	hardPull := room.MemberCount > delivery.config.RoomRealtimeFanoutLimit
	if hardPull || activePull {
		noticeStartedAt := time.Now()
		err := delivery.deliverLargeRoomNotice(roomID, event)
		diagnostics.Logf("stage=room_delivery message_id=%s seq=%d room_id=%s room_lookup_us=%d policy_us=%d notice_enqueue_us=%d total_us=%d member_count=%d active=%t mode=light_notice outcome=%s",
			event.MessageId, event.Seq, roomID, roomLookupDuration.Microseconds(), policyDuration.Microseconds(), time.Since(noticeStartedAt).Microseconds(),
			time.Since(startedAt).Microseconds(), room.MemberCount, activePull, deliveryOutcome(err))
		return err
	}

	fanoutStartedAt := time.Now()
	err = delivery.realtime.DeliverToOnlineRoomMembers(
		eventType,
		roomID,
		envelope.Payload,
		envelope.From,
	)
	diagnostics.Logf("stage=room_delivery message_id=%s seq=%d room_id=%s room_lookup_us=%d policy_us=%d fanout_us=%d total_us=%d member_count=%d active=%t mode=fanout outcome=%s",
		event.MessageId, event.Seq, roomID, roomLookupDuration.Microseconds(), policyDuration.Microseconds(), time.Since(fanoutStartedAt).Microseconds(),
		time.Since(startedAt).Microseconds(), room.MemberCount, activePull, deliveryOutcome(err))
	return err
}

// 大群只合并最新消息游标，不保存完整消息或预热缓存。
func (delivery *MessageDelivery) deliverLargeRoomNotice(roomID string, event protocol.MessageEvent) error {
	startedAt := time.Now()
	err := delivery.messageBatcher.enqueue(protocol.MessageNotifyEvent{
		ConversationId: roomID, MessageId: event.MessageId, Seq: event.Seq,
	})
	diagnostics.Logf("stage=large_room_notice_enqueue message_id=%s seq=%d room_id=%s enqueue_us=%d outcome=%s",
		event.MessageId, event.Seq, roomID, time.Since(startedAt).Microseconds(), deliveryOutcome(err))
	return err
}

func deliveryOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func (delivery *MessageDelivery) Close(ctx context.Context) {
	delivery.messageBatcher.close(ctx)
}

type pendingRoomNotice struct {
	notice          protocol.MessageNotifyEvent
	firstEnqueuedAt time.Time
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

var ErrLargeRoomMessageBatchClosed = errors.New("大群通知合并器已关闭")

func newLargeRoomMessageBatcher(delivery realtime.RoomMemberDelivery, options LargeRoomMessageBatchOptions) *largeRoomMessageBatcher {
	if options.Linger <= 0 || options.ShardCount <= 0 {
		panic("大群通知合并配置无效")
	}
	batcher := &largeRoomMessageBatcher{
		delivery: delivery, shards: make([]*largeRoomMessageBatchShard, options.ShardCount),
		done: make(chan struct{}), stopped: make(chan struct{}),
	}
	var workers sync.WaitGroup
	for i := range batcher.shards {
		shard := &largeRoomMessageBatchShard{
			pending: make(map[string]pendingRoomNotice),
			ticker:  time.NewTicker(options.Linger), parent: batcher,
		}
		batcher.shards[i] = shard
		workers.Add(1)
		go func() { defer workers.Done(); shard.run() }()
	}
	go func() { workers.Wait(); close(batcher.stopped) }()
	return batcher
}

func (batcher *largeRoomMessageBatcher) enqueue(notice protocol.MessageNotifyEvent) error {
	return batcher.shardFor(notice.ConversationId).put(pendingRoomNotice{notice: notice, firstEnqueuedAt: time.Now()})
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
			log.Printf("等待大群通知合并器关闭超时：%v", ctx.Err())
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
	key := item.notice.ConversationId
	current, exists := shard.pending[key]
	if exists {
		if current.notice.Seq > item.notice.Seq {
			item.notice = current.notice
		}
		if current.firstEnqueuedAt.Before(item.firstEnqueuedAt) {
			item.firstEnqueuedAt = current.firstEnqueuedAt
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
		startedAt := time.Now()
		payload, err := json.Marshal(item.notice)
		if err == nil {
			// 合并批次可能包含多个发送者，所有在线成员都需要获知最新游标。
			err = shard.parent.delivery.DeliverToOnlineRoomMembers(protocol.EventRoomMessageNotice, item.notice.ConversationId, payload, "")
		}
		if err != nil {
			log.Printf("投递大群轻量提醒失败：room=%s seq=%d error=%v", item.notice.ConversationId, item.notice.Seq, err)
			if retryErr := shard.put(item); retryErr != nil {
				log.Printf("重新合并大群提醒失败：room=%s seq=%d error=%v", item.notice.ConversationId, item.notice.Seq, retryErr)
			}
		}
		diagnostics.Logf("stage=large_room_notice_flush room_id=%s seq=%d linger_us=%d total_us=%d outcome=%s",
			item.notice.ConversationId, item.notice.Seq, startedAt.Sub(item.firstEnqueuedAt).Microseconds(), time.Since(startedAt).Microseconds(), deliveryOutcome(err))
	}
}
