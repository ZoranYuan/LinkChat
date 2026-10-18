package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomapp "IM_backend/internal/application/room"
	"IM_backend/internal/shared/protocol"
)

type policyTestRealtime struct{ calls int }

func (*policyTestRealtime) DeliverToUser(string, string, []byte) error { return nil }
func (d *policyTestRealtime) DeliverToOnlineRoomMembers(string, string, []byte, string) error {
	d.calls++
	return nil
}

func TestDeliveryUsesLocalActiveHoldAndKeepsLargeRoomsPulling(t *testing.T) {
	for _, test := range []struct {
		name     string
		members  int
		markedAt time.Duration
		marked   bool
		wantPull bool
	}{
		{name: "normal", members: 10},
		{name: "active", members: 10, marked: true, wantPull: true},
		{name: "expired", members: 10, marked: true, markedAt: -3 * time.Minute},
		{name: "large_expired", members: 501, marked: true, markedAt: -3 * time.Minute, wantPull: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := roomapp.NewRoomActivityState(2 * time.Minute)
			if test.marked {
				state.MarkActive("room", time.Now().Add(test.markedAt))
			}
			realtime := &policyTestRealtime{}
			delivery := NewMessageDelivery(realtime, sendTestRoomRepository{members: test.members}, state,
				configs.MessageConfig{RoomRealtimeFanoutLimit: 500, LargeRoomNoticeLingerMilliseconds: 3600000, LargeRoomNoticeShardCount: 1})
			t.Cleanup(func() { delivery.Close(context.Background()) })
			err := delivery.Deliver(context.Background(), protocol.EventTypeSendMessage,
				protocol.MessageEvent{SenderId: "sender", RecvId: "room", ConvType: protocol.RoomChat, ConversationId: "room", Seq: 1}, []byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			if (realtime.calls == 0) != test.wantPull {
				t.Fatalf("pull=%v immediate fanout calls=%d", test.wantPull, realtime.calls)
			}
			if test.wantPull {
				delivery.messageBatcher.shards[0].flush(context.Background())
				if realtime.calls != 1 {
					t.Fatal("pull notice was not delivered")
				}
			}
		})
	}
}

func TestCommittedActiveMessageImmediatelyChangesConsumerPolicy(t *testing.T) {
	app, tx, _, _, _ := newSendTestApplication()
	cache := &sendTestRoomCache{tx: tx, level: roomcache.RoomActivityActive}
	app.roomCache = cache
	app.roomRepository = sendTestRoomRepository{members: 10}
	app.config.Message.RoomRealtimeFanoutLimit = 500
	realtime := &policyTestRealtime{}
	delivery := NewMessageDelivery(realtime, app.roomRepository, app.roomActivityState,
		configs.MessageConfig{RoomRealtimeFanoutLimit: 500, LargeRoomNoticeLingerMilliseconds: 3600000, LargeRoomNoticeShardCount: 1})
	t.Cleanup(func() { delivery.Close(context.Background()) })
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{sendTestDTO()})
	if err != nil || len(acks) != 1 {
		t.Fatal(err)
	}
	ack := acks[0]
	if len(cache.warmed) != 1 || cache.warmed[0].Seq != ack.Seq {
		t.Fatal("committed ACTIVE message was not warmed")
	}
	if err := delivery.Deliver(context.Background(), protocol.EventTypeSendMessage, cache.warmed[0], []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if realtime.calls != 0 {
		t.Fatal("consumer failed to observe sender's shared ACTIVE state")
	}
	delivery.messageBatcher.shards[0].flush(context.Background())
	if realtime.calls != 1 {
		t.Fatal("shared ACTIVE state did not produce a pull notice")
	}
}

type noticeTestDelivery func(string, string, []byte, string) error

func (f noticeTestDelivery) DeliverToOnlineRoomMembers(eventType, roomID string, payload []byte, exclude string) error {
	return f(eventType, roomID, payload, exclude)
}

func newNoticeTestBatcher(t *testing.T, delivery noticeTestDelivery, linger time.Duration) *largeRoomMessageBatcher {
	t.Helper()
	batcher := newLargeRoomMessageBatcher(delivery, LargeRoomMessageBatchOptions{Linger: linger, ShardCount: 1})
	t.Cleanup(func() { batcher.close(context.Background()) })
	return batcher
}

func noticeTestEvent(seq int64) protocol.MessageNotifyEvent {
	return protocol.MessageNotifyEvent{ConversationId: "room", MessageId: "message", Seq: seq}
}

func TestRoomNoticeTimerBroadcastsOnlyLatestMessage(t *testing.T) {
	delivered := make(chan protocol.MessageNotifyEvent, 4)
	batcher := newNoticeTestBatcher(t, func(eventType, roomID string, payload []byte, exclude string) error {
		if eventType != protocol.EventRoomMessageNotice || roomID != "room" || exclude != "" {
			t.Errorf("unexpected routing: event=%s room=%s exclude=%s", eventType, roomID, exclude)
		}
		var notice protocol.MessageNotifyEvent
		if err := json.Unmarshal(payload, &notice); err != nil {
			return err
		}
		delivered <- notice
		return nil
	}, 100*time.Millisecond)
	for _, seq := range []int64{100, 102, 101} {
		notice := noticeTestEvent(seq)
		if seq == 102 {
			notice.MessageId = "latest"
		}
		if err := batcher.enqueue("room", notice); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case notice := <-delivered:
		if notice.Seq != 102 || notice.MessageId != "latest" {
			t.Fatalf("notice=%+v", notice)
		}
	case <-time.After(time.Second):
		t.Fatal("timer did not send latest notice")
	}
	select {
	case notice := <-delivered:
		t.Fatalf("same window broadcast more than once: %+v", notice)
	case <-time.After(120 * time.Millisecond):
	}
}

func TestRoomNoticeCoalescesMessagesAndAcceptsAdditionalRooms(t *testing.T) {
	batcher := newNoticeTestBatcher(t, func(string, string, []byte, string) error { return nil }, time.Hour)
	for seq := int64(1); seq <= 2000; seq++ {
		if err := batcher.enqueue("room", noticeTestEvent(seq)); err != nil {
			t.Fatalf("existing room update rejected: %v", err)
		}
	}
	shard := batcher.shards[0]
	shard.mu.Lock()
	count, latest := len(shard.pending), shard.pending["room"].notice.Seq
	shard.mu.Unlock()
	if count != 1 || latest != 2000 {
		t.Fatalf("pending rooms=%d latest=%d", count, latest)
	}
	other := noticeTestEvent(1)
	other.ConversationId = "another-room"
	if err := batcher.enqueue("another-room", other); err != nil {
		t.Fatalf("additional room should be accepted: %v", err)
	}
}

func TestRoomNoticeFailureMergesWithNewerPendingNotice(t *testing.T) {
	var batcher *largeRoomMessageBatcher
	calls := 0
	var delivered protocol.MessageNotifyEvent
	batcher = newNoticeTestBatcher(t, func(_, _ string, payload []byte, _ string) error {
		calls++
		if calls == 1 {
			if err := batcher.enqueue("room", noticeTestEvent(200)); err != nil {
				t.Fatal(err)
			}
			return errors.New("Redis unavailable")
		}
		return json.Unmarshal(payload, &delivered)
	}, time.Hour)
	if err := batcher.enqueue("room", noticeTestEvent(100)); err != nil {
		t.Fatal(err)
	}
	batcher.shards[0].flush(context.Background())
	batcher.shards[0].flush(context.Background())
	if calls != 2 || delivered.Seq != 200 {
		t.Fatalf("calls=%d latest=%d", calls, delivered.Seq)
	}
}

func TestRoomNoticeCloseFlushesAndRejectsFurtherInput(t *testing.T) {
	calls := 0
	batcher := newNoticeTestBatcher(t, func(string, string, []byte, string) error { calls++; return nil }, time.Hour)
	if err := batcher.enqueue("room", noticeTestEvent(100)); err != nil {
		t.Fatal(err)
	}
	batcher.close(context.Background())
	if calls != 1 {
		t.Fatalf("shutdown broadcasts=%d", calls)
	}
	if err := batcher.enqueue("room", noticeTestEvent(101)); !errors.Is(err, ErrLargeRoomMessageBatchClosed) {
		t.Fatalf("closed enqueue=%v", err)
	}
	batcher.close(context.Background())
	if calls != 1 {
		t.Fatal("shutdown flushed twice")
	}
}

func TestLargeRoomNoticeRoutesToRoomWithIndependentConversationID(t *testing.T) {
	var target string
	var received protocol.MessageNotifyEvent
	batcher := newNoticeTestBatcher(t, func(_ string, roomID string, payload []byte, _ string) error {
		target = roomID
		return json.Unmarshal(payload, &received)
	}, time.Hour)
	notice := protocol.MessageNotifyEvent{ConversationId: "conversation", MessageId: "m1", Seq: 10}
	if err := batcher.enqueue("room", notice); err != nil {
		t.Fatal(err)
	}
	batcher.shards[0].flush(context.Background())
	if target != "room" || received.ConversationId != "conversation" {
		t.Fatalf("target=%s notice=%+v", target, received)
	}
}
