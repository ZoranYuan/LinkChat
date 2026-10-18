package mysql

import (
	"bytes"
	"encoding/json"
	"testing"

	"IM_backend/internal/shared/protocol"
)

func TestMigrateStoredEventsPreservesBusinessDataAndIsRepeatable(t *testing.T) {
	for _, test := range []struct {
		name, eventType, from, to string
		event                     any
		wrapped                   bool
	}{
		{name: "聊天", eventType: protocol.EventTypeSendMessage, from: "a", to: "b", wrapped: true,
			event: protocol.MessageEvent{MessageId: "m", ConversationId: "c", SenderId: "a", RecvId: "b", Seq: 1, Content: "你好", ConvType: protocol.PrivateChat}},
		{name: "已读", eventType: protocol.EventReadMessageCommitted, to: "b", wrapped: true,
			event: protocol.MessageReadCommittedEvent{ReaderId: "a", ConversationId: "c", LastReadSeq: 101, ConvType: protocol.PrivateChat}},
		{name: "好友", eventType: protocol.EventFriendRequestCreated, from: "a", to: "b", wrapped: true,
			event: protocol.FriendRequestCreatedEvent{RequestId: "r", ApplicantUserId: "a", PeerUserId: "b", Message: "同学", ApplyTime: 123}},
		{name: "成员", eventType: protocol.EventRoomMemberChanged,
			event: protocol.RoomMemberChangedEvent{RoomID: "room", UserID: "a", Status: 1, Version: 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.event)
			if err != nil {
				t.Fatal(err)
			}
			if test.wrapped {
				payload, err = json.Marshal(struct {
					From, To string
					Payload  []byte
				}{test.from, test.to, payload})
				if err != nil {
					t.Fatal(err)
				}
			}
			converted, err := migrateStoredEventPayload(test.eventType, payload)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(converted, &fields); err != nil {
				t.Fatal(err)
			}
			if _, wrapped := fields["Payload"]; wrapped {
				t.Fatal("迁移后不能再包含旧包装")
			}
			if test.eventType == protocol.EventReadMessageCommitted {
				var event protocol.MessageReadCommittedEvent
				if err := json.Unmarshal(converted, &event); err != nil || event.RecipientUserId != test.to || event.LastReadSeq != 101 {
					t.Fatalf("迁移必须保留已读水位及接收者：%+v err=%v", event, err)
				}
			} else {
				want, _ := json.Marshal(test.event)
				if !bytes.Equal(want, converted) {
					t.Fatalf("业务数据发生变化：got=%s want=%s", converted, want)
				}
			}
			repeated, err := migrateStoredEventPayload(test.eventType, converted)
			if err != nil || !bytes.Equal(converted, repeated) {
				t.Fatalf("重复迁移不能改变数据：err=%v", err)
			}
		})
	}
}

func TestMigrateStoredEventsRejectsMissingDataAndUnknownTypes(t *testing.T) {
	for _, test := range []struct{ eventType, payload string }{
		{protocol.EventTypeSendMessage, "{}"},
		{protocol.EventReadMessageCommitted, `{"readerId":"a","conversationId":"c","lastReadSeq":101}`},
		{protocol.EventFriendRequestCreated, "{}"},
		{protocol.EventRoomMemberChanged, "{}"},
		{"unknown", "{}"},
		{protocol.EventTypeSendMessage, "not-json"},
	} {
		if _, err := migrateStoredEventPayload(test.eventType, []byte(test.payload)); err == nil {
			t.Fatalf("无效事件必须阻止迁移：type=%s", test.eventType)
		}
	}
}
