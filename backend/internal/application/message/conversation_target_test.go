package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	friendrepo "IM_backend/internal/application/ports/persistence/repository/friend"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	friendentity "IM_backend/internal/domain/friend/entity"
	friendvo "IM_backend/internal/domain/friend/value_object"
	messageentity "IM_backend/internal/domain/message/entity"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"IM_backend/internal/shared/protocol"
)

type targetTestFriends struct {
	friendrepo.FriendRepository
	calls     int
	pairs     []friendrepo.RelationPair
	notFriend bool
}

func (r *targetTestFriends) FindRelationsByPairs(_ context.Context, pairs []friendrepo.RelationPair) ([]*friendentity.Friend, error) {
	r.calls++
	r.pairs = append([]friendrepo.RelationPair(nil), pairs...)
	relations := make([]*friendentity.Friend, 0, len(pairs))
	for _, pair := range pairs {
		status := friendvo.Friend
		if r.notFriend {
			status = friendvo.BlockOther
		}
		relations = append(relations, &friendentity.Friend{UserId: pair.UserID, FriendUserId: pair.FriendUserID, Status: status})
	}
	return relations, nil
}

type targetBatchRoomUsers struct {
	roomrepo.RoomUserRepository
	members map[string]*roomentity.RoomUser
	calls   int
	userIDs []string
}

func (r *targetBatchRoomUsers) ListRelationsByRoomAndUserIDs(_ context.Context, _ string, userIDs []string) ([]*roomentity.RoomUser, error) {
	r.calls++
	r.userIDs = append([]string(nil), userIDs...)
	members := make([]*roomentity.RoomUser, 0, len(userIDs))
	for _, userID := range userIDs {
		if member := r.members[userID]; member != nil {
			members = append(members, member)
		}
	}
	return members, nil
}

func TestPrivateSendDerivesPeerAndRejectsNonParticipant(t *testing.T) {
	for _, sender := range []string{"a", "b", "outsider"} {
		app, tx, _, outbox, _ := newSendTestApplication()
		app.friendRepository = &targetTestFriends{}
		app.conversationRepository.(*sendTestConversations).records = map[string]*conversationentity.Conversation{
			"private": {ConversationId: "private", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b"},
		}
		dto := sendTestDTO()
		dto.ConversationID, dto.SenderID = "private", sender
		dto.ReceiverID, dto.ConversationType = "forged-target", int(protocol.RoomChat)
		acks, err := app.HandleSendMessages(context.Background(), "private", []SendMessageDTO{dto})
		var ack *MessageAckDTO
		if len(acks) == 1 {
			ack = acks[0]
		}
		if sender == "outsider" {
			if err != nil || ack == nil || ack.Status != string(protocol.AckStatusFailed) || ack.Error != ErrForbidden.Error() || tx.calls != 0 || len(outbox.entries) != 0 {
				t.Fatalf("unauthorized send: ack=%+v err=%v", ack, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var event protocol.MessageEvent
		if err := json.Unmarshal(outbox.entries[0].Payload, &event); err != nil {
			t.Fatal(err)
		}
		wantPeer := "b"
		if sender == "b" {
			wantPeer = "a"
		}
		if event.RecvId != wantPeer || event.ConvType != protocol.PrivateChat || ack.ConversationID != "private" {
			t.Fatalf("event=%+v ack=%+v", event, ack)
		}
	}
}

func TestPrivateSendBatchLoadsDirectedFriendRelationsOnce(t *testing.T) {
	app, _, _, outbox, _ := newSendTestApplication()
	app.conversationRepository.(*sendTestConversations).records = map[string]*conversationentity.Conversation{
		"private": {ConversationId: "private", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b"},
	}
	friends := &targetTestFriends{}
	app.friendRepository = friends
	first, second := sendTestDTO(), sendTestDTO()
	first.ConversationID, first.SenderID, first.ClientMessageID = "private", "a", "from-a"
	second.ConversationID, second.SenderID, second.ClientMessageID = "private", "b", "from-b"

	acks, err := app.HandleSendMessages(context.Background(), "private", []SendMessageDTO{first, second})
	if err != nil || len(acks) != 2 || friends.calls != 1 || len(friends.pairs) != 2 || len(outbox.entries) != 2 {
		t.Fatalf("acks=%+v err=%v relation_calls=%d pairs=%v outboxes=%d", acks, err, friends.calls, friends.pairs, len(outbox.entries))
	}
}

func TestPrivateSendRejectsWholeBatchWhenPairIsNotFriend(t *testing.T) {
	app, tx, _, outbox, _ := newSendTestApplication()
	app.conversationRepository.(*sendTestConversations).records = map[string]*conversationentity.Conversation{
		"private": {ConversationId: "private", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b"},
	}
	app.friendRepository = &targetTestFriends{notFriend: true}
	first, second := sendTestDTO(), sendTestDTO()
	first.ConversationID, first.SenderID, first.ClientMessageID = "private", "a", "from-a"
	second.ConversationID, second.SenderID, second.ClientMessageID = "private", "b", "from-b"

	acks, err := app.HandleSendMessages(context.Background(), "private", []SendMessageDTO{first, second})
	if err != nil || len(acks) != 2 || tx.calls != 0 || len(outbox.entries) != 0 {
		t.Fatalf("non-friend private batch should not persist: acks=%+v err=%v tx=%d outboxes=%d", acks, err, tx.calls, len(outbox.entries))
	}
	for _, ack := range acks {
		if ack.Status != string(protocol.AckStatusFailed) || ack.Error != ErrNotFriends.Error() {
			t.Fatalf("private batch must fail every message: %+v", acks)
		}
	}
}

func TestRoomSendUsesStoredRoomAndDoesNotChangeConversationID(t *testing.T) {
	app, _, _, outbox, _ := newSendTestApplication()
	dto := sendTestDTO()
	dto.ReceiverID, dto.ConversationType = "forged-target", int(protocol.PrivateChat)
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{dto})
	var ack *MessageAckDTO
	if len(acks) == 1 {
		ack = acks[0]
	}
	if err != nil {
		t.Fatal(err)
	}
	var event protocol.MessageEvent
	if err := json.Unmarshal(outbox.entries[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if ack.ConversationID != "conversation" || event.ConversationId != "conversation" || event.RecvId != "room" || event.ConvType != protocol.RoomChat {
		t.Fatalf("ack=%+v event=%+v", ack, event)
	}
}

func TestRoomSendBatchChecksDistinctSendersOnceAndReturnsPerMessageFailure(t *testing.T) {
	app, _, _, outbox, _ := newSendTestApplication()
	roomUsers := &targetBatchRoomUsers{members: map[string]*roomentity.RoomUser{
		"sender": {UserId: "sender", RoomId: "room", Status: roomvo.Activate},
	}}
	app.roomUserRepository = roomUsers
	first, second := sendTestDTO(), sendTestDTO()
	first.ClientMessageID = "allowed"
	second.ClientMessageID = "not-member"
	second.SenderID = "outsider"

	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{first, second})
	if err != nil || len(acks) != 2 {
		t.Fatalf("acks=%+v err=%v", acks, err)
	}
	if roomUsers.calls != 1 || len(roomUsers.userIDs) != 2 {
		t.Fatalf("batch member query calls=%d user_ids=%v", roomUsers.calls, roomUsers.userIDs)
	}
	if acks[0].Status != string(protocol.AckStatusSent) || acks[1].Status != string(protocol.AckStatusFailed) || acks[1].Error != ErrNotRoomMember.Error() {
		t.Fatalf("per-message authorization acks=%+v", acks)
	}
	if len(outbox.entries) != 1 {
		t.Fatalf("only authorized messages should persist, outboxes=%d", len(outbox.entries))
	}
}

func TestMissingConversationDoesNotPersistOrNotify(t *testing.T) {
	app, tx, _, outbox, notifier := newSendTestApplication()
	app.conversationRepository.(*sendTestConversations).records = map[string]*conversationentity.Conversation{}
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{sendTestDTO()})
	if !errors.Is(err, ErrConversationNotFound) || len(acks) != 0 || tx.calls != 0 || len(outbox.entries) != 0 || notifier.calls.Load() != 0 {
		t.Fatalf("acks=%+v err=%v", acks, err)
	}
}

type targetTestSyncCache struct {
	roomcache.RoomCache
	roomID string
}

type targetTestRoomUsers struct{ roomrepo.RoomUserRepository }

func (c *targetTestSyncCache) ListMessageAfterSeq(_ context.Context, roomID string, _, _ int64) (roomcache.RecentMessageRange, error) {
	c.roomID = roomID
	return roomcache.RecentMessageRange{Covered: true, Events: []protocol.MessageEvent{{ConversationId: "conversation", RecvId: "room", MessageId: "m7", Seq: 7}}}, nil
}

func TestSyncReadsLatestSequenceSeparatelyAndUsesRoomCacheKey(t *testing.T) {
	app, _, _, _, _ := newSendTestApplication()
	cache := &targetTestSyncCache{}
	app.roomCache = cache
	app.roomUserRepository = &targetTestRoomUsers{}
	messages, err := app.SyncMessages(context.Background(), "conversation", "sender", 6)
	if err != nil || cache.roomID != "room" || len(messages) != 1 || messages[0].ConversationID != "conversation" || messages[0].Seq != 7 {
		t.Fatalf("room=%s messages=%+v err=%v", cache.roomID, messages, err)
	}
}

func TestMessageCacheBackfillKeepsRoomAndConversationSeparate(t *testing.T) {
	events := messageAppDTOsToEvents([]MessageDTO{{MessageID: "m", ConversationID: "conversation", Seq: 2}}, "conversation", "room")
	if len(events) != 1 || events[0].ConversationId != "conversation" || events[0].RecvId != "room" {
		t.Fatalf("events=%+v", events)
	}
}

func (r *sendTestMessages) FindByMessageID(context.Context, string) (*messageentity.Message, error) {
	return r.message, nil
}
func (r *sendTestUserConversations) GetUserConversation(_ context.Context, userID, conversationID string) (*conversationentity.UserConversation, error) {
	return conversationentity.BuildUserConversation(userID, conversationID, 0), nil
}
func (r *sendTestUserConversations) AdvanceReadSeq(context.Context, *conversationentity.UserConversation) (bool, error) {
	return true, nil
}

func TestReadAckUsesDatabaseLatestSeqNotStableCache(t *testing.T) {
	app, _, messages, outbox, _ := newSendTestApplication()
	app.conversationRepository.(*sendTestConversations).records = map[string]*conversationentity.Conversation{"private": {ConversationId: "private", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b", LatestSeq: 7}}
	messages.message = &messageentity.Message{MessageId: "m7", ConversationId: "private", SenderId: "a", Seq: 7}
	if err := app.HandleReadMessage(context.Background(), "b", "m7"); err != nil {
		t.Fatal(err)
	}
	if len(outbox.entries) != 1 {
		t.Fatalf("outboxes=%d", len(outbox.entries))
	}
	var event protocol.MessageReadCommittedEvent
	if err := json.Unmarshal(outbox.entries[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if event.ConversationId != "private" || event.RecipientUserId != "a" || event.LastReadSeq != 7 {
		t.Fatalf("event=%+v", event)
	}
}
