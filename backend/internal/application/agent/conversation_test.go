package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	summaryrepo "IM_backend/internal/application/ports/persistence/repository/summary"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	messageentity "IM_backend/internal/domain/message/entity"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
)

type summaryTestRoomUsers struct{ roomrepo.RoomUserRepository }

func (summaryTestRoomUsers) GetRelationByIDs(string, string) (*roomentity.RoomUser, error) {
	return &roomentity.RoomUser{Status: roomvo.Activate}, nil
}

type summaryTestRuns struct {
	summaryrepo.SummaryRunRepository
}

func (summaryTestRuns) FindActiveRunByScope(context.Context, string, string) (*summaryrepo.RunRecord, error) {
	return nil, nil
}

type summaryTestConversations struct {
	conversationrepo.ConversationRepository
	calls int
}

func (r *summaryTestConversations) FindRoomConversation(_ context.Context, roomID string) (*conversationentity.Conversation, error) {
	r.calls++
	return &conversationentity.Conversation{ConversationId: "conversation", Convtype: 2, RoomId: roomID, LatestSeq: 20}, nil
}

type summaryTestUserConversations struct {
	conversationrepo.UserConversationRepository
	id string
}

func (r *summaryTestUserConversations) GetUserConversation(_ context.Context, userID, conversationID string) (*conversationentity.UserConversation, error) {
	r.id = conversationID
	return conversationentity.BuildUserConversation(userID, conversationID, 10), nil
}

type summaryTestSnapshots struct {
	summarycache.RoomUnreadSnapshotCache
	roomID string
}

func (c *summaryTestSnapshots) GetActive(context.Context, string, string) (*summarycache.RoomUnreadSnapshot, bool, error) {
	return nil, false, nil
}
func (c *summaryTestSnapshots) SetActive(_ context.Context, _, roomID string, _ summarycache.RoomUnreadSnapshot, _ time.Duration) error {
	c.roomID = roomID
	return nil
}

type summaryTestMessages struct {
	messagerepo.MessageRepository
	id       string
	from, to int64
	err      error
}

func (r *summaryTestMessages) ListBySeqRange(_ context.Context, id string, from, to int64) ([]*messageentity.Message, error) {
	r.id, r.from, r.to = id, from, to
	return nil, r.err
}

func TestSummaryQueriesMessagesByConversationNotRoomID(t *testing.T) {
	conversations, userConversations, snapshots := &summaryTestConversations{}, &summaryTestUserConversations{}, &summaryTestSnapshots{}
	queryErr := errors.New("stop after inspecting query")
	messages := &summaryTestMessages{err: queryErr}
	app := &SummaryApplication{conversations: conversations, userConversations: userConversations, messages: messages, snapshots: snapshots, roomUsers: summaryTestRoomUsers{}, runs: summaryTestRuns{}}
	if _, err := app.InitSummaryRun(context.Background(), "room", "user"); !errors.Is(err, queryErr) {
		t.Fatalf("err=%v", err)
	}
	if conversations.calls != 1 || userConversations.id != "conversation" || messages.id != "conversation" || messages.from != 11 || messages.to != 20 || snapshots.roomID != "room" {
		t.Fatalf("queries: calls=%d user=%s message=%+v snapshot=%s", conversations.calls, userConversations.id, messages, snapshots.roomID)
	}
}
