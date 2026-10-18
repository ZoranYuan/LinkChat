package friend

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	conversationapp "IM_backend/internal/application/conversation"
	outboxport "IM_backend/internal/application/ports/outbox"
	friendcache "IM_backend/internal/application/ports/persistence/cache/friend"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	friendrepo "IM_backend/internal/application/ports/persistence/repository/friend"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	friendentity "IM_backend/internal/domain/friend/entity"
	messageentity "IM_backend/internal/domain/message/entity"
	"IM_backend/internal/infrastructure/id/snow"
	conversationredis "IM_backend/internal/infrastructure/persistence/redis/cache/conversation"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type creationTestConversations struct {
	conversationrepo.ConversationRepository
	record  *conversationentity.Conversation
	locked  bool
	created int
}

func (r *creationTestConversations) WithTx(any) conversationrepo.ConversationRepository { return r }
func (r *creationTestConversations) LockPrivateParticipants(context.Context, string, string) error {
	r.locked = true
	return nil
}
func (r *creationTestConversations) FindPrivateConversation(context.Context, string, string) (*conversationentity.Conversation, error) {
	if !r.locked {
		panic("lookup before participant lock")
	}
	return r.record, nil
}
func (r *creationTestConversations) CreateConversation(_ context.Context, record *conversationentity.Conversation) error {
	r.record = record
	r.created++
	return nil
}
func (r *creationTestConversations) ReserveSequenceRange(_ context.Context, _ string, count int, _ string) (int64, error) {
	return r.record.LatestSeq + 1, nil
}

type creationTestRequests struct {
	friendrepo.FriendRequestRepository
}

func (r creationTestRequests) WithTx(any) friendrepo.FriendRequestRepository { return r }
func (creationTestRequests) OperateRequest(string, int, int) error           { return nil }

type creationTestFriends struct{ friendrepo.FriendRepository }

func (r creationTestFriends) WithTx(any) friendrepo.FriendRepository { return r }
func (creationTestFriends) Create([]friendentity.Friend) error       { return nil }

type creationTestUserConversations struct {
	conversationrepo.UserConversationRepository
	ids []string
}

func (r *creationTestUserConversations) WithTx(any) conversationrepo.UserConversationRepository {
	return r
}
func (r *creationTestUserConversations) CreateUserConversation(_ context.Context, record *conversationentity.UserConversation) error {
	r.ids = append(r.ids, record.ConversationId)
	return nil
}

type creationTestMessages struct {
	messagerepo.MessageRepository
	records []*messageentity.Message
}

func (r *creationTestMessages) WithTx(any) messagerepo.MessageRepository { return r }
func (r *creationTestMessages) CreateNewMessage(_ context.Context, record *messageentity.Message) error {
	r.records = append(r.records, record)
	return nil
}

type creationTestOutbox struct{ outboxport.OutboxRepository }

func (r creationTestOutbox) WithTx(any) outboxport.OutboxRepository        { return r }
func (creationTestOutbox) Create(context.Context, *outboxport.Entry) error { return nil }

type creationTestFriendCache struct{ friendcache.FriendCache }

func (creationTestFriendCache) SetRelation(context.Context, string, string, *friendcache.RelationState) error {
	return nil
}

type creationTestTransaction struct {
	beforeCommit func()
	err          error
}

func (tx creationTestTransaction) WithinTransaction(_ context.Context, fn func(any) error) error {
	if err := fn(tx); err != nil {
		return err
	}
	tx.beforeCommit()
	return tx.err
}

func TestPrivateConversationCreationAndReuseCacheOnlyAfterCommit(t *testing.T) {
	for _, mode := range []string{"new", "reuse", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			cache := conversationredis.NewConversationCache(client)
			repo := &creationTestConversations{}
			if mode == "reuse" {
				repo.record = conversationentity.NewConversation("a_b", "a", "b", 1, 41, "old")
			}
			ids, err := snow.NewGenerator(7)
			if err != nil {
				t.Fatal(err)
			}
			userConversations, messages := &creationTestUserConversations{}, &creationTestMessages{}
			tx := creationTestTransaction{beforeCommit: func() {
				_, found, err := cache.GetConversationInfo(context.Background(), repo.record.ConversationId)
				if err != nil || found {
					t.Fatalf("cache populated before commit: found=%t err=%v", found, err)
				}
			}}
			if mode == "rollback" {
				tx.err = errors.New("commit failed")
			}
			app := &FriendRequestApplication{conversationRepository: repo, conversationApplication: conversationapp.NewConversationApplication(repo, cache, 10*time.Minute), userConvRepository: userConversations, friendRequestRepository: creationTestRequests{}, friendRepository: creationTestFriends{}, messageRepository: messages, outboxRepository: creationTestOutbox{}, friendCache: creationTestFriendCache{}, idGenerator: ids, txManager: tx}
			err = app.acceptFriendRequest(&friendentity.FriendRequest{RequestId: "request", ApplicantUserId: "a", PeerUserId: "b", Message: "hello"}, "b")
			info, found, cacheErr := cache.GetConversationInfo(context.Background(), repo.record.ConversationId)
			if mode == "rollback" {
				if err == nil || found || cacheErr != nil {
					t.Fatalf("rollback cached conversation: found=%t err=%v", found, err)
				}
				return
			}
			if err != nil || cacheErr != nil || !found || info == nil {
				t.Fatalf("info=%+v err=%v cacheErr=%v", info, err, cacheErr)
			}
			wantFirst := int64(1)
			if mode == "reuse" {
				wantFirst = 42
				if repo.created != 0 || info.ConversationID != "a_b" || repo.record.LatestSeq != 43 {
					t.Fatalf("existing conversation reset: %+v", repo.record)
				}
			} else if _, err := strconv.ParseInt(info.ConversationID, 10, 64); err != nil || repo.created != 1 {
				t.Fatalf("not a Snowflake ID: %s", info.ConversationID)
			}
			for i, message := range messages.records {
				if message.ConversationId != info.ConversationID || message.Seq != wantFirst+int64(i) {
					t.Fatalf("message=%+v", message)
				}
			}
			for _, id := range userConversations.ids {
				if id != info.ConversationID {
					t.Fatalf("user conversation ID=%s", id)
				}
			}
		})
	}
}
