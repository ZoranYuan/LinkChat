package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	conversationredis "IM_backend/internal/infrastructure/persistence/redis/cache/conversation"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type infoTestRepository struct {
	conversationrepo.ConversationRepository
	record *conversationentity.Conversation
	err    error
	calls  int
}

func (r *infoTestRepository) GetByID(context.Context, string) (*conversationentity.Conversation, error) {
	r.calls++
	return r.record, r.err
}

func newInfoTestApplication(t *testing.T) (*ConversationApplication, *infoTestRepository, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	repo := &infoTestRepository{}
	return NewConversationApplication(repo, conversationredis.NewConversationCache(client), 10*time.Minute), repo, server, client
}

func TestConversationInfoCacheContainsOnlyStableFields(t *testing.T) {
	app, repo, server, _ := newInfoTestApplication(t)
	repo.record = &conversationentity.Conversation{ConversationId: "2099000000000000001", Convtype: conversationvo.RoomChat, UserId1: "owner", RoomId: "room", LatestSeq: 100, LatestMessageId: "message"}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		info, err := app.GetConversationInfo(ctx, repo.record.ConversationId)
		if err != nil || info.RoomID != "room" || info.ConversationID == info.RoomID {
			t.Fatalf("info=%+v err=%v", info, err)
		}
	}
	if repo.calls != 1 {
		t.Fatalf("database lookups=%d", repo.calls)
	}
	key := "im:conversation:info:" + repo.record.ConversationId
	raw, err := server.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 {
		t.Fatalf("unexpected cached fields: %s", raw)
	}
	if server.TTL(key) != 10*time.Minute {
		t.Fatalf("TTL=%s", server.TTL(key))
	}
}

func TestConversationNotFoundExpiresAndCreationOverridesIt(t *testing.T) {
	app, repo, server, _ := newInfoTestApplication(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := app.GetConversationInfo(ctx, "conversation"); !errors.Is(err, ErrConversationNotFound) {
			t.Fatal(err)
		}
	}
	key := "im:conversation:info:conversation"
	if repo.calls != 1 || server.TTL(key) != 2*time.Minute {
		t.Fatalf("calls=%d TTL=%s", repo.calls, server.TTL(key))
	}
	server.FastForward(2 * time.Minute)
	if _, err := app.GetConversationInfo(ctx, "conversation"); !errors.Is(err, ErrConversationNotFound) || repo.calls != 2 {
		t.Fatalf("calls=%d err=%v", repo.calls, err)
	}
	repo.record = &conversationentity.Conversation{ConversationId: "conversation", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b"}
	app.CacheConversationInfo(ctx, repo.record)
	if err := app.cache.SetConversationNotFound(ctx, "conversation"); err != nil {
		t.Fatal(err)
	}
	info, err := app.GetConversationInfo(ctx, "conversation")
	if err != nil || info.UserID1 != "a" || repo.calls != 2 {
		t.Fatalf("info=%+v calls=%d err=%v", info, repo.calls, err)
	}
	if err := app.cache.DeleteConversationInfo(ctx, "conversation"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.GetConversationInfo(ctx, "conversation"); err != nil || repo.calls != 3 {
		t.Fatalf("calls=%d err=%v", repo.calls, err)
	}
}

func TestDatabaseFailureDoesNotBecomeNegativeCache(t *testing.T) {
	app, repo, server, _ := newInfoTestApplication(t)
	repo.err = errors.New("database unavailable")
	for i := 0; i < 2; i++ {
		if _, err := app.GetConversationInfo(context.Background(), "conversation"); !errors.Is(err, repo.err) {
			t.Fatal(err)
		}
	}
	if repo.calls != 2 || len(server.Keys()) != 0 {
		t.Fatalf("calls=%d keys=%v", repo.calls, server.Keys())
	}
}

func TestRedisFailureAndMalformedCacheFallBackToDatabase(t *testing.T) {
	for _, broken := range []bool{false, true} {
		app, repo, server, client := newInfoTestApplication(t)
		repo.record = &conversationentity.Conversation{ConversationId: "conversation", Convtype: conversationvo.PrivateChat, UserId1: "a", UserId2: "b"}
		if broken {
			_ = client.Close()
		} else {
			_ = server.Set("im:conversation:info:conversation", `{"conversationId":"other","convType":1}`)
		}
		info, err := app.GetConversationInfo(context.Background(), "conversation")
		if err != nil || info.ConversationID != "conversation" || repo.calls != 1 {
			t.Fatalf("info=%+v calls=%d err=%v", info, repo.calls, err)
		}
	}
}

func TestInvalidConversationIDDoesNotQueryOrCache(t *testing.T) {
	app, repo, server, _ := newInfoTestApplication(t)
	for _, id := range []string{"", strings.Repeat("a", 65)} {
		if _, err := app.GetConversationInfo(context.Background(), id); err == nil {
			t.Fatal("expected validation error")
		}
	}
	if repo.calls != 0 || len(server.Keys()) != 0 {
		t.Fatalf("calls=%d keys=%v", repo.calls, server.Keys())
	}
}
