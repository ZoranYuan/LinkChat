package message

import (
	messagecache "IM_backend/internal/application/ports/persistence/cache/message"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMessageDedupIsScopedBySender(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cache := NewMessageCache(redis.NewClient(&redis.Options{Addr: redisServer.Addr()}))
	ctx := context.Background()

	entry := messagecache.DedupEntry{
		MessageID:      "message-1",
		RequestHash:    "hash-1",
		ConversationID: "conversation-1",
		Seq:            12,
		AttachmentID:   "attachment-1",
		SendTime:       1710000000000,
		SenderUsername: "alice",
	}
	if ok, err := cache.SetDedupEntry(ctx, "sender-1", "client-1", entry, time.Minute); err != nil || !ok {
		t.Fatalf("写入发送者 1 的去重记录失败: ok=%v err=%v", ok, err)
	}
	got, err := cache.GetDedupEntry(ctx, "sender-1", "client-1")
	if err != nil || got != entry {
		t.Fatalf("读取发送者 1 的去重记录错误: got=%q err=%v", got, err)
	}

	got, err = cache.GetDedupEntry(ctx, "sender-2", "client-1")
	if err != nil {
		t.Fatalf("读取发送者 2 的去重记录失败: %v", err)
	}
	if got.MessageID != "" {
		t.Fatalf("不同发送者不应命中同一 clientMsgId: got=%q", got)
	}
}

type dedupPipelineHook struct {
	singleGets int
	pipelines  int
	batchGets  int
}

func (*dedupPipelineHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *dedupPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if command.Name() == "get" {
			h.singleGets++
		}
		return next(ctx, command)
	}
}

func (h *dedupPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		gets := 0
		for _, command := range commands {
			if command.Name() == "get" {
				gets++
			}
		}
		if gets > 0 {
			h.pipelines++
			h.batchGets += gets
		}
		return next(ctx, commands)
	}
}

func TestGetDedupEntriesUsesOnePipelineAndScopesBySender(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &dedupPipelineHook{}
	client.AddHook(hook)
	cache := NewMessageCache(client)
	ctx := context.Background()
	first := messagecache.DedupKey{SenderID: "sender-1", ClientMsgID: "same-client-id"}
	second := messagecache.DedupKey{SenderID: "sender-2", ClientMsgID: "same-client-id"}
	missing := messagecache.DedupKey{SenderID: "sender-1", ClientMsgID: "missing"}
	corrupted := messagecache.DedupKey{SenderID: "sender-1", ClientMsgID: "corrupted"}
	firstEntry := messagecache.DedupEntry{MessageID: "message-1", ConversationID: "conversation", Seq: 1, RequestHash: "hash-1"}
	secondEntry := messagecache.DedupEntry{MessageID: "message-2", ConversationID: "conversation", Seq: 2, RequestHash: "hash-2"}
	for key, entry := range map[messagecache.DedupKey]messagecache.DedupEntry{first: firstEntry, second: secondEntry} {
		if _, err := cache.SetDedupEntry(ctx, key.SenderID, key.ClientMsgID, entry, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Set(ctx, MessageDedupKey(corrupted.SenderID, corrupted.ClientMsgID), "invalid JSON", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	entries, err := cache.GetDedupEntries(ctx, []messagecache.DedupKey{
		first, missing, second, first, corrupted,
		{SenderID: "sender-1"}, {ClientMsgID: "empty-sender"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[first] != firstEntry || entries[second] != secondEntry {
		t.Fatalf("unexpected batch entries: %+v", entries)
	}
	if hook.pipelines != 1 || hook.batchGets != 4 || hook.singleGets != 0 {
		t.Fatalf("pipelines=%d batch GETs=%d single GETs=%d want 1,4,0", hook.pipelines, hook.batchGets, hook.singleGets)
	}
}

func TestGetDedupEntriesEmptyKeysDoNotAccessRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	_ = client.Close()
	cache := NewMessageCache(client)
	for _, keys := range [][]messagecache.DedupKey{
		nil,
		{{SenderID: "sender"}, {ClientMsgID: "client"}},
	} {
		entries, err := cache.GetDedupEntries(context.Background(), keys)
		if err != nil || len(entries) != 0 {
			t.Fatalf("empty keys must not execute Redis commands: entries=%+v err=%v", entries, err)
		}
	}
}

func TestGetDedupEntriesReturnsRedisFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	_ = client.Close()
	entries, err := NewMessageCache(client).GetDedupEntries(context.Background(), []messagecache.DedupKey{
		{SenderID: "sender", ClientMsgID: "client"},
	})
	if err == nil || len(entries) != 0 {
		t.Fatalf("Redis failure must be returned: entries=%+v err=%v", entries, err)
	}
}
