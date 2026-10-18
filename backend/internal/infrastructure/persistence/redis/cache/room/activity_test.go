package room

import (
	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newActivityCacheTest(t *testing.T) (*RoomCache, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})

	cache := NewRoomCache(client, configs.MessageConfig{
		RoomActivityWindowSeconds:    60,
		RoomActivityBucketSeconds:    10,
		RoomActivityKeyTTLSeconds:    2,
		RoomActivityWarnFanoutWork:   2,
		RoomActivityActiveFanoutWork: 4,
	})
	return cache, client, server
}

func TestRoomCacheActivityLevelScalesWithOnlineSessions(t *testing.T) {
	cache, _, _ := newActivityCacheTest(t)
	ctx := context.Background()
	roomID := "room-activity-fanout"
	now := time.Unix(1_000_000, 0)

	level, err := cache.recordActivityAndGetLevelAt(ctx, roomID, 1, now, 1)
	if err != nil || level != roomcache.RoomActivityNormal {
		t.Fatalf("1 条消息 × 1 个在线会话应为 normal：level=%d err=%v", level, err)
	}
	level, err = cache.recordActivityAndGetLevelAt(ctx, roomID, 1, now, 1)
	if err != nil || level != roomcache.RoomActivityWarn {
		t.Fatalf("2 条消息 × 1 个在线会话应为 warn：level=%d err=%v", level, err)
	}
	level, err = cache.recordActivityAndGetLevelAt(ctx, roomID, 1, now, 2)
	if err != nil || level != roomcache.RoomActivityActive {
		t.Fatalf("3 条消息 × 2 个在线会话应为 active：level=%d err=%v", level, err)
	}
}

func TestRoomCacheActivityBatchCountsMessagesInOneBucket(t *testing.T) {
	cache, client, _ := newActivityCacheTest(t)
	ctx := context.Background()
	roomID := "room-activity-batch"
	now := time.Unix(1_000_000, 0)

	level, err := cache.recordActivityAndGetLevelAt(ctx, roomID, 2, now, 1)
	if err != nil || level != roomcache.RoomActivityWarn {
		t.Fatalf("同批 2 条消息 × 1 个在线会话应为 warn：level=%d err=%v", level, err)
	}
	count, err := client.HGet(ctx, ActivateLevelKey(roomID), "100000").Int64()
	if err != nil || count != 2 {
		t.Fatalf("同一时间桶应一次增加 2 条：count=%d err=%v", count, err)
	}
}

func TestRoomCacheReadingActivityDoesNotIncreaseMessageCount(t *testing.T) {
	cache, client, _ := newActivityCacheTest(t)
	ctx := context.Background()
	now := time.Now()
	if err := cache.recordActivityAt(ctx, "read-only", now); err != nil {
		t.Fatal(err)
	}
	before, err := client.HGetAll(ctx, ActivateLevelKey("read-only")).Result()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		level, err := cache.activateLevelAt(ctx, "read-only", now, 4)
		if err != nil || level != roomcache.RoomActivityActive {
			t.Fatalf("level=%d err=%v", level, err)
		}
	}
	after, err := client.HGetAll(ctx, ActivateLevelKey("read-only")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatal("reading changed activity buckets")
	}
	for key, value := range before {
		if after[key] != value {
			t.Fatal("reading increased activity count")
		}
	}
	level, err := cache.activateLevelAt(ctx, "read-only", now, 0)
	if err != nil || level != roomcache.RoomActivityNormal {
		t.Fatalf("offline room level=%d err=%v", level, err)
	}
}

func TestRoomCacheActivityLevelUsesRecentWindow(t *testing.T) {
	cache, client, _ := newActivityCacheTest(t)
	ctx := context.Background()
	roomID := "room-activity-1"
	now := time.Unix(1_000_000, 0)

	if err := cache.recordActivityAt(ctx, roomID, now); err != nil {
		t.Fatal(err)
	}
	level, err := cache.activateLevelAt(ctx, roomID, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if level != roomcache.RoomActivityNormal {
		t.Fatalf("1 条消息应为 normal，实际=%d", level)
	}

	if err := cache.recordActivityAt(ctx, roomID, now); err != nil {
		t.Fatal(err)
	}
	level, err = cache.activateLevelAt(ctx, roomID, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if level != roomcache.RoomActivityWarn {
		t.Fatalf("2 条消息应为 warn，实际=%d", level)
	}

	for range 2 {
		if err := cache.recordActivityAt(ctx, roomID, now); err != nil {
			t.Fatal(err)
		}
	}
	level, err = cache.activateLevelAt(ctx, roomID, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if level != roomcache.RoomActivityActive {
		t.Fatalf("4 条消息应为 active，实际=%d", level)
	}

	if fields, err := client.HLen(ctx, ActivateLevelKey(roomID)).Result(); err != nil || fields != 1 {
		t.Fatalf("同一时间桶应只产生一个 Hash field：fields=%d err=%v", fields, err)
	}
}

func TestRoomCacheActivityWindowDropsExpiredBuckets(t *testing.T) {
	cache, _, _ := newActivityCacheTest(t)
	ctx := context.Background()
	roomID := "room-activity-2"
	t0 := time.Unix(1_000_000, 0)

	if err := cache.recordActivityAt(ctx, roomID, t0); err != nil {
		t.Fatal(err)
	}
	// 经过完整窗口后，旧桶不再参与统计；当前桶没有新消息，因此回到 normal。
	t1 := t0.Add(70 * time.Second)
	if err := cache.recordActivityAt(ctx, roomID, t1); err != nil {
		t.Fatal(err)
	}
	level, err := cache.activateLevelAt(ctx, roomID, t1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if level != roomcache.RoomActivityNormal {
		t.Fatalf("旧桶过期后应为 normal，实际=%d", level)
	}
}

func TestRoomCacheActivityKeyExpiresAfterIdle(t *testing.T) {
	cache, client, server := newActivityCacheTest(t)
	ctx := context.Background()
	roomID := "room-activity-3"

	if err := cache.RecordActivity(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	server.FastForward(3 * time.Second)
	if exists, err := client.Exists(ctx, ActivateLevelKey(roomID)).Result(); err != nil || exists != 0 {
		t.Fatalf("活跃度 key 应在空闲后过期：exists=%d err=%v", exists, err)
	}
}
