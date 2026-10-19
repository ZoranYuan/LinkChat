package summary

import (
	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRoomUnreadSnapshotCacheSetAndGetActive(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache := NewRoomUnreadSnapshotCache(client)

	want := summarycache.RoomUnreadSnapshot{
		RoomID:      "room-1",
		UnreadBegin: 101,
		UnreadEnd:   120,
	}
	if err := cache.SetActive(context.Background(), "user-1", "room-1", want, time.Minute); err != nil {
		t.Fatalf("SetActive() error = %v", err)
	}

	got, found, err := cache.GetActive(context.Background(), "user-1", "room-1")
	if err != nil {
		t.Fatalf("GetActive() error = %v", err)
	}
	if !found || *got != want {
		t.Fatalf("GetActive() = %#v, found=%v, want %#v", got, found, want)
	}
}

func TestRoomUnreadSnapshotCacheAllowsEmptyRange(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache := NewRoomUnreadSnapshotCache(client)
	want := summarycache.RoomUnreadSnapshot{RoomID: "room-1"}

	if err := cache.SetActive(context.Background(), "user-1", "room-1", want, time.Minute); err != nil {
		t.Fatalf("SetActive() empty range error = %v", err)
	}
	got, found, err := cache.GetActive(context.Background(), "user-1", "room-1")
	if err != nil || !found || *got != want {
		t.Fatalf("GetActive() empty range = %#v, found=%v, err=%v", got, found, err)
	}
}

func TestRoomUnreadSnapshotCacheKeepsRoomsIsolated(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache := NewRoomUnreadSnapshotCache(client)

	first := summarycache.RoomUnreadSnapshot{RoomID: "room-a", UnreadBegin: 1, UnreadEnd: 2}
	second := summarycache.RoomUnreadSnapshot{RoomID: "room-b", UnreadBegin: 3, UnreadEnd: 4}
	ctx := context.Background()
	if err := cache.SetActive(ctx, "user-1", "room-a", first, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetActive(ctx, "user-1", "room-b", second, time.Minute); err != nil {
		t.Fatal(err)
	}

	gotFirst, foundFirst, err := cache.GetActive(ctx, "user-1", "room-a")
	if err != nil || !foundFirst || *gotFirst != first {
		t.Fatalf("room-a snapshot = %#v, found=%v, err=%v", gotFirst, foundFirst, err)
	}
	gotSecond, foundSecond, err := cache.GetActive(ctx, "user-1", "room-b")
	if err != nil || !foundSecond || *gotSecond != second {
		t.Fatalf("room-b snapshot = %#v, found=%v, err=%v", gotSecond, foundSecond, err)
	}
}

func TestRoomUnreadSnapshotCacheDeleteActive(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache := NewRoomUnreadSnapshotCache(client)
	ctx := context.Background()

	if err := cache.SetActive(ctx, "user-1", "room-1", summarycache.RoomUnreadSnapshot{
		RoomID: "room-1", UnreadBegin: 1, UnreadEnd: 2,
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := cache.DeleteActive(ctx, "user-1", "room-1"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.GetActive(ctx, "user-1", "room-1"); err != nil || found {
		t.Fatalf("snapshot should be deleted, found=%v, err=%v", found, err)
	}
}

func TestSummaryScopeRunStoreKeepsOneRunID(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache := NewRoomUnreadSnapshotCache(client)
	ctx := context.Background()

	existing, lockToken, acquired, err := cache.ClaimSummaryRunIDByScope(ctx, "user-1", "room-1", "run-1", time.Minute)
	if err != nil || !acquired || existing != "run-1" || lockToken == "" {
		t.Fatalf("first claim = run=%s token=%s acquired=%v err=%v", existing, lockToken, acquired, err)
	}

	existing, otherToken, acquired, err := cache.ClaimSummaryRunIDByScope(ctx, "user-1", "room-1", "run-2", time.Minute)
	if err != nil || acquired || existing != "run-1" {
		t.Fatalf("duplicate claim = run=%s token=%s acquired=%v err=%v", existing, otherToken, acquired, err)
	}

	foundRun, found, err := cache.GetSummaryRunIDByScope(ctx, "user-1", "room-1")
	if err != nil || !found || foundRun != "run-1" {
		t.Fatalf("scope run = run=%s found=%v err=%v", foundRun, found, err)
	}

	if err := cache.ReleaseSummaryRunIDByScope(ctx, "user-1", "room-1", "run-2", "wrong-token"); err != nil {
		t.Fatal(err)
	}
	foundRun, found, err = cache.GetSummaryRunIDByScope(ctx, "user-1", "room-1")
	if err != nil || !found || foundRun != "run-1" {
		t.Fatalf("wrong owner should not release scope run = run=%s found=%v err=%v", foundRun, found, err)
	}

	if ok, err := cache.RefreshSummaryRunIDByScope(ctx, "user-1", "room-1", "run-1", lockToken, time.Minute); err != nil || !ok {
		t.Fatalf("refresh scope run = ok=%v err=%v", ok, err)
	}
	if err := cache.ReleaseSummaryRunIDByScope(ctx, "user-1", "room-1", "run-1", lockToken); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.GetSummaryRunIDByScope(ctx, "user-1", "room-1"); err != nil || found {
		t.Fatalf("scope run should be released, found=%v err=%v", found, err)
	}
}
