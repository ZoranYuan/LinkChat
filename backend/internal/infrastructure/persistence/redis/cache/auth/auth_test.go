package auth

import (
	authport "IM_backend/internal/application/ports/persistence/cache/auth"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newAuthCacheTest(t *testing.T) (*authCache, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})
	return NewAuthCache(client), client, server
}

func TestAuthCacheCreatesAndRotatesSessionWithStableID(t *testing.T) {
	cache, client, _ := newAuthCacheTest(t)
	ctx := context.Background()
	expire := time.Hour
	initial := authport.RefreshSession{UserID: "user-1", SessionID: "session-1"}

	if err := cache.CreateSession(ctx, "refresh-old", initial, expire); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if active, err := cache.IsSessionActive(ctx, initial.UserID, initial.SessionID); err != nil || !active {
		t.Fatalf("new session active=%v err=%v", active, err)
	}
	got, found, err := cache.GetRefreshSession(ctx, "refresh-old")
	if err != nil || !found || got != initial {
		t.Fatalf("GetRefreshSession() = %#v found=%v err=%v", got, found, err)
	}

	rotated, err := cache.RotateRefreshSession(ctx, "refresh-old", "refresh-new", initial, expire)
	if err != nil || !rotated {
		t.Fatalf("RotateRefreshSession() = %v, err=%v", rotated, err)
	}
	if _, found, err := cache.GetRefreshSession(ctx, "refresh-old"); err != nil || found {
		t.Fatalf("old refresh token should be invalid, found=%v err=%v", found, err)
	}
	got, found, err = cache.GetRefreshSession(ctx, "refresh-new")
	if err != nil || !found || got != initial {
		t.Fatalf("new refresh session = %#v found=%v err=%v", got, found, err)
	}

	refreshID, err := client.Get(ctx, RefreshTokenKey("refresh-new")).Result()
	if err != nil || refreshID != initial.SessionID {
		t.Fatalf("refresh token mapping = %q, err=%v", refreshID, err)
	}
	if err := cache.DeleteSession(ctx, initial.UserID, initial.SessionID); err != nil {
		t.Fatalf("DeleteSession() error = %v", err)
	}
	if active, err := cache.IsSessionActive(ctx, initial.UserID, initial.SessionID); err != nil || active {
		t.Fatalf("deleted session active=%v err=%v", active, err)
	}
	if _, err := client.Get(ctx, RefreshTokenKey("refresh-new")).Result(); err != redis.Nil {
		t.Fatalf("refresh mapping should be deleted, err=%v", err)
	}
}

func TestAuthCacheDeleteSessionOnlyAffectsItsDevice(t *testing.T) {
	cache, _, _ := newAuthCacheTest(t)
	ctx := context.Background()
	deviceA := authport.RefreshSession{UserID: "user-1", SessionID: "session-a"}
	deviceB := authport.RefreshSession{UserID: "user-1", SessionID: "session-b"}
	if err := cache.CreateSession(ctx, "refresh-a", deviceA, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := cache.CreateSession(ctx, "refresh-b", deviceB, time.Hour); err != nil {
		t.Fatal(err)
	}

	if err := cache.DeleteSession(ctx, deviceA.UserID, deviceA.SessionID); err != nil {
		t.Fatal(err)
	}
	if active, err := cache.IsSessionActive(ctx, deviceA.UserID, deviceA.SessionID); err != nil || active {
		t.Fatalf("device A session active=%v err=%v", active, err)
	}
	if active, err := cache.IsSessionActive(ctx, deviceB.UserID, deviceB.SessionID); err != nil || !active {
		t.Fatalf("device B session active=%v err=%v", active, err)
	}
	if _, found, err := cache.GetRefreshSession(ctx, "refresh-b"); err != nil || !found {
		t.Fatalf("device B refresh token found=%v err=%v", found, err)
	}
}

func TestAuthCacheSessionExpiresWithRefreshToken(t *testing.T) {
	cache, _, server := newAuthCacheTest(t)
	session := authport.RefreshSession{UserID: "user-1", SessionID: "session-1"}
	if err := cache.CreateSession(context.Background(), "refresh-1", session, time.Minute); err != nil {
		t.Fatal(err)
	}

	server.FastForward(time.Minute)
	if active, err := cache.IsSessionActive(context.Background(), session.UserID, session.SessionID); err != nil || active {
		t.Fatalf("expired session active=%v err=%v", active, err)
	}
	if _, found, err := cache.GetRefreshSession(context.Background(), "refresh-1"); err != nil || found {
		t.Fatalf("expired refresh token found=%v err=%v", found, err)
	}
}

func TestAuthCacheRequiresBothSessionMappingsToBeActive(t *testing.T) {
	cache, client, _ := newAuthCacheTest(t)
	ctx := context.Background()
	session := authport.RefreshSession{UserID: "user-1", SessionID: "session-1"}
	if err := cache.CreateSession(ctx, "refresh-1", session, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, RefreshTokenKey("refresh-1")).Err(); err != nil {
		t.Fatal(err)
	}
	if active, err := cache.IsSessionActive(ctx, session.UserID, session.SessionID); err != nil || active {
		t.Fatalf("session with missing refresh mapping active=%v err=%v", active, err)
	}
}
