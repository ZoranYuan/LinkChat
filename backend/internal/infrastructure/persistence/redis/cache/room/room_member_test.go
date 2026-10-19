package room

import (
	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomvo "IM_backend/internal/domain/room/value_object"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRoomMemberCacheTest(t *testing.T) (*RoomMemberCache, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})
	return NewRoomMemberCache(client, configs.MessageConfig{
		RoomMemberStateTTLSeconds:    3600,
		RoomMemberNegativeTTLSeconds: 120,
	}), client, server
}

func memberState(version int64, status roomvo.RoomUserStatus) *roomcache.MemberState {
	return &roomcache.MemberState{Status: status, Role: roomvo.RegularUser, Version: version}
}

func TestRoomMemberCacheUsesOneHashPerRoom(t *testing.T) {
	cache, client, _ := newRoomMemberCacheTest(t)
	ctx := context.Background()

	for _, userID := range []string{"user-1", "user-2"} {
		updated, err := cache.SetMemberIfVersionGreater(ctx, "room-1", userID, memberState(1, roomvo.Activate))
		if err != nil || !updated {
			t.Fatalf("写入成员状态失败：userID=%s updated=%v err=%v", userID, updated, err)
		}
	}
	updated, err := cache.SetMemberIfVersionGreater(ctx, "room-2", "user-1", memberState(1, roomvo.BeMuted))
	if err != nil || !updated {
		t.Fatalf("写入另一房间成员状态失败：updated=%v err=%v", updated, err)
	}

	roomOneKey := RoomMembersKey("room-1")
	if got, err := client.Type(ctx, roomOneKey).Result(); err != nil || got != "hash" {
		t.Fatalf("房间成员缓存应为 Hash：type=%q err=%v", got, err)
	}
	if got, err := client.HLen(ctx, roomOneKey).Result(); err != nil || got != 2 {
		t.Fatalf("同一房间的用户应作为 Hash field：length=%d err=%v", got, err)
	}
	if _, err := client.HGet(ctx, roomOneKey, "user-1").Result(); err != nil {
		t.Fatalf("Hash field 应为 userID：err=%v", err)
	}
	if _, err := client.HGet(ctx, RoomMembersKey("room-2"), "user-1").Result(); err != nil {
		t.Fatalf("不同房间应使用独立 Hash：err=%v", err)
	}
}

func TestRoomMemberCacheVersionIsIdempotent(t *testing.T) {
	cache, _, _ := newRoomMemberCacheTest(t)
	ctx := context.Background()

	updated, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(2, roomvo.Left))
	if err != nil || !updated {
		t.Fatalf("写入新版本失败：updated=%v err=%v", updated, err)
	}
	updated, err = cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(1, roomvo.Activate))
	if err != nil || updated {
		t.Fatalf("旧版本不应覆盖新版本：updated=%v err=%v", updated, err)
	}

	state, hit, err := cache.GetMember(ctx, "room-1", "user-1")
	if err != nil || !hit || state == nil {
		t.Fatalf("读取成员缓存失败：state=%+v hit=%v err=%v", state, hit, err)
	}
	if state.Version != 2 || state.Status != roomvo.Left {
		t.Fatalf("缓存被旧版本覆盖：state=%+v", state)
	}
}

func TestRoomMemberCacheNewerVersionUpdatesMemberState(t *testing.T) {
	cache, client, _ := newRoomMemberCacheTest(t)
	ctx := context.Background()
	key := RoomMembersKey("room-1")

	if _, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(1, roomvo.Activate)); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-2", memberState(5, roomvo.Activate)); err != nil {
		t.Fatal(err)
	}
	updated, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(2, roomvo.Left))
	if err != nil || !updated {
		t.Fatalf("新版本应更新成员状态：updated=%v err=%v", updated, err)
	}
	state, hit, err := cache.GetMember(ctx, "room-1", "user-1")
	if err != nil || !hit || state == nil || state.Version != 2 || state.Status != roomvo.Left {
		t.Fatalf("成员状态应更新为新版本：state=%+v hit=%v err=%v", state, hit, err)
	}
	otherData, err := client.HGet(ctx, key, "user-2").Result()
	if err != nil {
		t.Fatalf("更新成员不应影响同房间其他 field：err=%v", err)
	}
	var other roomMemberEntry
	if err := json.Unmarshal([]byte(otherData), &other); err != nil || other.Version != 5 || other.Status != int(roomvo.Activate) {
		t.Fatalf("其他成员状态不应变化：entry=%+v err=%v", other, err)
	}
}

func TestRoomMemberCacheNegativeEntryDoesNotOverwriteMember(t *testing.T) {
	cache, _, _ := newRoomMemberCacheTest(t)
	ctx := context.Background()

	if _, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(7, roomvo.Activate)); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetMemberNotFound(ctx, "room-1", "user-1"); err != nil {
		t.Fatal(err)
	}
	state, hit, err := cache.GetMember(ctx, "room-1", "user-1")
	if err != nil || !hit || state == nil || state.Version != 7 {
		t.Fatalf("负缓存不应覆盖已存在的成员状态：state=%+v hit=%v err=%v", state, hit, err)
	}
}

func TestRoomMemberCacheNegativeEntryAndMemberReplacement(t *testing.T) {
	cache, _, _ := newRoomMemberCacheTest(t)
	ctx := context.Background()

	if err := cache.SetMemberNotFound(ctx, "room-1", "user-1"); err != nil {
		t.Fatal(err)
	}
	state, hit, err := cache.GetMember(ctx, "room-1", "user-1")
	if err != nil || !hit || state != nil {
		t.Fatalf("应读取到成员负缓存：state=%+v hit=%v err=%v", state, hit, err)
	}
	updated, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(1, roomvo.Activate))
	if err != nil || !updated {
		t.Fatalf("成员状态应覆盖负缓存：updated=%v err=%v", updated, err)
	}
	state, hit, err = cache.GetMember(ctx, "room-1", "user-1")
	if err != nil || !hit || state == nil || state.Status != roomvo.Activate {
		t.Fatalf("负缓存应被有效成员状态替换：state=%+v hit=%v err=%v", state, hit, err)
	}
}

func TestRoomMemberCacheNativeFieldExpiration(t *testing.T) {
	cache, client, server := newRoomMemberCacheTest(t)
	ctx := context.Background()
	cache = NewRoomMemberCache(client, configs.MessageConfig{
		RoomMemberStateTTLSeconds:    1,
		RoomMemberNegativeTTLSeconds: 1,
	})
	if _, err := cache.SetMemberIfVersionGreater(ctx, "room-1", "user-1", memberState(3, roomvo.Activate)); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetMemberNotFound(ctx, "room-1", "user-2"); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)

	for _, userID := range []string{"user-1", "user-2"} {
		state, hit, err := cache.GetMember(ctx, "room-1", userID)
		if err != nil || hit || state != nil {
			t.Fatalf("过期 field 应视为缓存未命中：userID=%s state=%+v hit=%v err=%v", userID, state, hit, err)
		}
	}
	if _, err := client.HGet(ctx, RoomMembersKey("room-1"), "user-1").Result(); err != redis.Nil {
		t.Fatalf("过期成员 field 应从 Hash 清理：err=%v", err)
	}
	if _, err := client.HGet(ctx, RoomMembersKey("room-1"), "user-2").Result(); err != redis.Nil {
		t.Fatalf("过期负缓存 field 应从 Hash 清理：err=%v", err)
	}
}
