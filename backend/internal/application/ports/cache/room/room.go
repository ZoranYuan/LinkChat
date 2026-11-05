package room

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"time"

	domainroom "IM_backend/internal/domain/room"
)

type MemberState struct {
	Status    domainroom.RoomMemberStatus
	Role      domainroom.Role
	MuteUntil *int64
	Version   int64
}

type RecentMessageRange struct {
	Events  []messaging.MessagePushEvent
	Covered bool // 是否连续覆盖 afterSeq 到本次缓存末尾。
}

const (
	RoomActivityNormal = iota
	RoomActivityWarn
	RoomActivityActive
)

type RoomCache interface {
	UpdateRoomActivity(ctx context.Context, roomID string, messageCount, onlineSessions int, at time.Time) (int, error)
	ListMessageAfterSeq(ctx context.Context, roomID string, afterSeq int64) (RecentMessageRange, error)
	WarmRecentMessageEvents(ctx context.Context, roomID string, events []messaging.MessagePushEvent) error
	GetRoomIDByCode(ctx context.Context, code string) (string, error)
	UpdateInviteCode(ctx context.Context, roomID string, ttl time.Duration) (string, error)
	DeleteInviteCode(ctx context.Context, roomID string) error
}

type RoomMemberCache interface {
	GetMember(ctx context.Context, roomID, userID string) (*MemberState, bool, error)
	SetMemberIfVersionGreater(ctx context.Context, roomID, userID string, state *MemberState) (bool, error)
	SetMemberNotFound(ctx context.Context, roomID, userID string) error
	DeleteMember(ctx context.Context, roomID, userID string) error
}

// RoomInfoCache 缓存房间相对稳定的基础信息；成员总数变化后由应用层主动失效。
type RoomInfoCache interface {
	GetRoomInfo(ctx context.Context, roomID string) (*domainroom.Room, bool, error)
	GetRoomInfos(ctx context.Context, roomIDs []string) (map[string]*domainroom.Room, error)
	SetRoomInfo(ctx context.Context, room *domainroom.Room, ttl time.Duration) error
	SetRoomInfos(ctx context.Context, rooms []*domainroom.Room, ttl time.Duration) error
	DeleteRoomInfo(ctx context.Context, roomID string) error
}
