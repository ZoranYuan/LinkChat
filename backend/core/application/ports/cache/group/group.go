package group

import (
	eventport "IM_backend/core/application/ports/events"
	"context"
	"time"

	domaingroup "IM_backend/core/domain/group"
)

type MemberState struct {
	Status    domaingroup.GroupMemberStatus
	Role      domaingroup.Role
	MuteUntil *int64
	Version   int64
}

type RecentMessageRange struct {
	Events  []eventport.MessagePushEvent
	Covered bool // 是否连续覆盖 afterSeq 到本次缓存末尾。
}

const (
	GroupActivityNormal = iota
	GroupActivityWarn
	GroupActivityActive
)

type GroupCache interface {
	UpdateGroupActivity(ctx context.Context, groupID string, messageCount, onlineSessions int, at time.Time) (int, error)
	ListMessageAfterSeq(ctx context.Context, conversationID string, afterSeq int64) (RecentMessageRange, error)
	WarmRecentMessageEvents(ctx context.Context, conversationID string, events []eventport.MessagePushEvent) error
	GetGroupIDByCode(ctx context.Context, code string) (string, error)
	UpdateInviteCode(ctx context.Context, groupID string, ttl time.Duration) (string, error)
	DeleteInviteCode(ctx context.Context, groupID string) error
}

type GroupMemberCache interface {
	GetMember(ctx context.Context, groupID, userID string) (*MemberState, bool, error)
	SetMemberIfVersionGreater(ctx context.Context, groupID, userID string, state *MemberState) (bool, error)
	SetMemberNotFound(ctx context.Context, groupID, userID string) error
	DeleteMember(ctx context.Context, groupID, userID string) error
}

// GroupInfoCache 缓存群聊相对稳定的基础信息；成员总数变化后由应用层主动失效。
type GroupInfoCache interface {
	GetGroupInfo(ctx context.Context, groupID string) (*domaingroup.Group, bool, error)
	GetGroupInfos(ctx context.Context, groupIDs []string) (map[string]*domaingroup.Group, error)
	SetGroupInfo(ctx context.Context, group *domaingroup.Group, ttl time.Duration) error
	SetGroupInfos(ctx context.Context, groups []*domaingroup.Group, ttl time.Duration) error
	DeleteGroupInfo(ctx context.Context, groupID string) error
}
