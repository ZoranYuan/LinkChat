package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/cache/room"
	domainroom "IM_backend/internal/domain/room"
)

type RoomAgentApplication struct {
	roomMemberRepository domainroom.RoomMemberRepository
	roomMemberCache      roomcache.RoomMemberCache
	config               configs.MessageConfig
	sf                   singleflight.Group
}

func NewRoomAgentApplication(
	roomMemberRepository domainroom.RoomMemberRepository,
	roomMemberCache roomcache.RoomMemberCache,
	config configs.MessageConfig,
) *RoomAgentApplication {
	return &RoomAgentApplication{
		roomMemberRepository: roomMemberRepository,
		roomMemberCache:      roomMemberCache,
		config:               config,
	}
}

func (ra *RoomAgentApplication) VerifyRoomAccess(ctx context.Context, roomID, userID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	roomID = strings.TrimSpace(roomID)
	userID = strings.TrimSpace(userID)
	if roomID == "" || userID == "" {
		return "", ErrRoomAgentAccessDenied
	}

	state, hit, err := ra.roomMemberCache.GetMember(ctx, roomID, userID)
	if err != nil || !hit {
		resultCh := ra.sf.DoChan("room-member:"+roomID+":"+userID, func() (any, error) {
			queryCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				time.Duration(ra.config.MemberQueryTimeoutSeconds)*time.Second,
			)
			defer cancel()

			// 合并回源前再次读取缓存，复用其他请求已回填的成员状态。
			if cached, found, err := ra.roomMemberCache.GetMember(queryCtx, roomID, userID); err == nil && found {
				return cached, nil
			}
			members, err := ra.roomMemberRepository.ListRoomMembersByRoomIDAndUserIDs(queryCtx, roomID, []string{userID})
			if err != nil {
				return nil, err
			}
			if len(members) == 0 || members[0] == nil {
				if err := ra.roomMemberCache.SetMemberNotFound(queryCtx, roomID, userID); err != nil {
					log.Printf("回填 Room Agent 成员空缓存失败：room=%s user=%s err=%v", roomID, userID, err)
				}
				return nil, ErrRoomAgentAccessDenied
			}
			member := members[0]
			memberState := &roomcache.MemberState{
				Status:    member.Status,
				Role:      member.Role,
				MuteUntil: member.MuteUntil,
				Version:   member.Version,
			}
			if _, err := ra.roomMemberCache.SetMemberIfVersionGreater(queryCtx, roomID, userID, memberState); err != nil {
				log.Printf("回填 Room Agent 成员缓存失败：room=%s user=%s err=%v", roomID, userID, err)
			}
			return memberState, nil
		})
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case result := <-resultCh:
			if result.Err != nil {
				return "", result.Err
			}
			state = result.Val.(*roomcache.MemberState)
		}
	}
	if state == nil || (state.Status != domainroom.Active && state.Status != domainroom.Muted) {
		return "", ErrRoomAgentAccessDenied
	}

	threadID := sha256.Sum256([]byte(userID + ":" + roomID))
	return hex.EncodeToString(threadID[:]), nil
}

func (ra *RoomAgentApplication) GetRoomUnreadMessages(ctx context.Context, roomID, userID string, pageSize uint32, cursor int64) (
	[]ChatMessageDTO, int64, error,
) {
	return nil, 0, errors.New("Room Agent 未读消息查询尚未实现")
}
