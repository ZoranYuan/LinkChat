package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	messageentity "IM_backend/internal/domain/message/entity"
	messagevo "IM_backend/internal/domain/message/value_object"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"

	"golang.org/x/sync/singleflight"
)

type RoomAgentApplication struct {
	roomUserRepository      roomrepo.RoomUserRepository
	roomMemberCache         roomcache.RoomMemberCache
	conversationRepository  conversationrepo.ConversationRepository
	userConversationRepo    conversationrepo.UserConversationRepository
	messageRepository       messagerepo.MessageRepository
	roomUnreadSnapshotCache summarycache.RoomUnreadSnapshotCache
	roomUnreadSnapshotTTL   time.Duration
	sf                      *singleflight.Group
}

func NewRoomAgentApplication(
	roomUserRepository roomrepo.RoomUserRepository,
	roomMemberCache roomcache.RoomMemberCache,
	conversationRepository conversationrepo.ConversationRepository,
	userConversationRepo conversationrepo.UserConversationRepository,
	messageRepository messagerepo.MessageRepository,
	roomUnreadSnapshotCache summarycache.RoomUnreadSnapshotCache,
	roomUnreadSnapshotTTL time.Duration,
) *RoomAgentApplication {
	if roomUnreadSnapshotTTL <= 0 {
		roomUnreadSnapshotTTL = 2 * time.Minute
	}
	return &RoomAgentApplication{
		roomUserRepository:      roomUserRepository,
		roomMemberCache:         roomMemberCache,
		conversationRepository:  conversationRepository,
		userConversationRepo:    userConversationRepo,
		messageRepository:       messageRepository,
		roomUnreadSnapshotCache: roomUnreadSnapshotCache,
		roomUnreadSnapshotTTL:   roomUnreadSnapshotTTL,
		sf:                      &singleflight.Group{},
	}
}

func (ra *RoomAgentApplication) GetRoomAgentContext(
	ctx context.Context,
	roomID, userID string,
) (*RoomAgentContext, error) {
	roomID = strings.TrimSpace(roomID)
	userID = strings.TrimSpace(userID)
	if roomID == "" || userID == "" {
		return nil, ErrRoomAgentAccessDenied
	}
	if err := ra.validateRoomMembership(ctx, roomID, userID); err != nil {
		return nil, err
	}
	if ra.conversationRepository == nil || ra.userConversationRepo == nil || ra.messageRepository == nil {
		return nil, errors.New("Room Agent 未读消息查询仓储未配置")
	}

	conversation, err := ra.conversationRepository.FindRoomConversation(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if conversation == nil || conversation.Convtype != conversationvo.RoomChat {
		return nil, conversationentity.ErrConversationNotCreated
	}

	snapshot, err := ra.getUnreadSnapshot(ctx, userID, roomID, conversation.ConversationId)
	if err != nil {
		return nil, err
	}

	result := &RoomAgentContext{
		UserID:         userID,
		ThreadID:       roomAgentThreadID(userID, roomID),
		UnreadBeginSeq: snapshot.UnreadBegin,
		UnreadEndSeq:   snapshot.UnreadEnd,
		Messages:       make([]RoomAgentContextMessage, 0),
	}
	if snapshot.UnreadBegin == 0 && snapshot.UnreadEnd == 0 {
		return result, nil
	}

	messages, err := ra.messageRepository.ListBySeqRange(
		ctx,
		conversation.ConversationId,
		snapshot.UnreadBegin,
		snapshot.UnreadEnd,
	)
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		if message == nil || message.Status != messagevo.Normal || message.Type != messagevo.Text {
			continue
		}
		result.Messages = append(result.Messages, toRoomAgentContextMessage(message))
	}
	result.UnreadMessageCount = uint32(len(result.Messages))
	return result, nil
}

func (ra *RoomAgentApplication) getUnreadSnapshot(
	ctx context.Context,
	userID, roomID, conversationID string,
) (*summarycache.RoomUnreadSnapshot, error) {
	if ra.roomUnreadSnapshotCache != nil {
		if snapshot, found, err := ra.roomUnreadSnapshotCache.GetActive(ctx, userID, roomID); err == nil && found {
			if validRoomAgentSnapshot(snapshot, roomID) {
				return snapshot, nil
			}
		}
	}
	if ra.sf == nil {
		return nil, errors.New("Room Agent singleflight 未初始化")
	}

	resultCh := ra.sf.DoChan("room-unread-snapshot:"+userID+":"+roomID, func() (any, error) {
		// 再查一次缓存，避免并发请求在 singleflight 等待期间重复回源。
		if ra.roomUnreadSnapshotCache != nil {
			if snapshot, found, err := ra.roomUnreadSnapshotCache.GetActive(ctx, userID, roomID); err == nil && found {
				if validRoomAgentSnapshot(snapshot, roomID) {
					return snapshot, nil
				}
			}
		}

		userConversation, err := ra.userConversationRepo.GetUserConversation(ctx, userID, conversationID)
		if err != nil {
			return nil, err
		}
		if userConversation == nil {
			return nil, conversationentity.ErrConversationNotCreated
		}

		snapshot := &summarycache.RoomUnreadSnapshot{
			RoomID:      roomID,
			UnreadBegin: userConversation.SummaryUnreadBeginSeq,
			UnreadEnd:   userConversation.SummaryUnreadEndSeq,
		}
		if !validRoomAgentUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
			return nil, errors.New("数据库中的 Room Agent 未读范围无效")
		}
		if ra.roomUnreadSnapshotCache != nil {
			_ = ra.roomUnreadSnapshotCache.SetActive(ctx, userID, roomID, *snapshot, ra.roomUnreadSnapshotTTL)
		}
		return snapshot, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		snapshot, ok := result.Val.(*summarycache.RoomUnreadSnapshot)
		if !ok || snapshot == nil {
			return nil, errors.New("Room Agent 未读范围查询结果类型无效")
		}
		return snapshot, nil
	}
}

func validRoomAgentUnreadRange(beginSeq, endSeq int64) bool {
	return (beginSeq == 0 && endSeq == 0) || (beginSeq > 0 && endSeq >= beginSeq)
}

func validRoomAgentSnapshot(snapshot *summarycache.RoomUnreadSnapshot, roomID string) bool {
	return snapshot != nil && snapshot.RoomID == roomID && validRoomAgentUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd)
}

func toRoomAgentContextMessage(message *messageentity.Message) RoomAgentContextMessage {
	return RoomAgentContextMessage{
		MessageID:      message.MessageId,
		ConversationID: message.ConversationId,
		SenderID:       message.SenderId,
		ClientMsgID:    message.ClientMsgId,
		Seq:            message.Seq,
		Type:           int32(message.Type),
		Content:        message.Content,
		Status:         int32(message.Status),
		SendTime:       message.SendTime,
	}
}

func roomAgentThreadID(userID, roomID string) string {
	scope := []byte(userID + ":" + roomID)
	threadID := sha256.Sum256(scope)
	return hex.EncodeToString(threadID[:])
}

func (ra *RoomAgentApplication) validateRoomMembership(ctx context.Context, roomID, userID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ra == nil || ra.roomUserRepository == nil {
		return errors.New("Room Agent 成员查询仓储未配置")
	}
	if ra.sf == nil {
		return errors.New("Room Agent singleflight 未初始化")
	}
	if ra.roomMemberCache != nil {
		state, hit, err := ra.roomMemberCache.GetMember(ctx, roomID, userID)
		if err == nil && hit {
			return validateRoomMemberState(state)
		}
	}

	resultCh := ra.sf.DoChan("room-member:"+roomID+":"+userID, func() (any, error) {
		// 再查一次缓存，避免等待 singleflight 期间前一个请求已经回填。
		if ra.roomMemberCache != nil {
			state, hit, cacheErr := ra.roomMemberCache.GetMember(ctx, roomID, userID)
			if cacheErr == nil && hit {
				if err := validateRoomMemberState(state); err != nil {
					return nil, err
				}
				return state, nil
			}
		}

		member, findErr := ra.roomUserRepository.GetRelationByIDs(userID, roomID)
		if errors.Is(findErr, roomentity.ErrMemberNotFound) {
			if ra.roomMemberCache != nil {
				_ = ra.roomMemberCache.SetMemberNotFound(ctx, roomID, userID)
			}
			return nil, ErrRoomAgentAccessDenied
		}
		if findErr != nil {
			return nil, findErr
		}
		if member == nil {
			if ra.roomMemberCache != nil {
				_ = ra.roomMemberCache.SetMemberNotFound(ctx, roomID, userID)
			}
			return nil, ErrRoomAgentAccessDenied
		}

		state := &roomcache.MemberState{
			Status:    member.Status,
			Role:      member.Role,
			MuteUntil: member.MuteUtil,
			Version:   member.Version,
		}
		if ra.roomMemberCache != nil {
			_, _ = ra.roomMemberCache.SetMemberIfVersionGreater(ctx, roomID, userID, state)
		}
		return state, nil
	})
	var value any
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return result.Err
		}
		value = result.Val
	}
	state, ok := value.(*roomcache.MemberState)
	if !ok {
		return errors.New("Room Agent 成员查询结果类型无效")
	}
	return validateRoomMemberState(state)
}

func validateRoomMemberState(state *roomcache.MemberState) error {
	if state == nil || (state.Status != roomvo.Activate && state.Status != roomvo.BeMuted) {
		return ErrRoomAgentAccessDenied
	}
	return nil
}
