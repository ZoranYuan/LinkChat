package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/cache/room"
	domainconversation "IM_backend/internal/domain/conversation"
	domainmessage "IM_backend/internal/domain/message"
	domainroom "IM_backend/internal/domain/room"
	domainuser "IM_backend/internal/domain/user"
)

type RoomAgentApplication struct {
	roomMemberRepository       domainroom.RoomMemberRepository
	conversationRepository     domainconversation.ConversationRepository
	userConversationRepository domainconversation.UserConversationRepository
	messageRepository          domainmessage.MessageRepository
	userRepository             domainuser.UserRepository
	roomMemberCache            roomcache.RoomMemberCache
	config                     configs.MessageConfig
	grpcConfig                 configs.GRPCConfig
	sf                         singleflight.Group
}

func NewRoomAgentApplication(
	roomMemberRepository domainroom.RoomMemberRepository,
	conversationRepository domainconversation.ConversationRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	userRepository domainuser.UserRepository,
	messageRepository domainmessage.MessageRepository,
	roomMemberCache roomcache.RoomMemberCache,
	config configs.MessageConfig,
	grpcConfig configs.GRPCConfig,
) *RoomAgentApplication {
	return &RoomAgentApplication{
		roomMemberRepository:       roomMemberRepository,
		conversationRepository:     conversationRepository,
		userConversationRepository: userConversationRepository,
		userRepository:             userRepository,
		messageRepository:          messageRepository,
		roomMemberCache:            roomMemberCache,
		config:                     config,
		grpcConfig:                 grpcConfig,
	}
}

// sendMessagesByStream 按 (afterSeq, untilSeq] 分批查询，只输出未撤回的文本消息。
func (ra *RoomAgentApplication) sendMessagesByStream(
	ctx context.Context,
	conversationID string,
	afterSeq, untilSeq int64,
	limit int,
	send func([]ChatMessageDTO) error,
) error {
	if limit <= 0 {
		limit = ra.grpcConfig.RoomMessageSyncDefaultBatchSize
	}
	if limit > ra.grpcConfig.RoomMessageSyncMaxBatchSize {
		limit = ra.grpcConfig.RoomMessageSyncMaxBatchSize
	}

	for afterSeq < untilSeq {
		if err := ctx.Err(); err != nil {
			return err
		}

		messages, err := ra.messageRepository.ListAfterSeqUntil(
			ctx, conversationID, afterSeq, untilSeq, limit,
		)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}

		// 先推进原始查询游标，过滤后为空也能继续读取下一批。
		afterSeq = messages[len(messages)-1].Seq

		items := make([]ChatMessageDTO, 0, len(messages))
		senderIDs := make([]string, 0, len(messages))
		senders := make(map[string]struct{})

		for _, message := range messages {
			if message.Type != domainmessage.Text ||
				message.Status == domainmessage.Withdrawn {
				continue
			}

			items = append(items, ChatMessageDTO{
				MessageID:      message.MessageID,
				ConversationID: message.ConversationID,
				SenderID:       message.SenderID,
				Seq:            message.Seq,
				Type:           0,
				Content:        message.Content,
				Status:         0,
				SendTime:       message.SendTime,
			})

			if _, exists := senders[message.SenderID]; !exists {
				senders[message.SenderID] = struct{}{}
				senderIDs = append(senderIDs, message.SenderID)
			}
		}

		// 当前一批消息可能是撤回或者不满足条件的消息，这里需要再次查询
		if len(items) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		// 批量查询用户信息
		users, err := ra.userRepository.ListUsersByUserIDs(senderIDs)
		if err != nil {
			return err
		}

		names := make(map[string]string, len(users))
		for _, user := range users {
			name := user.Nickname
			if name == "" {
				name = user.Username
			}
			names[user.UserID] = name
		}
		for i := range items {
			items[i].SenderName = names[items[i].SenderID]
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		if err := send(items); err != nil {
			return err
		}
	}

	return nil
}

func (ra *RoomAgentApplication) SearchRoomMessages(
	ctx context.Context,
	roomID, userID string,
	ranges [][2]int64,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyRoomAccess(ctx, roomID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return err
	}
	if conversation == nil {
		return domainconversation.ErrConversationNotCreated
	}

	// 区间已由 Agent 校验、排序及合并，本次查询不超过开始时的最新序号。
	untilSeq := conversation.LatestSeq
	for _, item := range ranges {
		if err := ra.sendMessagesByStream(
			ctx,
			conversation.ConversationID,
			item[0]-1,
			min(item[1], untilSeq),
			int(limit),
			send,
		); err != nil {
			return err
		}
	}
	return nil
}

func (ra *RoomAgentApplication) SyncRoomMessages(
	ctx context.Context,
	roomID, userID string,
	afterSeq int64,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyRoomAccess(ctx, roomID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return err
	}
	if conversation == nil {
		return domainconversation.ErrConversationNotCreated
	}

	return ra.sendMessagesByStream(
		ctx,
		conversation.ConversationID,
		afterSeq,
		conversation.LatestSeq,
		int(limit),
		send,
	)
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

func (ra *RoomAgentApplication) GetRoomUnreadMessages(
	ctx context.Context,
	roomID, userID string,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyRoomAccess(ctx, roomID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return err
	}
	if conversation == nil {
		return domainconversation.ErrConversationNotCreated
	}

	userConversation, err := ra.userConversationRepository.FindUserConversationByUserIDAndConversationID(
		ctx, userID, conversation.ConversationID,
	)
	if err != nil {
		return err
	}
	if userConversation == nil {
		return domainconversation.ErrConversationNotCreated
	}

	// 固定已保存的未读范围，不使用当前已读水位重新计算，也不修改快照。
	beginSeq := userConversation.SummaryUnreadBeginSeq
	endSeq := userConversation.SummaryUnreadEndSeq
	if beginSeq == 0 && endSeq == 0 {
		return nil
	}

	return ra.sendMessagesByStream(
		ctx,
		conversation.ConversationID,
		beginSeq-1,
		endSeq,
		int(limit),
		send,
	)
}
