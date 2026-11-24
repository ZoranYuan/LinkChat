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
	groupcache "IM_backend/core/application/ports/cache/group"
	domainconversation "IM_backend/core/domain/conversation"
	domainmessage "IM_backend/core/domain/message"
	domaingroup "IM_backend/core/domain/group"
	domainuser "IM_backend/core/domain/user"
)

type GroupAgentApplication struct {
	groupMemberRepository       domaingroup.GroupMemberRepository
	conversationRepository     domainconversation.ConversationRepository
	userConversationRepository domainconversation.UserConversationRepository
	messageRepository          domainmessage.MessageRepository
	userRepository             domainuser.UserRepository
	groupMemberCache            groupcache.GroupMemberCache
	config                     configs.MessageConfig
	grpcConfig                 configs.GRPCConfig
	sf                         singleflight.Group
}

func NewGroupAgentApplication(
	groupMemberRepository domaingroup.GroupMemberRepository,
	conversationRepository domainconversation.ConversationRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	userRepository domainuser.UserRepository,
	messageRepository domainmessage.MessageRepository,
	groupMemberCache groupcache.GroupMemberCache,
	config configs.MessageConfig,
	grpcConfig configs.GRPCConfig,
) *GroupAgentApplication {
	return &GroupAgentApplication{
		groupMemberRepository:       groupMemberRepository,
		conversationRepository:     conversationRepository,
		userConversationRepository: userConversationRepository,
		userRepository:             userRepository,
		messageRepository:          messageRepository,
		groupMemberCache:            groupMemberCache,
		config:                     config,
		grpcConfig:                 grpcConfig,
	}
}

// sendMessagesByStream 按 (afterSeq, untilSeq] 分批查询，只输出未撤回的文本消息。
func (ra *GroupAgentApplication) sendMessagesByStream(
	ctx context.Context,
	conversationID string,
	afterSeq, untilSeq int64,
	limit int,
	send func([]ChatMessageDTO) error,
) error {
	if limit <= 0 {
		limit = ra.grpcConfig.GroupMessageSyncDefaultBatchSize
	}
	if limit > ra.grpcConfig.GroupMessageSyncMaxBatchSize {
		limit = ra.grpcConfig.GroupMessageSyncMaxBatchSize
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

func (ra *GroupAgentApplication) SearchGroupMessages(
	ctx context.Context,
	groupID, userID string,
	ranges [][2]int64,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyGroupAccess(ctx, groupID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByGroupID(ctx, groupID)
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

func (ra *GroupAgentApplication) SyncGroupMessages(
	ctx context.Context,
	groupID, userID string,
	afterSeq int64,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyGroupAccess(ctx, groupID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByGroupID(ctx, groupID)
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

func (ra *GroupAgentApplication) VerifyGroupAccess(ctx context.Context, groupID, userID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	groupID = strings.TrimSpace(groupID)
	userID = strings.TrimSpace(userID)
	if groupID == "" || userID == "" {
		return "", ErrGroupAgentAccessDenied
	}

	state, hit, err := ra.groupMemberCache.GetMember(ctx, groupID, userID)
	if err != nil || !hit {
		resultCh := ra.sf.DoChan("group-member:"+groupID+":"+userID, func() (any, error) {
			queryCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				time.Duration(ra.config.MemberQueryTimeoutSeconds)*time.Second,
			)
			defer cancel()

			// 合并回源前再次读取缓存，复用其他请求已回填的成员状态。
			if cached, found, err := ra.groupMemberCache.GetMember(queryCtx, groupID, userID); err == nil && found {
				return cached, nil
			}
			members, err := ra.groupMemberRepository.ListGroupMembersByGroupIDAndUserIDs(queryCtx, groupID, []string{userID})
			if err != nil {
				return nil, err
			}
			if len(members) == 0 || members[0] == nil {
				if err := ra.groupMemberCache.SetMemberNotFound(queryCtx, groupID, userID); err != nil {
					log.Printf("回填 Group Agent 成员空缓存失败：group=%s user=%s err=%v", groupID, userID, err)
				}
				return nil, ErrGroupAgentAccessDenied
			}
			member := members[0]
			memberState := &groupcache.MemberState{
				Status:    member.Status,
				Role:      member.Role,
				MuteUntil: member.MuteUntil,
				Version:   member.Version,
			}
			if _, err := ra.groupMemberCache.SetMemberIfVersionGreater(queryCtx, groupID, userID, memberState); err != nil {
				log.Printf("回填 Group Agent 成员缓存失败：group=%s user=%s err=%v", groupID, userID, err)
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
			state = result.Val.(*groupcache.MemberState)
		}
	}
	if state == nil || (state.Status != domaingroup.Active && state.Status != domaingroup.Muted) {
		return "", ErrGroupAgentAccessDenied
	}

	threadID := sha256.Sum256([]byte(userID + ":" + groupID))
	return hex.EncodeToString(threadID[:]), nil
}

func (ra *GroupAgentApplication) GetGroupUnreadMessages(
	ctx context.Context,
	groupID, userID string,
	limit uint32,
	send func([]ChatMessageDTO) error,
) error {
	if _, err := ra.VerifyGroupAccess(ctx, groupID, userID); err != nil {
		return err
	}

	conversation, err := ra.conversationRepository.FindConversationByGroupID(ctx, groupID)
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
