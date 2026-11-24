package group

import (
	"context"
	"errors"
	"log"

	conversationapp "IM_backend/core/application/conversation"
	domainconversation "IM_backend/core/domain/conversation"
	domaingroup "IM_backend/core/domain/group"
)

func (ra *GroupApplication) Join(ctx context.Context, userID, inviteCode string) (*GroupMemberDTO, *GroupAppDTO, error) {
	groupID, err := ra.groupCache.GetGroupIDByCode(ctx, inviteCode)
	if err != nil {
		if errors.Is(err, domaingroup.ErrInviteCodeExpired) {
			return nil, nil, ErrInviteCodeExpired
		}

		log.Println("根据邀请码获取群聊 ID 失败：", err)
		return nil, nil, ErrUnknown
	}

	conversation, err := ra.conversationRepository.FindConversationByGroupID(ctx, groupID)
	if err != nil {
		return nil, nil, err
	}
	if conversation == nil {
		return nil, nil, conversationapp.ErrConversationNotFound
	}
	conversationID := conversation.ConversationID

	var persistedMember *domaingroup.GroupMember
	group, err := ra.GetActiveGroup(ctx, groupID)
	if err != nil {
		return nil, nil, err
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		transactionGroupRepository := ra.groupRepository.WithTx(tx)

		member, err := ra.groupMemberRepository.WithTx(tx).FindGroupMemberByUserIDAndGroupID(userID, groupID)

		switch {
		case errors.Is(err, domaingroup.ErrMemberNotFound):
			// 第一次加入
			newGroupMember := domaingroup.NewGroupMember(userID, groupID, domaingroup.RegularUser)
			newGroupMember.Join()

			updated, err := transactionGroupRepository.IncrementMemberCount(ctx, groupID, int(domaingroup.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionGroupRepository.FindGroupByGroupIDAndStatus(groupID, int(domaingroup.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrGroupFull
			}

			persistedMember, err = ra.groupMemberRepository.WithTx(tx).JoinGroup(newGroupMember)

			if err != nil {
				return err
			}
		case err != nil:
			return err

		default:
			// 重新加入
			if err := member.ReJoin(); err != nil {
				return err
			}

			updated, err := transactionGroupRepository.IncrementMemberCount(ctx, groupID, int(domaingroup.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionGroupRepository.FindGroupByGroupIDAndStatus(groupID, int(domaingroup.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrGroupFull
			}

			if err := ra.groupMemberRepository.WithTx(tx).RejoinGroup(member); err != nil {
				return err
			}

			persistedMember = member
		}
		group.MemberCount++

		conv, err := ra.conversationRepository.WithTx(tx).FindConversationByID(ctx, conversationID)
		if err != nil {
			return err
		}
		if conv == nil {
			return conversationapp.ErrConversationNotFound
		}

		userConversation := domainconversation.BuildUserConversation(
			userID,
			conversationID,
			conv.LatestSeq,
		)

		// 用户退出后，重新加入群聊需要重新创建一个 userConversation
		if err := ra.userConversationRepository.WithTx(tx).CreateUserConversation(ctx, userConversation); err != nil {
			if errors.Is(err, domaingroup.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}
	ra.invalidateGroupMember(ctx, groupID, userID)
	ra.invalidateGroupInfo(ctx, groupID)

	return toGroupMemberDTO(persistedMember), toGroupAppDTO(group, ""), nil
}

func (ra *GroupApplication) Leave(ctx context.Context, userID, groupID string) error {
	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		groupMember, err := ra.groupMemberRepository.WithTx(tx).FindGroupMemberByUserIDAndGroupID(userID, groupID)
		if err != nil {
			return err
		}

		if groupMember == nil {
			return ErrNotGroupMember
		}

		if err := groupMember.Leave(); err != nil {
			return err
		}

		if err := ra.groupMemberRepository.WithTx(tx).LeaveGroup(groupMember, []int{int(domaingroup.Active), int(domaingroup.Muted)}); err != nil {
			if errors.Is(err, domaingroup.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}
		updated, err := ra.groupRepository.WithTx(tx).DecrementMemberCount(ctx, groupID, int(domaingroup.Normal))
		if err != nil {
			return err
		}
		if !updated {
			return ErrConcurrentUpdate
		}
		conversation, err := ra.conversationRepository.WithTx(tx).FindConversationByGroupID(ctx, groupID)
		if err != nil {
			return err
		}
		if conversation == nil {
			return conversationapp.ErrConversationNotFound
		}
		return ra.userConversationRepository.WithTx(tx).DelUserConversation(ctx, userID, conversation.ConversationID)
	}); err != nil {
		return err
	}
	ra.invalidateGroupMember(ctx, groupID, userID)
	ra.invalidateGroupInfo(ctx, groupID)

	return nil
}
