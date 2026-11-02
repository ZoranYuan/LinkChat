package room

import (
	"context"
	"errors"
	"log"

	conversationapp "IM_backend/internal/application/conversation"
	domainconversation "IM_backend/internal/domain/conversation"
	domainroom "IM_backend/internal/domain/room"
)

func (ra *RoomApplication) Join(ctx context.Context, userID, inviteCode string) (*RoomMemberDTO, *RoomAppDTO, error) {
	roomID, err := ra.roomCache.GetRoomIDByCode(ctx, inviteCode)
	if err != nil {
		if errors.Is(err, domainroom.ErrInviteCodeExpired) {
			return nil, nil, ErrInviteCodeExpired
		}

		log.Println("根据邀请码获取房间 ID 失败：", err)
		return nil, nil, ErrUnknown
	}

	conversation, err := ra.conversationRepository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return nil, nil, err
	}
	if conversation == nil {
		return nil, nil, conversationapp.ErrConversationNotFound
	}
	conversationID := conversation.ConversationID

	var persistedMember *domainroom.RoomMember
	room, err := ra.GetActiveRoom(ctx, roomID)
	if err != nil {
		return nil, nil, err
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		transactionRoomRepository := ra.roomRepository.WithTx(tx)

		member, err := ra.roomMemberRepository.WithTx(tx).FindRoomMemberByUserIDAndRoomID(userID, roomID)

		switch {
		case errors.Is(err, domainroom.ErrMemberNotFound):
			// 第一次加入
			newRoomMember := domainroom.NewRoomMember(userID, roomID, domainroom.RegularUser)
			newRoomMember.Join()

			updated, err := transactionRoomRepository.IncrementMemberCount(ctx, roomID, int(domainroom.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionRoomRepository.FindRoomByRoomIDAndStatus(roomID, int(domainroom.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrRoomFull
			}

			persistedMember, err = ra.roomMemberRepository.WithTx(tx).JoinRoom(newRoomMember)

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

			updated, err := transactionRoomRepository.IncrementMemberCount(ctx, roomID, int(domainroom.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionRoomRepository.FindRoomByRoomIDAndStatus(roomID, int(domainroom.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrRoomFull
			}

			if err := ra.roomMemberRepository.WithTx(tx).RejoinRoom(member); err != nil {
				return err
			}

			persistedMember = member
		}
		room.MemberCount++

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

		// 用户退出后，重新加入房间需要重新创建一个 userConversation
		if err := ra.userConversationRepository.WithTx(tx).CreateUserConversation(ctx, userConversation); err != nil {
			if errors.Is(err, domainroom.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}
	ra.invalidateRoomMember(ctx, roomID, userID)
	ra.invalidateRoomInfo(ctx, roomID)

	return toRoomMemberDTO(persistedMember), toRoomAppDTO(room, ""), nil
}

func (ra *RoomApplication) Leave(ctx context.Context, userID, roomID string) error {
	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		roomMember, err := ra.roomMemberRepository.WithTx(tx).FindRoomMemberByUserIDAndRoomID(userID, roomID)
		if err != nil {
			return err
		}

		if roomMember == nil {
			return ErrNotRoomMember
		}

		if err := roomMember.Leave(); err != nil {
			return err
		}

		if err := ra.roomMemberRepository.WithTx(tx).LeaveRoom(roomMember, []int{int(domainroom.Active), int(domainroom.Muted)}); err != nil {
			if errors.Is(err, domainroom.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}
		updated, err := ra.roomRepository.WithTx(tx).DecrementMemberCount(ctx, roomID, int(domainroom.Normal))
		if err != nil {
			return err
		}
		if !updated {
			return ErrConcurrentUpdate
		}
		conversation, err := ra.conversationRepository.WithTx(tx).FindConversationByRoomID(ctx, roomID)
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
	ra.invalidateRoomMember(ctx, roomID, userID)
	ra.invalidateRoomInfo(ctx, roomID)

	return nil
}
