package room

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"encoding/json"
	"errors"
	"log"

	conversationapp "IM_backend/internal/application/conversation"
	roomcache "IM_backend/internal/application/ports/cache/room"
	outboxport "IM_backend/internal/application/ports/outbox"
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
		if err := ra.writeMemberChangedEvent(ctx, tx, persistedMember); err != nil {
			return err
		}

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
	ra.notifyProducer()
	ra.invalidateRoomInfo(ctx, roomID)

	ra.cacheRoomMemberState(ctx, persistedMember)
	if ra.roomPresence != nil {
		ra.roomPresence.BindUserToRoom(userID, roomID)
	}

	return toRoomMemberDTO(persistedMember), toRoomAppDTO(room, ""), nil
}

func (ra *RoomApplication) writeMemberChangedEvent(ctx context.Context, tx any, member *domainroom.RoomMember) error {
	if ra.roomMemberOutboxRepository == nil || member == nil {
		return nil
	}
	payload, err := json.Marshal(messaging.RoomMemberChangedEvent{
		RoomID: member.RoomID, UserID: member.UserID,
		Status: int(member.Status), Role: int(member.Role),
		MuteUntil: member.MuteUntil, Version: member.Version,
	})
	if err != nil {
		return err
	}
	return ra.roomMemberOutboxRepository.WithTx(tx).Create(ctx, &outboxport.Entry{
		EventType:    messaging.TypeRoomMemberChanged,
		PartitionKey: member.RoomID + ":" + member.UserID,
		Payload:      payload,
	})
}

func (ra *RoomApplication) ApplyMemberChanged(ctx context.Context, event messaging.RoomMemberChangedEvent) error {
	state := &roomcache.MemberState{
		Status:    domainroom.RoomMemberStatus(event.Status),
		Role:      domainroom.Role(event.Role),
		MuteUntil: event.MuteUntil,
		Version:   event.Version,
	}
	updated := true
	if ra.roomMemberCache != nil {
		var err error
		updated, err = ra.roomMemberCache.SetMemberIfVersionGreater(ctx, event.RoomID, event.UserID, state)
		if err != nil {
			return err
		}
	}
	if !updated || ra.roomPresence == nil {
		return nil
	}
	if state.Status == domainroom.Active || state.Status == domainroom.Muted {
		ra.roomPresence.BindUserToRoom(event.UserID, event.RoomID)
	} else {
		ra.roomPresence.UnbindUserFromRoom(event.UserID, event.RoomID)
	}
	return nil
}

func (ra *RoomApplication) cacheRoomMemberState(ctx context.Context, member *domainroom.RoomMember) {
	if ra.roomMemberCache == nil || member == nil {
		return
	}
	_, err := ra.roomMemberCache.SetMemberIfVersionGreater(ctx, member.RoomID, member.UserID, &roomcache.MemberState{
		Status: member.Status, Role: member.Role, MuteUntil: member.MuteUntil, Version: member.Version,
	})
	if err != nil {
		log.Printf("回填房间成员缓存失败：房间=%s 用户=%s 版本=%d 错误=%v", member.RoomID, member.UserID, member.Version, err)
	}
}

func (ra *RoomApplication) Leave(ctx context.Context, userID, roomID string) error {
	var memberState *roomcache.MemberState
	var exist bool
	var persistedMember *domainroom.RoomMember
	if ra.roomMemberCache != nil {
		memberState, exist, _ = ra.roomMemberCache.GetMember(ctx, roomID, userID)
	}

	if exist && (memberState == nil ||
		(memberState.Status != domainroom.Active &&
			memberState.Status != domainroom.Muted)) {
		return ErrNotRoomMember
	}

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
		persistedMember = roomMember
		updated, err := ra.roomRepository.WithTx(tx).DecrementMemberCount(ctx, roomID, int(domainroom.Normal))
		if err != nil {
			return err
		}
		if !updated {
			return ErrConcurrentUpdate
		}
		if err := ra.writeMemberChangedEvent(ctx, tx, roomMember); err != nil {
			return err
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
	ra.notifyProducer()
	ra.invalidateRoomInfo(ctx, roomID)

	ra.cacheRoomMemberState(ctx, persistedMember)
	if ra.roomPresence != nil {
		ra.roomPresence.UnbindUserFromRoom(userID, roomID)
	}

	return nil
}
