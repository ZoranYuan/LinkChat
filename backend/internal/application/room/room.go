package room

import (
	conversationapp "IM_backend/internal/application/conversation"
	eventbus "IM_backend/internal/application/ports/eventbus"
	idport "IM_backend/internal/application/ports/id"
	outboxport "IM_backend/internal/application/ports/outbox"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	"IM_backend/internal/application/ports/realtime"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"
)

type RoomApplication struct {
	roomRepository             roomrepo.RoomRepository
	roomUserRepository         roomrepo.RoomUserRepository
	userConversationRepository conversationrepo.UserConversationRepository
	conversationRepository     conversationrepo.ConversationRepository
	conversationApplication    *conversationapp.ConversationApplication
	roomCache                  roomcache.RoomCache
	roomMemberCache            roomcache.RoomMemberCache
	roomInfoCache              roomcache.RoomInfoCache
	roomInfoCacheTTL           time.Duration
	txManager                  txmanager.TxManager
	idGenerator                idport.Generator
	roomPresence               realtime.RoomPresence
	roomMemberOutboxRepository outboxport.OutboxRepository
	producerNotifier           eventbus.ProducerNotifier
}

func NewRoomApplication(roomRepository roomrepo.RoomRepository,
	roomUserRepository roomrepo.RoomUserRepository,
	userConversationRepository conversationrepo.UserConversationRepository,
	conversationRepository conversationrepo.ConversationRepository,
	roomCache roomcache.RoomCache,
	roomMemberCache roomcache.RoomMemberCache,
	txManager txmanager.TxManager,
	idGenerator idport.Generator,
	roomPresence realtime.RoomPresence,
	roomMemberOutboxRepository outboxport.OutboxRepository,
) *RoomApplication {
	return &RoomApplication{
		roomRepository:             roomRepository,
		roomUserRepository:         roomUserRepository,
		conversationRepository:     conversationRepository,
		userConversationRepository: userConversationRepository,
		roomCache:                  roomCache,
		roomMemberCache:            roomMemberCache,
		txManager:                  txManager,
		idGenerator:                idGenerator,
		roomPresence:               roomPresence,
		roomMemberOutboxRepository: roomMemberOutboxRepository,
	}
}

// SetProducerNotifier 在事务提交后唤醒 Producer。
func (ra *RoomApplication) SetProducerNotifier(notifier eventbus.ProducerNotifier) {
	if ra == nil {
		return
	}
	ra.producerNotifier = notifier
}

func (ra *RoomApplication) SetRoomInfoCache(cache roomcache.RoomInfoCache, ttl time.Duration) {
	if ra != nil {
		ra.roomInfoCache = cache
		ra.roomInfoCacheTTL = ttl
	}
}

func (ra *RoomApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	ra.conversationApplication = application
}

func (ra *RoomApplication) findActiveRoom(ctx context.Context, roomID string, status int) (*roomentity.Room, error) {
	room, err := ra.GetRoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if int(room.Status) != status {
		return nil, roomentity.ErrRoomNotFound
	}
	return room, nil
}

func (ra *RoomApplication) notifyProducer() {
	if ra != nil && ra.producerNotifier != nil {
		ra.producerNotifier.Notify()
	}
}

func (ra *RoomApplication) Create(ctx context.Context, userId, roomName, avatar, description string) (*RoomAppDTO, error) {
	roomId, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	conversationId, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	room, err := roomentity.NewRoom(roomId, userId, description, roomName, avatar)
	if err != nil {
		if errors.Is(err, roomentity.ErrRoomNameRequired) {
			return nil, ErrRoomNameRequired
		} else {
			return nil, ErrUnknown
		}
	}
	roomUser := roomentity.NewRoomUser(userId, roomId, roomvo.HomeOwner)
	roomUser.Join()

	conversation := conversationentity.NewConversation(
		conversationId,
		userId,
		roomId,
		int(conversationvo.RoomChat),
		0,
		"",
	)

	userConversation := conversationentity.BuildUserConversation(
		userId,
		conversationId,
		0,
	)

	inviteCode, err := ra.roomCache.UpdateInviteCode(ctx, roomId, 5*time.Minute)
	if err != nil {
		log.Println("生成邀请码失败：", err)
		return nil, ErrInviteCodeUnavailable
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		roomRepository := ra.roomRepository.WithTx(tx)
		roomUserRepository := ra.roomUserRepository.WithTx(tx)
		conversationRepository := ra.conversationRepository.WithTx(tx)
		userConversationRepository := ra.userConversationRepository.WithTx(tx)

		if err := roomRepository.Create(room); err != nil {
			if errors.Is(err, roomentity.ErrDuplicateCreation) {
				return ErrConcurrentUpdate
			}
			return err
		}

		if _, err := roomUserRepository.JoinRoom(roomUser); err != nil {
			return err
		}
		if err := ra.writeMemberChangedEvent(ctx, tx, roomUser); err != nil {
			return err
		}

		if err := conversationRepository.CreateConversation(ctx, conversation); err != nil {
			return err
		}

		if err := userConversationRepository.CreateUserConversation(ctx, userConversation); err != nil {
			return err
		}
		return nil
	}); err != nil {
		if cleanupErr := ra.roomCache.DeleteInviteCode(ctx, roomId); cleanupErr != nil {
			log.Println("清理邀请码缓存失败：", cleanupErr)
		}
		return nil, err
	}
	ra.notifyProducer()
	ra.cacheRoomInfo(ctx, room)
	ra.conversationApplication.CacheConversationInfo(ctx, conversation)

	ra.invalidateRoomMemberCache(ctx, roomId, userId)
	if ra.roomPresence != nil {
		ra.roomPresence.BindUserToRoom(userId, roomId)
	}

	return toRoomAppDTO(room, inviteCode), nil
}

func (ra *RoomApplication) Invite(ctx context.Context, userId, roomId string) (string, error) {
	room, err := ra.findActiveRoom(ctx, roomId, int(roomvo.Normal))
	if err != nil {
		if errors.Is(err, roomentity.ErrRoomNotFound) {
			return "", ErrRoomNotFound
		}

		log.Println("查询房间失败：", err)
		return "", ErrUnknown
	}

	roomUser, err := ra.roomUserRepository.GetRelationByIDs(userId, roomId)
	if err != nil {
		if errors.Is(err, roomentity.ErrMemberNotFound) {
			return "", ErrNotRoomMember
		}
		return "", ErrUnknown
	}

	if err := roomUser.Invite(); err != nil {
		return "", ErrPermissionDenied
	}

	if !room.Invite() {
		return "", ErrRoomUnavailable
	}

	inviteCode, err := ra.roomCache.UpdateInviteCode(ctx, roomId, 5*time.Minute)

	if err != nil {
		log.Println("获取邀请码失败：", err)
		return "", ErrInviteCodeUnavailable
	}

	return inviteCode, nil
}

func (ra *RoomApplication) Join(ctx context.Context, userId, inviteCode string) (*RoomUserDTO, *RoomAppDTO, error) {
	roomId, err := ra.roomCache.GetRoomIDByCode(ctx, inviteCode)
	if err != nil {
		if errors.Is(err, roomentity.ErrInviteCodeExpired) {
			return nil, nil, ErrInviteCodeExpired
		}

		log.Println("根据邀请码获取房间 ID 失败：", err)
		return nil, nil, ErrUnknown
	}

	conversation, err := ra.conversationRepository.FindRoomConversation(ctx, roomId)
	if err != nil {
		return nil, nil, err
	}
	if conversation == nil {
		return nil, nil, conversationapp.ErrConversationNotFound
	}
	conversationId := conversation.ConversationId

	var persistedMember *roomentity.RoomUser
	room, err := ra.findActiveRoom(ctx, roomId, int(roomvo.Normal))
	if err != nil {
		return nil, nil, err
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		transactionRoomRepository := ra.roomRepository.WithTx(tx)

		member, err := ra.roomUserRepository.WithTx(tx).GetRelationByIDs(userId, roomId)

		switch {
		case errors.Is(err, roomentity.ErrMemberNotFound):
			// 第一次加入
			newRoomUser := roomentity.NewRoomUser(userId, roomId, roomvo.RegularUser)
			newRoomUser.Join()

			updated, err := transactionRoomRepository.IncrementMemberCount(ctx, roomId, int(roomvo.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionRoomRepository.FindActiveRoom(roomId, int(roomvo.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrRoomFull
			}

			persistedMember, err = ra.roomUserRepository.WithTx(tx).JoinRoom(newRoomUser)

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

			updated, err := transactionRoomRepository.IncrementMemberCount(ctx, roomId, int(roomvo.Normal))
			if err != nil {
				return err
			}
			if !updated {
				if _, checkErr := transactionRoomRepository.FindActiveRoom(roomId, int(roomvo.Normal)); checkErr != nil {
					return checkErr
				}
				return ErrRoomFull
			}

			if err := ra.roomUserRepository.WithTx(tx).RejoinRoom(member); err != nil {
				return err
			}

			persistedMember = member
		}
		room.MemberCount++
		if err := ra.writeMemberChangedEvent(ctx, tx, persistedMember); err != nil {
			return err
		}

		conv, err := ra.conversationRepository.WithTx(tx).GetByID(ctx, conversationId)
		if err != nil {
			return err
		}
		if conv == nil {
			return conversationapp.ErrConversationNotFound
		}

		userConversation := conversationentity.BuildUserConversation(
			userId,
			conversationId,
			conv.LatestSeq,
		)

		// 用户退出后，重新加入房间需要重新创建一个 userConversation
		if err := ra.userConversationRepository.WithTx(tx).CreateUserConversation(ctx, userConversation); err != nil {
			if errors.Is(err, roomentity.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}
	ra.notifyProducer()
	ra.invalidateRoomInfo(ctx, roomId)

	ra.invalidateRoomMemberCache(ctx, roomId, userId)
	if ra.roomPresence != nil {
		ra.roomPresence.BindUserToRoom(userId, roomId)
	}

	return toRoomUserDTO(persistedMember), toRoomAppDTO(room, ""), nil
}

func (ra *RoomApplication) writeMemberChangedEvent(ctx context.Context, tx any, member *roomentity.RoomUser) error {
	if ra.roomMemberOutboxRepository == nil || member == nil {
		return nil
	}
	payload, err := json.Marshal(protocol.RoomMemberChangedEvent{
		RoomID: member.RoomId, UserID: member.UserId,
		Status: int(member.Status), Role: int(member.Role),
		MuteUntil: member.MuteUtil, Version: member.Version,
	})
	if err != nil {
		return err
	}
	return ra.roomMemberOutboxRepository.WithTx(tx).Create(ctx, &outboxport.Entry{
		EventType:    protocol.EventRoomMemberChanged,
		PartitionKey: member.RoomId + ":" + member.UserId,
		Payload:      payload,
	})
}

func (ra *RoomApplication) ApplyMemberChanged(ctx context.Context, event protocol.RoomMemberChangedEvent) error {
	state := &roomcache.MemberState{
		Status:    roomvo.RoomUserStatus(event.Status),
		Role:      roomvo.Role(event.Role),
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
	if state.Status == roomvo.Activate || state.Status == roomvo.BeMuted {
		ra.roomPresence.BindUserToRoom(event.UserID, event.RoomID)
	} else {
		ra.roomPresence.UnbindUserFromRoom(event.UserID, event.RoomID)
	}
	return nil
}

func (ra *RoomApplication) invalidateRoomMemberCache(ctx context.Context, roomID, userID string) {
	if ra.roomMemberCache == nil {
		return
	}
	if err := ra.roomMemberCache.DeleteMember(ctx, roomID, userID); err != nil {
		log.Printf("删除房间成员缓存失败：房间=%s 用户=%s 错误=%v", roomID, userID, err)
	}
}

func (ra *RoomApplication) Leave(ctx context.Context, userId, roomId string) error {
	var memberState *roomcache.MemberState
	var exist bool
	if ra.roomMemberCache != nil {
		memberState, exist, _ = ra.roomMemberCache.GetMember(ctx, roomId, userId)
	}

	if exist && (memberState == nil ||
		(memberState.Status != roomvo.Activate &&
			memberState.Status != roomvo.BeMuted)) {
		return ErrNotRoomMember
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		roomUser, err := ra.roomUserRepository.WithTx(tx).GetRelationByIDs(userId, roomId)
		if err != nil {
			return err
		}

		if roomUser == nil {
			return ErrNotRoomMember
		}

		if err := roomUser.Leave(); err != nil {
			return err
		}

		if err := ra.roomUserRepository.WithTx(tx).LeaveRoom(roomUser, []int{int(roomvo.Activate), int(roomvo.BeMuted)}); err != nil {
			if errors.Is(err, roomentity.ErrVersionConflict) {
				return ErrConcurrentUpdate
			}
			return ErrUnknown
		}
		updated, err := ra.roomRepository.WithTx(tx).DecrementMemberCount(ctx, roomId, int(roomvo.Normal))
		if err != nil {
			return err
		}
		if !updated {
			return ErrConcurrentUpdate
		}
		if err := ra.writeMemberChangedEvent(ctx, tx, roomUser); err != nil {
			return err
		}

		conversation, err := ra.conversationRepository.WithTx(tx).FindRoomConversation(ctx, roomId)
		if err != nil {
			return err
		}
		if conversation == nil {
			return conversationapp.ErrConversationNotFound
		}
		return ra.userConversationRepository.WithTx(tx).DelUserConversation(ctx, userId, conversation.ConversationId)
	}); err != nil {
		return err
	}
	ra.notifyProducer()
	ra.invalidateRoomInfo(ctx, roomId)

	ra.invalidateRoomMemberCache(ctx, roomId, userId)
	if ra.roomPresence != nil {
		ra.roomPresence.UnbindUserFromRoom(userId, roomId)
	}

	return nil
}

// GetRoomInfo 先读 Redis；缓存未命中或不可用时回源 MySQL 并尝试回填。
func (ra *RoomApplication) GetRoomInfo(ctx context.Context, roomID string) (*roomentity.Room, error) {
	if ra == nil || ra.roomRepository == nil {
		return nil, errors.New("房间应用未配置")
	}
	if roomID == "" {
		return nil, roomentity.ErrRoomNotFound
	}
	if ra.roomInfoCache != nil {
		cached, found, err := ra.roomInfoCache.GetRoomInfo(ctx, roomID)
		if err == nil && found && cached != nil {
			return cached, nil
		}
		if err != nil {
			log.Printf("读取房间基础信息缓存失败，回源 MySQL：room=%s error=%v", roomID, err)
		}
	}

	room, err := ra.roomRepository.FindByID(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if room == nil {
		return nil, roomentity.ErrRoomNotFound
	}
	ra.cacheRoomInfo(ctx, room)
	return room, nil
}

func (ra *RoomApplication) cacheRoomInfo(ctx context.Context, room *roomentity.Room) {
	if ra == nil || ra.roomInfoCache == nil || room == nil || ra.roomInfoCacheTTL <= 0 {
		return
	}
	if err := ra.roomInfoCache.SetRoomInfo(ctx, room, ra.roomInfoCacheTTL); err != nil {
		log.Printf("写入房间基础信息缓存失败：room=%s error=%v", room.RoomId, err)
	}
}

func (ra *RoomApplication) invalidateRoomInfo(ctx context.Context, roomID string) {
	if ra == nil || ra.roomInfoCache == nil || roomID == "" {
		return
	}
	if err := ra.roomInfoCache.DeleteRoomInfo(ctx, roomID); err != nil {
		log.Printf("删除房间基础信息缓存失败：room=%s error=%v", roomID, err)
	}
}
