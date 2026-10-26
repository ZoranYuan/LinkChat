package room

import (
	"context"
	"errors"
	"log"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	roomcache "IM_backend/internal/application/ports/cache/room"
	idport "IM_backend/internal/application/ports/id"
	"IM_backend/internal/application/ports/messaging"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/application/ports/realtime"
	"IM_backend/internal/application/ports/transaction"
	domainconversation "IM_backend/internal/domain/conversation"
	domainroom "IM_backend/internal/domain/room"
)

type RoomApplication struct {
	config                     configs.RoomConfig
	roomRepository             domainroom.RoomRepository
	roomMemberRepository       domainroom.RoomMemberRepository
	userConversationRepository domainconversation.UserConversationRepository
	conversationRepository     domainconversation.ConversationRepository
	conversationApplication    *conversationapp.ConversationApplication
	roomCache                  roomcache.RoomCache
	roomMemberCache            roomcache.RoomMemberCache
	roomInfoCache              roomcache.RoomInfoCache
	roomInfoCacheTTL           time.Duration
	txManager                  transaction.Manager
	idGenerator                idport.Generator
	roomPresence               realtime.RoomPresence
	roomMemberOutboxRepository outboxport.OutboxRepository
	producerNotifier           messaging.ProducerNotifier
}

func NewRoomApplication(roomRepository domainroom.RoomRepository,
	roomMemberRepository domainroom.RoomMemberRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	roomCache roomcache.RoomCache,
	roomMemberCache roomcache.RoomMemberCache,
	txManager transaction.Manager,
	idGenerator idport.Generator,
	roomPresence realtime.RoomPresence,
	roomMemberOutboxRepository outboxport.OutboxRepository,
	config configs.RoomConfig,
) *RoomApplication {
	return &RoomApplication{
		config:                     config,
		roomRepository:             roomRepository,
		roomMemberRepository:       roomMemberRepository,
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
func (ra *RoomApplication) SetProducerNotifier(notifier messaging.ProducerNotifier) {
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

func (ra *RoomApplication) notifyProducer() {
	if ra != nil && ra.producerNotifier != nil {
		ra.producerNotifier.Notify()
	}
}

func (ra *RoomApplication) Create(ctx context.Context, userID, roomName, avatar, description string) (*RoomAppDTO, error) {
	roomID, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	conversationID, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	room, err := domainroom.NewRoom(roomID, userID, description, roomName, avatar, ra.config.MaxMembers)
	if err != nil {
		if errors.Is(err, domainroom.ErrRoomNameRequired) {
			return nil, ErrRoomNameRequired
		} else {
			return nil, ErrUnknown
		}
	}
	roomMember := domainroom.NewRoomMember(userID, roomID, domainroom.HomeOwner)
	roomMember.Join()

	conversation := domainconversation.NewRoomConversation(conversationID, roomID)

	userConversation := domainconversation.BuildUserConversation(
		userID,
		conversationID,
		0,
	)

	inviteCode, err := ra.roomCache.UpdateInviteCode(ctx, roomID, time.Duration(ra.config.InviteCodeTTLSeconds)*time.Second)
	if err != nil {
		log.Println("生成邀请码失败：", err)
		return nil, ErrInviteCodeUnavailable
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		roomRepository := ra.roomRepository.WithTx(tx)
		roomMemberRepository := ra.roomMemberRepository.WithTx(tx)
		conversationRepository := ra.conversationRepository.WithTx(tx)
		userConversationRepository := ra.userConversationRepository.WithTx(tx)

		if err := roomRepository.Create(room); err != nil {
			if errors.Is(err, domainroom.ErrDuplicateCreation) {
				return ErrConcurrentUpdate
			}
			return err
		}

		if _, err := roomMemberRepository.JoinRoom(roomMember); err != nil {
			return err
		}
		if err := ra.writeMemberChangedEvent(ctx, tx, roomMember); err != nil {
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
		if cleanupErr := ra.roomCache.DeleteInviteCode(ctx, roomID); cleanupErr != nil {
			log.Println("清理邀请码缓存失败：", cleanupErr)
		}
		return nil, err
	}
	ra.notifyProducer()
	ra.cacheRoomInfo(ctx, room)
	ra.conversationApplication.CacheConversationInfo(ctx, conversation)

	ra.cacheRoomMemberState(ctx, roomMember)
	if ra.roomPresence != nil {
		ra.roomPresence.BindUserToRoom(userID, roomID)
	}

	return toRoomAppDTO(room, inviteCode), nil
}

func (ra *RoomApplication) Invite(ctx context.Context, userID, roomID string) (string, error) {
	room, err := ra.GetActiveRoom(ctx, roomID)
	if err != nil {
		if errors.Is(err, domainroom.ErrRoomNotFound) {
			return "", ErrRoomNotFound
		}

		log.Println("查询房间失败：", err)
		return "", ErrUnknown
	}

	roomMember, err := ra.roomMemberRepository.FindRoomMemberByUserIDAndRoomID(userID, roomID)
	if err != nil {
		if errors.Is(err, domainroom.ErrMemberNotFound) {
			return "", ErrNotRoomMember
		}
		return "", ErrUnknown
	}

	if err := roomMember.Invite(); err != nil {
		return "", ErrPermissionDenied
	}

	if !room.Invite() {
		return "", ErrRoomUnavailable
	}

	inviteCode, err := ra.roomCache.UpdateInviteCode(ctx, roomID, time.Duration(ra.config.InviteCodeTTLSeconds)*time.Second)

	if err != nil {
		log.Println("获取邀请码失败：", err)
		return "", ErrInviteCodeUnavailable
	}

	return inviteCode, nil
}

// GetActiveRoom 复用基础信息缓存，统一拒绝已失效的房间。
func (ra *RoomApplication) GetActiveRoom(ctx context.Context, roomID string) (*domainroom.Room, error) {
	room, err := ra.GetRoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if room.Status != domainroom.Normal {
		return nil, domainroom.ErrRoomNotFound
	}
	return room, nil
}

// GetRoomInfo 先读 Redis；缓存未命中或不可用时回源 MySQL 并尝试回填。
func (ra *RoomApplication) GetRoomInfo(ctx context.Context, roomID string) (*domainroom.Room, error) {
	if roomID == "" {
		return nil, domainroom.ErrRoomNotFound
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

	room, err := ra.roomRepository.FindRoomByRoomID(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if room == nil {
		return nil, domainroom.ErrRoomNotFound
	}
	ra.cacheRoomInfo(ctx, room)
	return room, nil
}

func (ra *RoomApplication) cacheRoomInfo(ctx context.Context, room *domainroom.Room) {
	if ra == nil || ra.roomInfoCache == nil || room == nil || ra.roomInfoCacheTTL <= 0 {
		return
	}
	if err := ra.roomInfoCache.SetRoomInfo(ctx, room, ra.roomInfoCacheTTL); err != nil {
		log.Printf("写入房间基础信息缓存失败：room=%s error=%v", room.RoomID, err)
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
