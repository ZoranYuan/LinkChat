package message

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	roomunreadcache "IM_backend/internal/application/ports/cache/agent"
	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	filecache "IM_backend/internal/application/ports/cache/file"
	friendcache "IM_backend/internal/application/ports/cache/friend"
	messagecache "IM_backend/internal/application/ports/cache/message"
	roomcache "IM_backend/internal/application/ports/cache/room"
	idport "IM_backend/internal/application/ports/id"
	"IM_backend/internal/application/ports/messaging"
	realtimeport "IM_backend/internal/application/ports/realtime"
	"IM_backend/internal/application/ports/transaction"
	roomapp "IM_backend/internal/application/room"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfile "IM_backend/internal/domain/file"
	domainfriend "IM_backend/internal/domain/friend"
	domainmessage "IM_backend/internal/domain/message"
	domainroom "IM_backend/internal/domain/room"
)

type MessageApplication struct {
	messageCache                 messagecache.MessageCache
	roomMemberCache              roomcache.RoomMemberCache
	roomCache                    roomcache.RoomCache
	roomApplication              *roomapp.RoomApplication
	roomUnreadSnapshotCache      roomunreadcache.RoomUnreadSnapshotCache
	messageRepository            domainmessage.MessageRepository
	txManager                    transaction.Manager
	userConversationRepository   domainconversation.UserConversationRepository
	conversationRepository       domainconversation.ConversationRepository
	conversationApplication      *conversationapp.ConversationApplication
	friendRepository             domainfriend.FriendRepository
	friendRelationCache          friendcache.RelationCache
	fileRepository               domainfile.FileRepository
	fileCache                    filecache.FileCache
	messageStickerRepository     domainmessage.MessageStickerRepository
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository
	messageProducer              messaging.Publisher
	userDelivery                 realtimeport.UserDelivery
	roomMemberRepository         domainroom.RoomMemberRepository
	sf                           singleflight.Group
	idGenerator                  idport.Generator
	config                       configs.Config
}

func (ma *MessageApplication) SetUserDelivery(delivery realtimeport.UserDelivery) {
	if ma == nil {
		return
	}
	ma.userDelivery = delivery
}

func (ma *MessageApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	ma.conversationApplication = application
}

func NewMessageApplication(
	messageCache messagecache.MessageCache,
	roomMemberCache roomcache.RoomMemberCache,
	roomCache roomcache.RoomCache,
	txManager transaction.Manager,
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	friendRepository domainfriend.FriendRepository,
	friendRelationCache friendcache.RelationCache,
	fileRepository domainfile.FileRepository,
	fileCache filecache.FileCache,
	messageStickerRepository domainmessage.MessageStickerRepository,
	messageRepository domainmessage.MessageRepository,
	roomMemberRepository domainroom.RoomMemberRepository,
	roomApplication *roomapp.RoomApplication,
	idGenerator idport.Generator,
	config configs.Config,
	roomUnreadSnapshotCache roomunreadcache.RoomUnreadSnapshotCache,
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository,
	messageProducer messaging.Publisher,
) *MessageApplication {
	app := &MessageApplication{
		messageCache:                 messageCache,
		roomMemberCache:              roomMemberCache,
		roomCache:                    roomCache,
		roomUnreadSnapshotCache:      roomUnreadSnapshotCache,
		txManager:                    txManager,
		messageRepository:            messageRepository,
		conversationRepository:       conversationRepository,
		userConversationRepository:   userConversationRepository,
		roomApplication:              roomApplication,
		roomMemberRepository:         roomMemberRepository,
		friendRepository:             friendRepository,
		friendRelationCache:          friendRelationCache,
		fileRepository:               fileRepository,
		fileCache:                    fileCache,
		messageStickerRepository:     messageStickerRepository,
		idGenerator:                  idGenerator,
		config:                       config,
		messageAttachmentsRepository: messageAttachmentsRepository,
		messageProducer:              messageProducer,
	}
	return app
}

func (ma *MessageApplication) HandleSendMessage(ctx context.Context, conversationID string, dto messaging.SendMessageDTO) (string, error) {
	if dto.ClientMessageID == "" {
		return "", errors.New("clientMsgId 不能为空")
	}
	info, err := ma.getConversationInfo(ctx, conversationID)
	if err != nil {
		return "", err
	}
	if err := ma.authorizeSend(ctx, info, dto.SenderID); err != nil {
		return "", err
	}
	dto.ConversationID = conversationID
	dto.ConversationType = info.ConversationType
	eventUUID := uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte("im.messages\x00"+conversationID+"\x00"+dto.SenderID+"\x00"+dto.ClientMessageID),
	)
	eventID := hex.EncodeToString(eventUUID[:])
	payload, err := json.Marshal(dto)
	if err != nil {
		return "", err
	}
	err = ma.messageProducer.Publish(ctx, messaging.IntegrationEvent{
		EventID:      eventID,
		Name:         messaging.TypeMessage,
		PartitionKey: conversationID,
		Payload:      payload,
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func (ma *MessageApplication) getConversationInfo(ctx context.Context, conversationID string) (*conversationcache.ConversationInfo, error) {
	if ma.conversationApplication == nil {
		return nil, errors.New("会话应用服务未配置")
	}
	resultCh := ma.sf.DoChan("conversation_info:"+conversationID, func() (any, error) {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ma.config.Message.MemberQueryTimeoutSeconds)*time.Second)
		defer cancel()
		return ma.conversationApplication.GetConversationInfo(queryCtx, conversationID)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		info, ok := result.Val.(*conversationcache.ConversationInfo)
		if !ok || info == nil {
			return nil, errors.New("会话信息无效")
		}
		return info, nil
	}
}

func (ma *MessageApplication) authorizeSend(ctx context.Context, info *conversationcache.ConversationInfo, senderID string) error {
	if info == nil || senderID == "" {
		return ErrForbidden
	}
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.RoomChat:
		if info.RoomID == "" {
			return ErrForbidden
		}
		return ma.isRoomConvMember(ctx, senderID, info.RoomID)
	case domainconversation.PrivateChat:
		var peerID string
		switch senderID {
		case info.UserID1:
			peerID = info.UserID2
		case info.UserID2:
			peerID = info.UserID1
		default:
			return ErrForbidden
		}
		return ma.isFriendPair(ctx, senderID, peerID)
	default:
		return ErrUnknownConversationType
	}
}

func (ma *MessageApplication) isFriendPair(ctx context.Context, userID, friendUserID string) error {
	if userID == "" || friendUserID == "" {
		return ErrNotFriends
	}
	if ma.friendRepository == nil {
		return errors.New("好友关系仓储未配置")
	}
	first, second := userID, friendUserID
	if second < first {
		first, second = second, first
	}
	resultCh := ma.sf.DoChan("friend_relation:"+first+":"+second, func() (any, error) {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ma.config.Message.MemberQueryTimeoutSeconds)*time.Second)
		defer cancel()
		pairs := []domainfriend.RelationPair{
			{UserID: first, FriendUserID: second},
			{UserID: second, FriendUserID: first},
		}
		if ma.friendRelationCache != nil {
			firstRelation, firstHit, firstErr := ma.friendRelationCache.Get(queryCtx, pairs[0].UserID, pairs[0].FriendUserID)
			secondRelation, secondHit, secondErr := ma.friendRelationCache.Get(queryCtx, pairs[1].UserID, pairs[1].FriendUserID)
			if firstErr == nil && secondErr == nil && firstHit && secondHit {
				if isActiveFriendRelation(firstRelation) && isActiveFriendRelation(secondRelation) {
					return nil, nil
				}
				return nil, ErrNotFriends
			}
		}

		relations, err := ma.friendRepository.ListFriendsByUserPairs(queryCtx, pairs)
		if err != nil {
			return nil, err
		}
		byPair := make(map[domainfriend.RelationPair]*domainfriend.Friend, len(relations))
		for _, relation := range relations {
			if relation != nil {
				byPair[domainfriend.RelationPair{UserID: relation.UserID, FriendUserID: relation.FriendUserID}] = relation
			}
		}
		firstFriend, secondFriend := byPair[pairs[0]], byPair[pairs[1]]
		firstCache := friendCacheRelation(firstFriend)
		secondCache := friendCacheRelation(secondFriend)
		areFriends := isActiveFriendRelation(firstCache) && isActiveFriendRelation(secondCache)
		if ma.friendRelationCache != nil {
			ttlSeconds := ma.config.Cache.FriendRelation.NegativeTTLSeconds
			if areFriends {
				ttlSeconds = ma.config.Cache.FriendRelation.TTLSeconds
			}
			if ttlSeconds > 0 {
				ttl := time.Duration(ttlSeconds) * time.Second
				if cacheErr := ma.friendRelationCache.Set(queryCtx, pairs[0].UserID, pairs[0].FriendUserID, firstCache, ttl); cacheErr != nil {
					log.Printf("回填好友关系缓存失败：user=%s friend=%s error=%v", pairs[0].UserID, pairs[0].FriendUserID, cacheErr)
				}
				if cacheErr := ma.friendRelationCache.Set(queryCtx, pairs[1].UserID, pairs[1].FriendUserID, secondCache, ttl); cacheErr != nil {
					log.Printf("回填好友关系缓存失败：user=%s friend=%s error=%v", pairs[1].UserID, pairs[1].FriendUserID, cacheErr)
				}
			}
		}
		if !areFriends {
			return nil, ErrNotFriends
		}
		return nil, nil
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		return result.Err
	}
}

func friendCacheRelation(relation *domainfriend.Friend) *friendcache.Relation {
	if relation == nil {
		return &friendcache.Relation{}
	}
	return &friendcache.Relation{Exists: true, Status: int(relation.Status), Remarks: relation.Remarks}
}

func isActiveFriendRelation(relation *friendcache.Relation) bool {
	return relation != nil && relation.Exists && relation.Status == int(domainfriend.StatusFriend)
}

// existingMessageResult 将数据库中的已有消息转换成幂等请求的返回值。
func (ma *MessageApplication) existingMessageResult(
	ctx context.Context,
	dto messaging.SendMessageDTO,
	existing *domainmessage.Message,
) *MessageAckDTO {
	if existing == nil {
		return nil
	}

	attachmentID := ""
	if ma.messageAttachmentsRepository != nil {
		attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{existing.MessageID}, false)
		if err == nil {
			if attachment := attachments[existing.MessageID]; attachment != nil {
				attachmentID = attachment.AttachmentID
			}
		}
	}

	return &MessageAckDTO{
		ClientMessageID: dto.ClientMessageID,
		ConversationID:  existing.ConversationID,
		MessageID:       existing.MessageID,
		Seq:             existing.Seq,
		AttachmentID:    attachmentID,
		SendTime:        existing.SendTime,
		Status:          string(messaging.AckStatusSent),
	}
}

func (ma *MessageApplication) isRoomConvMember(ctx context.Context, userID, roomID string) error {
	if ma.roomMemberRepository == nil {
		return errors.New("房间成员仓储未配置")
	}
	key := "room_member:" + roomID + ":" + userID
	resultCh := ma.sf.DoChan(key, func() (any, error) {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ma.config.Message.MemberQueryTimeoutSeconds)*time.Second)
		defer cancel()
		var state *roomcache.MemberState
		var hit bool
		if ma.roomMemberCache != nil {
			var err error
			state, hit, err = ma.roomMemberCache.GetMember(queryCtx, roomID, userID)
			if err != nil {
				hit = false
			}
		}
		if !hit {
			member, err := ma.roomMemberRepository.FindRoomMemberByUserIDAndRoomID(userID, roomID)
			if err != nil && !errors.Is(err, domainroom.ErrMemberNotFound) {
				return nil, err
			}
			if member == nil || errors.Is(err, domainroom.ErrMemberNotFound) {
				if ma.roomMemberCache != nil {
					_ = ma.roomMemberCache.SetMemberNotFound(queryCtx, roomID, userID)
				}
				return nil, ErrNotRoomMember
			}
			state = &roomcache.MemberState{Status: member.Status, Role: member.Role, MuteUntil: member.MuteUntil, Version: member.Version}
			if ma.roomMemberCache != nil {
				_, _ = ma.roomMemberCache.SetMemberIfVersionGreater(queryCtx, roomID, userID, state)
			}
		}
		switch {
		case state == nil:
			return nil, ErrNotRoomMember
		case state.Status == domainroom.Active:
			return nil, nil
		case state.Status == domainroom.Muted && state.MuteUntil != nil && *state.MuteUntil <= time.Now().UnixMilli():
			return nil, nil
		case state.Status == domainroom.Kicked || state.Status == domainroom.Muted:
			return nil, ErrForbidden
		default:
			return nil, ErrNotRoomMember
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		return result.Err
	}
}

func (ma *MessageApplication) ListActiveRoomIDsByUserID(userID string) ([]string, error) {
	if ma.roomMemberRepository == nil {
		return nil, errors.New("房间成员仓储未配置")
	}
	return ma.roomMemberRepository.ListActiveRoomIDsByUserID(userID)
}

func (ma *MessageApplication) checkConversationAccess(ctx context.Context, info *conversationcache.ConversationInfo, userID string) error {
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.PrivateChat:
		if info.UserID1 != userID && info.UserID2 != userID {
			return ErrForbidden
		}
		return nil
	case domainconversation.RoomChat:
		err := ma.isRoomConvMember(ctx, userID, info.RoomID)
		if errors.Is(err, domainroom.ErrMemberNotFound) || errors.Is(err, ErrNotRoomMember) || errors.Is(err, ErrForbidden) {
			return ErrForbidden
		}
		return err
	default:
		return ErrUnknownConversationType
	}
}

func (ma *MessageApplication) fillAttachmentIDs(ctx context.Context, messages []MessageDTO) error {
	if len(messages) == 0 || ma.messageAttachmentsRepository == nil {
		return nil
	}

	messageIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		if message.MessageID != "" {
			messageIDs = append(messageIDs, message.MessageID)
		}
	}
	attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, messageIDs, true)
	if err != nil {
		return err
	}

	for i := range messages {
		if attachment := attachments[messages[i].MessageID]; attachment != nil {
			messages[i].AttachmentID = attachment.AttachmentID
		}
	}
	return nil
}

func messageEventsToAppDTO(events []messaging.MessageEvent, conversationID string) []MessageDTO {
	if len(events) == 0 {
		return []MessageDTO{}
	}

	result := make([]MessageDTO, 0, len(events))
	for _, event := range events {
		if event.MessageID == "" || event.Seq <= 0 {
			continue
		}

		resolvedID := event.ConversationID
		if resolvedID == "" {
			resolvedID = conversationID
		}

		item := MessageDTO{
			SenderID:       event.SenderID,
			ConversationID: resolvedID,
			MessageID:      event.MessageID,
			Seq:            event.Seq,
			Type:           event.MessageType,
			Content:        event.Content,
			SendTime:       event.SendTime,
			Status:         event.Status,
			AttachmentID:   event.AttachmentID,
			Width:          event.Width,
			Height:         event.Height,
			DurationMs:     event.DurationMs,
			StickerID:      event.StickerID,
			PackID:         event.PackID,
		}
		if item.Status == 0 {
			item.Status = int8(domainmessage.Normal)
		}
		if event.ClientMsgID != "" {
			clientMsgID := event.ClientMsgID
			item.ClientMessageID = &clientMsgID
		}
		result = append(result, item)
	}

	return result
}

// messageAppDTOsToEvents 将应用层消息 DTO 转换为内部事件，用于回填房间近期消息缓存。
// 内部事件可以携带后端投递所需的路由信息，但这些字段会在 WebSocket 编码时被过滤。
func messageAppDTOsToEvents(messages []MessageDTO, conversationID, roomID string) []messaging.MessageEvent {
	if len(messages) == 0 {
		return []messaging.MessageEvent{}
	}

	events := make([]messaging.MessageEvent, 0, len(messages))
	for _, message := range messages {
		if message.MessageID == "" || message.Seq <= 0 {
			continue
		}

		resolvedID := message.ConversationID
		if resolvedID == "" {
			resolvedID = conversationID
		}

		clientMsgID := ""
		if message.ClientMessageID != nil {
			clientMsgID = *message.ClientMessageID
		}

		events = append(events, messaging.MessageEvent{
			MessageID:      message.MessageID,
			ConversationID: resolvedID,
			SenderID:       message.SenderID,
			RecvID:         roomID,
			Seq:            message.Seq,
			MessageType:    message.Type,
			Content:        message.Content,
			SendTime:       message.SendTime,
			ClientMsgID:    clientMsgID,
			Status:         message.Status,
			AttachmentID:   message.AttachmentID,
			Width:          message.Width,
			Height:         message.Height,
			DurationMs:     message.DurationMs,
			StickerID:      message.StickerID,
			PackID:         message.PackID,
		})
	}

	return events
}
