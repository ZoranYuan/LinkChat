package message

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	conversationapp "IM_backend/core/application/conversation"
	groupunreadcache "IM_backend/core/application/ports/cache/agent"
	conversationcache "IM_backend/core/application/ports/cache/conversation"
	filecache "IM_backend/core/application/ports/cache/file"
	friendcache "IM_backend/core/application/ports/cache/friend"
	messagecacheport "IM_backend/core/application/ports/cache/message"
	groupcache "IM_backend/core/application/ports/cache/group"
	eventport "IM_backend/core/application/ports/events"
	inboxport "IM_backend/core/application/ports/inbox"
	realtimeport "IM_backend/core/application/ports/realtime"
	"IM_backend/core/application/ports/transaction"
	groupapp "IM_backend/core/application/group"
	domainconversation "IM_backend/core/domain/conversation"
	domainfile "IM_backend/core/domain/file"
	domainfriend "IM_backend/core/domain/friend"
	domainmessage "IM_backend/core/domain/message"
	domaingroup "IM_backend/core/domain/group"
	messagecache "IM_backend/core/infra/cache/message"
)

type MessageApplication struct {
	messageCache                 messagecacheport.MessageCache
	inboxRepository              inboxport.InboxRepository
	groupMemberCache              groupcache.GroupMemberCache
	groupCache                    groupcache.GroupCache
	groupApplication              *groupapp.GroupApplication
	groupUnreadSnapshotCache      groupunreadcache.GroupUnreadSnapshotCache
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
	messageProducer              eventport.Publisher
	userDelivery                 realtimeport.UserDelivery
	groupMemberRepository         domaingroup.GroupMemberRepository
	sf                           singleflight.Group
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
	messageCache messagecacheport.MessageCache,
	groupMemberCache groupcache.GroupMemberCache,
	groupCache groupcache.GroupCache,
	txManager transaction.Manager,
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	friendRepository domainfriend.FriendRepository,
	friendRelationCache friendcache.RelationCache,
	fileRepository domainfile.FileRepository,
	fileCache filecache.FileCache,
	messageStickerRepository domainmessage.MessageStickerRepository,
	messageRepository domainmessage.MessageRepository,
	groupMemberRepository domaingroup.GroupMemberRepository,
	groupApplication *groupapp.GroupApplication,
	config configs.Config,
	groupUnreadSnapshotCache groupunreadcache.GroupUnreadSnapshotCache,
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository,
	messageProducer eventport.Publisher,
	inboxRepository inboxport.InboxRepository,
) *MessageApplication {
	app := &MessageApplication{
		messageCache:                 messageCache,
		inboxRepository:              inboxRepository,
		groupMemberCache:              groupMemberCache,
		groupCache:                    groupCache,
		groupUnreadSnapshotCache:      groupUnreadSnapshotCache,
		txManager:                    txManager,
		messageRepository:            messageRepository,
		conversationRepository:       conversationRepository,
		userConversationRepository:   userConversationRepository,
		groupApplication:              groupApplication,
		groupMemberRepository:         groupMemberRepository,
		friendRepository:             friendRepository,
		friendRelationCache:          friendRelationCache,
		fileRepository:               fileRepository,
		fileCache:                    fileCache,
		messageStickerRepository:     messageStickerRepository,
		config:                       config,
		messageAttachmentsRepository: messageAttachmentsRepository,
		messageProducer:              messageProducer,
	}
	return app
}

func (ma *MessageApplication) HandleSendMessage(ctx context.Context, conversationID string, dto SendMessageDTO) (messagecache.DedupEntry, error) {
	if dto.ClientMessageID == "" {
		return messagecache.DedupEntry{}, errors.New("clientMsgId 不能为空")
	}
	if maxLen := ma.config.Message.MaxIdentifierLength; maxLen > 0 && len(dto.ClientMessageID) > maxLen {
		return messagecache.DedupEntry{}, fmt.Errorf("clientMsgId 长度不能超过 %d", maxLen)
	}
	conversation, err := ma.getConversationInfo(ctx, conversationID)

	if err != nil {
		return messagecache.DedupEntry{}, err
	}

	if err := ma.authorizeSend(ctx, conversation, dto.SenderID); err != nil {
		return messagecache.DedupEntry{}, err
	}

	payload, err := json.Marshal(dto)
	if err != nil {
		return messagecache.DedupEntry{}, err
	}
	sum := sha256.Sum256(payload)
	requestHash := hex.EncodeToString(sum[:])
	entry, isCacheMiss, err := ma.messageCache.GetOrCreateDedupEntry(ctx, dto.SenderID, dto.ClientMessageID, messagecache.DedupEntry{
		RequestHash: requestHash, SendTime: time.Now().UnixMilli(),
	}, time.Duration(ma.config.Cache.MessageDedup.ProcessingTTLSeconds)*time.Second)
	if err != nil {
		return messagecache.DedupEntry{}, err
	}

	if !isCacheMiss {
		return entry, nil
	}

	if err := ma.messageProducer.Publish(ctx, ma.config.Kafka.Topics.MessageSendPersist, eventport.KafkaEventMessageSend, dto.ClientMessageID, conversationID, payload); err != nil {
		return entry, fmt.Errorf("%w: %v", ErrMessagePublishUncertain, err)
	}

	return entry, nil
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
	case domainconversation.GroupChat:
		if info.GroupID == "" {
			return ErrForbidden
		}
		return ma.isGroupConvMember(ctx, senderID, info.GroupID)
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

func (ma *MessageApplication) isGroupConvMember(ctx context.Context, userID, groupID string) error {
	if ma.groupMemberRepository == nil {
		return errors.New("群聊成员仓储未配置")
	}
	key := "group_member:" + groupID + ":" + userID
	resultCh := ma.sf.DoChan(key, func() (any, error) {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ma.config.Message.MemberQueryTimeoutSeconds)*time.Second)
		defer cancel()
		var state *groupcache.MemberState
		var hit bool
		if ma.groupMemberCache != nil {
			var err error
			state, hit, err = ma.groupMemberCache.GetMember(queryCtx, groupID, userID)
			if err != nil {
				hit = false
			}
		}
		if !hit {
			member, err := ma.groupMemberRepository.FindGroupMemberByUserIDAndGroupID(userID, groupID)
			if err != nil && !errors.Is(err, domaingroup.ErrMemberNotFound) {
				return nil, err
			}
			if member == nil || errors.Is(err, domaingroup.ErrMemberNotFound) {
				if ma.groupMemberCache != nil {
					_ = ma.groupMemberCache.SetMemberNotFound(queryCtx, groupID, userID)
				}
				return nil, ErrNotGroupMember
			}
			state = &groupcache.MemberState{Status: member.Status, Role: member.Role, MuteUntil: member.MuteUntil, Version: member.Version}
			if ma.groupMemberCache != nil {
				_, _ = ma.groupMemberCache.SetMemberIfVersionGreater(queryCtx, groupID, userID, state)
			}
		}
		switch {
		case state == nil:
			return nil, ErrNotGroupMember
		case state.Status == domaingroup.Active:
			return nil, nil
		case state.Status == domaingroup.Muted && state.MuteUntil != nil && *state.MuteUntil <= time.Now().UnixMilli():
			return nil, nil
		case state.Status == domaingroup.Kicked || state.Status == domaingroup.Muted:
			return nil, ErrForbidden
		default:
			return nil, ErrNotGroupMember
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		return result.Err
	}
}

func (ma *MessageApplication) ListActiveGroupIDsByUserID(userID string) ([]string, error) {
	if ma.groupMemberRepository == nil {
		return nil, errors.New("群聊成员仓储未配置")
	}
	return ma.groupMemberRepository.ListActiveGroupIDsByUserID(userID)
}

func (ma *MessageApplication) checkConversationAccess(ctx context.Context, info *conversationcache.ConversationInfo, userID string) error {
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.PrivateChat:
		if info.UserID1 != userID && info.UserID2 != userID {
			return ErrForbidden
		}
		return nil
	case domainconversation.GroupChat:
		err := ma.isGroupConvMember(ctx, userID, info.GroupID)
		if errors.Is(err, domaingroup.ErrMemberNotFound) || errors.Is(err, ErrNotGroupMember) || errors.Is(err, ErrForbidden) {
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

func messageEventsToAppDTO(events []eventport.MessagePushEvent, conversationID string) []MessageDTO {
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

// messageAppDTOsToEvents 将应用层消息 DTO 转换为内部事件，用于回填群聊近期消息缓存。
// 内部事件可以携带后端投递所需的路由信息，但这些字段会在 WebSocket 编码时被过滤。
func messageAppDTOsToEvents(messages []MessageDTO, conversationID, groupID string) []eventport.MessagePushEvent {
	if len(messages) == 0 {
		return []eventport.MessagePushEvent{}
	}

	events := make([]eventport.MessagePushEvent, 0, len(messages))
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

		events = append(events, eventport.MessagePushEvent{
			MessageID:      message.MessageID,
			ConversationID: resolvedID,
			SenderID:       message.SenderID,
			RecvID:         groupID,
			Seq:            message.Seq,
			MessageType:    message.Type,
			Content:        message.Content,
			SendTime:       message.SendTime,
			ClientMsgID:    clientMsgID,
			Status:         message.Status,
			AttachmentID:   message.AttachmentID,
			StickerID:      message.StickerID,
			PackID:         message.PackID,
		})
	}

	return events
}
