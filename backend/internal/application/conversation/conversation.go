package conversation

import (
	"context"
	"log"
	"sort"
	"time"

	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	usercache "IM_backend/internal/application/ports/cache/user"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfriend "IM_backend/internal/domain/friend"
	domainmessage "IM_backend/internal/domain/message"
	domainroom "IM_backend/internal/domain/room"
	domainuser "IM_backend/internal/domain/user"
)

type ConversationApplication struct {
	repository domainconversation.ConversationRepository
	cache      conversationcache.ConversationCache
	cacheTTL   time.Duration
}

func NewConversationApplication(repository domainconversation.ConversationRepository, cache conversationcache.ConversationCache, ttl time.Duration) *ConversationApplication {
	return &ConversationApplication{repository: repository, cache: cache, cacheTTL: ttl}
}

func (ca *ConversationApplication) FindConversationByRoomID(ctx context.Context, roomID string) (*domainconversation.Conversation, error) {
	conversation, err := ca.repository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if conversation == nil {
		return nil, ErrConversationNotFound
	}
	return conversation, nil
}

func (ca *ConversationApplication) GetConversationInfo(ctx context.Context, id string) (*conversationcache.ConversationInfo, error) {
	if id == "" {
		return nil, ErrEmptyConversationID
	}
	if len(id) > 64 {
		return nil, ErrInvalidConversationID
	}
	info, found, err := ca.cache.GetConversationInfo(ctx, id)
	if err != nil {
		log.Printf("读取会话缓存失败：conversation=%s error=%v", id, err)
	} else if found {
		if info == nil {
			return nil, ErrConversationNotFound
		}
		return info, nil
	}
	record, err := ca.repository.FindConversationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		if err := ca.cache.SetConversationNotFound(ctx, id); err != nil {
			log.Printf("写入会话空缓存失败：conversation=%s error=%v", id, err)
		}
		return nil, ErrConversationNotFound
	}
	if record.ConvType != domainconversation.PrivateChat && record.ConvType != domainconversation.RoomChat {
		return nil, ErrUnknownConversationType
	}
	ca.CacheConversationInfo(ctx, record)
	return conversationcache.FromConversation(record), nil
}

// CacheConversationInfo 只在事务提交后或读取已提交记录时调用，缓存异常不改变业务结果。
func (ca *ConversationApplication) CacheConversationInfo(ctx context.Context, record *domainconversation.Conversation) {
	if err := ca.cache.SetConversationInfo(ctx, conversationcache.FromConversation(record), ca.cacheTTL); err != nil {
		log.Printf("写入会话缓存失败：conversation=%s error=%v", record.ConversationID, err)
	}
}

type roomInfoService interface {
	GetActiveRooms(ctx context.Context, roomIDs []string) (map[string]*domainroom.Room, error)
}

type UserConversationApplication struct {
	friendRepository           domainfriend.FriendRepository
	userRepository             domainuser.UserRepository
	userProfileCache           usercache.ProfileCache
	roomApplication            roomInfoService
	userConversationRepository domainconversation.UserConversationRepository
	conversationRepository     domainconversation.ConversationRepository
	messageRepository          domainmessage.MessageRepository
}

func NewUserConversationApplication(
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	messageRepository domainmessage.MessageRepository,
	friendRepository domainfriend.FriendRepository,
	userRepository domainuser.UserRepository,
	userProfileCache usercache.ProfileCache,
	roomApplication roomInfoService,
) *UserConversationApplication {
	return &UserConversationApplication{
		friendRepository:           friendRepository,
		userRepository:             userRepository,
		userProfileCache:           userProfileCache,
		roomApplication:            roomApplication,
		userConversationRepository: userConversationRepository,
		conversationRepository:     conversationRepository,
		messageRepository:          messageRepository,
	}
}

func (uc *UserConversationApplication) GetUserConversationsByUserID(ctx context.Context, userID string) ([]ConversationItemDTO, error) {
	if userID == "" {
		return nil, ErrEmptyUserID
	}

	userConversations, err := uc.userConversationRepository.ListUserConversationsByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(userConversations) == 0 {
		return []ConversationItemDTO{}, nil
	}

	conversationIDs := make([]string, 0, len(userConversations))
	for _, ucItem := range userConversations {
		if ucItem == nil || ucItem.ConversationID == "" {
			continue
		}
		conversationIDs = append(conversationIDs, ucItem.ConversationID)
	}

	conversations, err := uc.conversationRepository.ListConversationsByIDs(ctx, conversationIDs)
	if err != nil {
		return nil, err
	}

	conversationByID := make(map[string]*domainconversation.Conversation, len(conversations))
	privatePeerIDs := make([]string, 0, len(conversations))
	privateSeen := make(map[string]struct{}, len(conversations))
	roomIDs := make([]string, 0, len(conversations))
	roomSeen := make(map[string]struct{}, len(conversations))

	for _, conv := range conversations {
		if conv == nil {
			continue
		}
		conversationByID[conv.ConversationID] = conv

		switch conv.ConvType {
		case domainconversation.PrivateChat:
			var peerID string
			if conv.UserID1 == userID {
				peerID = conv.UserID2
			} else {
				peerID = conv.UserID1
			}

			if peerID != "" {
				if _, ok := privateSeen[peerID]; !ok {
					privateSeen[peerID] = struct{}{}
					privatePeerIDs = append(privatePeerIDs, peerID)
				}
			}
		case domainconversation.RoomChat:
			if conv.RoomID == "" {
				continue
			}
			if _, ok := roomSeen[conv.RoomID]; ok {
				continue
			}
			roomSeen[conv.RoomID] = struct{}{}
			roomIDs = append(roomIDs, conv.RoomID)
		}
	}

	latestMessages, err := uc.messageRepository.GetLatestMessagesByConversationIDs(ctx, conversationIDs)
	if err != nil {
		return nil, err
	}

	messageByConversationID := make(map[string]LatestMessageDTO, len(latestMessages))
	for _, msg := range latestMessages {
		if msg == nil {
			continue
		}
		messageByConversationID[msg.ConversationID] = LatestMessageDTO{
			MessageID:      msg.MessageID,
			ConversationID: msg.ConversationID,
			SenderID:       msg.SenderID,
			Seq:            msg.Seq,
			Type:           msg.Type,
			Content:        msg.Content,
			SendTime:       msg.SendTime,
		}
	}

	userByID := make(map[string]PeerUserDTO, len(privatePeerIDs))
	if len(privatePeerIDs) > 0 {
		profileByID := make(map[string]*usercache.Profile, len(privatePeerIDs))
		if uc.userProfileCache != nil {
			cached, cacheErr := uc.userProfileCache.GetByUserIDs(ctx, privatePeerIDs)
			if cacheErr != nil {
				log.Printf("批量读取用户资料缓存失败：user_count=%d error=%v", len(privatePeerIDs), cacheErr)
			} else {
				profileByID = cached
			}
		}

		missIDs := make([]string, 0, len(privatePeerIDs))
		for _, id := range privatePeerIDs {
			if profileByID[id] == nil {
				missIDs = append(missIDs, id)
			}
		}
		users, err := uc.userRepository.ListUsersByUserIDs(missIDs)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			profile := &usercache.Profile{
				UserID: user.UserID, Username: user.Username,
				Nickname: user.Nickname, Avatar: user.Avatar,
			}
			profileByID[user.UserID] = profile
			if uc.userProfileCache != nil {
				if err := uc.userProfileCache.SetByUserID(ctx, user.UserID, profile, 10*time.Minute); err != nil {
					log.Printf("回填用户 ID 资料缓存失败：user=%s error=%v", user.UserID, err)
				}
			}
		}
		for id, profile := range profileByID {
			userByID[id] = PeerUserDTO{
				UserID: id, Username: profile.Username,
				Nickname: profile.Nickname, AvatarURL: profile.Avatar,
			}
		}
	}

	roomByID := make(map[string]RoomDTO, len(roomIDs))
	rooms, err := uc.roomApplication.GetActiveRooms(ctx, roomIDs)
	if err != nil {
		log.Printf("批量查询房间资料失败：room_count=%d error=%v", len(roomIDs), err)
	}
	for roomID, room := range rooms {
		roomByID[roomID] = RoomDTO{
			RoomID:      room.RoomID,
			RoomName:    room.RoomName,
			AvatarURL:   room.Avatar,
			Description: room.Description,
			MemberCount: room.MemberCount,
		}
	}

	relations, err := uc.friendRepository.ListFriendsByUserIDAndFriendUserIDs(ctx, userID, privatePeerIDs)
	if err != nil {
		return nil, err
	}

	peerFriendRelations := make(map[string]*domainfriend.Friend)

	for _, relation := range relations {
		peerFriendRelations[relation.FriendUserID] = relation
	}

	items := make([]ConversationItemDTO, 0, len(userConversations))
	for _, userConv := range userConversations {
		if userConv == nil {
			continue
		}
		conv := conversationByID[userConv.ConversationID]
		if conv == nil {
			continue
		}

		item := ConversationItemDTO{
			ConversationID:   userConv.ConversationID,
			ConversationType: int8(conv.ConvType),
			IsMuted:          userConv.IsMuted,
			Unread:           unreadCount(userConv.LastReadSeq, conv.LatestSeq),
			LatestSeq:        conv.LatestSeq,
		}

		if latestMessage, ok := messageByConversationID[userConv.ConversationID]; ok {
			copy := latestMessage
			item.LastMessage = &copy
		}

		switch conv.ConvType {
		case domainconversation.PrivateChat:
			peerID := conv.UserID1
			if peerID == userID {
				peerID = conv.UserID2
			}
			item.TargetID = peerID

			peerUser := userByID[peerID]

			if peer, ok := peerFriendRelations[peerUser.UserID]; !ok {
				continue
			} else {
				peerUser.Remark = peer.Remarks
			}

			item.PeerUser = &peerUser
			item.DisplayName = peerUser.Remark
			if item.DisplayName == "" {
				item.DisplayName = peerUser.Nickname
			}
			if item.DisplayName == "" {
				item.DisplayName = peerUser.Username
			}
			item.AvatarURL = peerUser.AvatarURL
			if item.DisplayName == "" {
				item.DisplayName = "好友 " + peerUser.Username
			}

		case domainconversation.RoomChat:
			item.TargetID = conv.RoomID
			if room, ok := roomByID[conv.RoomID]; ok {
				roomCopy := room
				item.Room = &roomCopy
				item.DisplayName = room.RoomName
				item.AvatarURL = room.AvatarURL
			}
			if item.DisplayName == "" {
				item.DisplayName = "房间 " + item.TargetID
			}
		}

		items = append(items, item)
	}

	// 排序
	sort.SliceStable(items, func(i, j int) bool {
		leftTime := int64(0)
		if items[i].LastMessage != nil {
			leftTime = items[i].LastMessage.SendTime
		}
		rightTime := int64(0)
		if items[j].LastMessage != nil {
			rightTime = items[j].LastMessage.SendTime
		}
		if leftTime != rightTime {
			return leftTime > rightTime
		}
		return items[i].ConversationID > items[j].ConversationID
	})

	return items, nil
}

func unreadCount(lastReadSeq int64, latestSeq int64) int64 {
	if latestSeq <= lastReadSeq {
		return 0
	}
	return latestSeq - lastReadSeq
}
