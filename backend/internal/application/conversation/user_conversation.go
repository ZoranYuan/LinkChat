package conversation

import (
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	friendrepo "IM_backend/internal/application/ports/persistence/repository/friend"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	userrepo "IM_backend/internal/application/ports/persistence/repository/user"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	friendentity "IM_backend/internal/domain/friend/entity"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"context"
	"sort"
)

type roomInfoService interface {
	GetRoomInfo(ctx context.Context, roomID string) (*roomentity.Room, error)
}

type UserConversationApplication struct {
	friendRepository           friendrepo.FriendRepository
	userRepository             userrepo.UserRepository
	roomRepository             roomrepo.RoomRepository
	roomApplication            roomInfoService
	userConversationRepository conversationrepo.UserConversationRepository
	conversationRepository     conversationrepo.ConversationRepository
	messageRepository          messagerepo.MessageRepository
}

func (uc *UserConversationApplication) SetRoomApplication(application roomInfoService) {
	if uc != nil {
		uc.roomApplication = application
	}
}

func NewUserConversationApplication(
	userConversationRepository conversationrepo.UserConversationRepository,
	conversationRepository conversationrepo.ConversationRepository,
	messageRepository messagerepo.MessageRepository,
	friendRepository friendrepo.FriendRepository,
	userRepository userrepo.UserRepository,
	roomRepository roomrepo.RoomRepository,
) *UserConversationApplication {
	return &UserConversationApplication{
		friendRepository:           friendRepository,
		userRepository:             userRepository,
		roomRepository:             roomRepository,
		userConversationRepository: userConversationRepository,
		conversationRepository:     conversationRepository,
		messageRepository:          messageRepository,
	}
}

func (uc *UserConversationApplication) GetUserConversationsByUserID(ctx context.Context, userId string) ([]ConversationItemDTO, error) {
	if userId == "" {
		return nil, ErrEmptyUserId
	}

	userConversations, err := uc.userConversationRepository.ListByUser(ctx, userId)
	if err != nil {
		return nil, err
	}
	if len(userConversations) == 0 {
		return []ConversationItemDTO{}, nil
	}

	conversationIDs := make([]string, 0, len(userConversations))
	for _, ucItem := range userConversations {
		if ucItem == nil || ucItem.ConversationId == "" {
			continue
		}
		conversationIDs = append(conversationIDs, ucItem.ConversationId)
	}

	conversations, err := uc.conversationRepository.ListByIDs(ctx, conversationIDs)
	if err != nil {
		return nil, err
	}

	conversationByID := make(map[string]*conversationentity.Conversation, len(conversations))
	privatePeerIDs := make([]string, 0, len(conversations))
	privateSeen := make(map[string]struct{}, len(conversations))
	roomIDs := make([]string, 0, len(conversations))
	roomSeen := make(map[string]struct{}, len(conversations))

	for _, conv := range conversations {
		if conv == nil {
			continue
		}
		conversationByID[conv.ConversationId] = conv

		switch conv.Convtype {
		case conversationvo.PrivateChat:
			var peerID string
			if conv.UserId1 == userId {
				peerID = conv.UserId2
			} else {
				peerID = conv.UserId1
			}

			if peerID != "" {
				if _, ok := privateSeen[peerID]; !ok {
					privateSeen[peerID] = struct{}{}
					privatePeerIDs = append(privatePeerIDs, peerID)
				}
			}
		case conversationvo.RoomChat:
			if conv.RoomId == "" {
				continue
			}
			if _, ok := roomSeen[conv.RoomId]; ok {
				continue
			}
			roomSeen[conv.RoomId] = struct{}{}
			roomIDs = append(roomIDs, conv.RoomId)
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
		messageByConversationID[msg.ConversationId] = LatestMessageDTO{
			MessageID:      msg.MessageId,
			ConversationID: msg.ConversationId,
			SenderID:       msg.SenderId,
			Seq:            msg.Seq,
			Type:           msg.Type,
			Content:        msg.Content,
			SendTime:       msg.SendTime,
		}
	}

	userByID := make(map[string]PeerUserDTO, len(privatePeerIDs))
	if len(privatePeerIDs) > 0 {
		users, err := uc.userRepository.FindByUserIDs(privatePeerIDs)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			userByID[user.UserId] = PeerUserDTO{
				UserID:    user.UserId,
				Username:  user.UserName,
				Nickname:  user.NickName,
				AvatarURL: user.Avatar,
			}
		}
	}

	roomByID := make(map[string]RoomDTO, len(roomIDs))
	for _, roomID := range roomIDs {
		var room *roomentity.Room
		var err error
		if uc.roomApplication != nil {
			room, err = uc.roomApplication.GetRoomInfo(ctx, roomID)
			if err == nil && room != nil && room.Status != roomvo.Normal {
				err = roomentity.ErrRoomNotFound
			}
		} else {
			room, err = uc.roomRepository.FindActiveRoom(roomID, int(roomvo.Normal))
		}
		if err != nil || room == nil {
			continue
		}
		roomByID[roomID] = RoomDTO{
			RoomID:      room.RoomId,
			RoomName:    room.RoomName,
			AvatarURL:   room.Avatar,
			Description: room.Description,
			MemberCount: room.MemberCount,
		}
	}

	relations, err := uc.friendRepository.FindRelations(ctx, userId, privatePeerIDs)
	if err != nil {
		return nil, err
	}

	peerFriendRelations := make(map[string]*friendentity.Friend)

	for _, relation := range relations {
		peerFriendRelations[relation.FriendUserId] = relation
	}

	items := make([]ConversationItemDTO, 0, len(userConversations))
	for _, userConv := range userConversations {
		if userConv == nil {
			continue
		}
		conv := conversationByID[userConv.ConversationId]
		if conv == nil {
			continue
		}

		item := ConversationItemDTO{
			ConversationID:   userConv.ConversationId,
			ConversationType: int8(conv.Convtype),
			IsMuted:          userConv.IsMuted,
			Unread:           unreadCount(userConv.LastReadSeq, conv.LatestSeq),
			LatestSeq:        conv.LatestSeq,
		}

		if latestMessage, ok := messageByConversationID[userConv.ConversationId]; ok {
			copy := latestMessage
			item.LastMessage = &copy
		}

		switch conv.Convtype {
		case conversationvo.PrivateChat:
			peerID := conv.UserId1
			if peerID == userId {
				peerID = conv.UserId2
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

		case conversationvo.RoomChat:
			item.TargetID = conv.RoomId
			if room, ok := roomByID[conv.RoomId]; ok {
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
