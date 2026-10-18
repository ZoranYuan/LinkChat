package entity

import conversationvo "IM_backend/internal/domain/conversation/value_object"

type Conversation struct {
	ConversationId  string
	Convtype        conversationvo.ConvType
	UserId1         string
	UserId2         string
	RoomId          string
	LatestMessageId string
	LatestSeq       int64
}

func NewConversation(
	conversationId string,
	senderId string,
	receiverId string,
	convType int,
	seq int64,
	latestMessageId string,
) *Conversation {
	conversation := &Conversation{
		ConversationId:  conversationId,
		Convtype:        conversationvo.ConvType(convType),
		UserId1:         senderId,
		LatestMessageId: latestMessageId,
		LatestSeq:       seq,
	}

	if conversation.Convtype == conversationvo.RoomChat {
		conversation.RoomId = receiverId
	} else {
		conversation.UserId2 = receiverId
	}

	return conversation
}
