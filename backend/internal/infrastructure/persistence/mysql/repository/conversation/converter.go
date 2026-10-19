package conversation

import (
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	"IM_backend/internal/infrastructure/persistence/mysql/model"
)

func toUserConversationDomain(model *model.UserConversation) *conversationentity.UserConversation {
	if model == nil {
		return nil
	}

	return &conversationentity.UserConversation{
		UserId:                model.UserId,
		ConversationId:        model.ConversationId,
		LastReadSeq:           model.LastReadSeq,
		SummaryUnreadBeginSeq: model.SummaryUnreadBeginSeq,
		SummaryUnreadEndSeq:   model.SummaryUnreadEndSeq,
		IsMuted:               model.IsMuted,
	}
}

func toUserConversationModel(entity *conversationentity.UserConversation) *model.UserConversation {
	if entity == nil {
		return nil
	}

	return &model.UserConversation{
		UserId:                entity.UserId,
		ConversationId:        entity.ConversationId,
		LastReadSeq:           entity.LastReadSeq,
		SummaryUnreadBeginSeq: entity.SummaryUnreadBeginSeq,
		SummaryUnreadEndSeq:   entity.SummaryUnreadEndSeq,
		IsMuted:               entity.IsMuted,
	}
}

func toConversationDomain(model *model.Conversation) *conversationentity.Conversation {
	if model == nil {
		return nil
	}

	return &conversationentity.Conversation{
		ConversationId:  model.ConversationId,
		Convtype:        conversationvo.ConvType(model.Convtype),
		UserId1:         model.UserId1,
		UserId2:         model.UserId2,
		RoomId:          model.RoomId,
		LatestSeq:       model.LatestSeq,
		LatestMessageId: model.LatestMessageId,
	}
}

func toConversationModel(entity *conversationentity.Conversation) *model.Conversation {
	if entity == nil {
		return nil
	}

	return &model.Conversation{
		ConversationId:  entity.ConversationId,
		Convtype:        int8(entity.Convtype),
		UserId1:         entity.UserId1,
		UserId2:         entity.UserId2,
		RoomId:          entity.RoomId,
		LatestSeq:       entity.LatestSeq,
		LatestMessageId: entity.LatestMessageId,
	}
}
