package infraconversation

import (
	domainconversation "IM_backend/internal/domain/conversation"
)

func toUserConversationDomain(model *UserConversationModel) *domainconversation.UserConversation {
	if model == nil {
		return nil
	}

	return &domainconversation.UserConversation{
		UserID:                model.UserID,
		ConversationID:        model.ConversationID,
		LastReadSeq:           model.LastReadSeq,
		SummaryUnreadBeginSeq: model.SummaryUnreadBeginSeq,
		SummaryUnreadEndSeq:   model.SummaryUnreadEndSeq,
		IsMuted:               model.IsMuted,
	}
}

func toUserConversationModel(entity *domainconversation.UserConversation) *UserConversationModel {
	if entity == nil {
		return nil
	}

	return &UserConversationModel{
		UserID:                entity.UserID,
		ConversationID:        entity.ConversationID,
		LastReadSeq:           entity.LastReadSeq,
		SummaryUnreadBeginSeq: entity.SummaryUnreadBeginSeq,
		SummaryUnreadEndSeq:   entity.SummaryUnreadEndSeq,
		IsMuted:               entity.IsMuted,
	}
}

func toConversationDomain(model *ConversationModel) *domainconversation.Conversation {
	if model == nil {
		return nil
	}

	return &domainconversation.Conversation{
		ConversationID:  model.ConversationID,
		ConvType:        domainconversation.ConvType(model.ConvType),
		UserID1:         model.UserID1,
		UserID2:         model.UserID2,
		RoomID:          model.RoomID,
		LatestSeq:       model.LatestSeq,
		LatestMessageID: model.LatestMessageID,
	}
}

func toConversationModel(entity *domainconversation.Conversation) *ConversationModel {
	if entity == nil {
		return nil
	}

	return &ConversationModel{
		ConversationID:  entity.ConversationID,
		ConvType:        int8(entity.ConvType),
		UserID1:         entity.UserID1,
		UserID2:         entity.UserID2,
		RoomID:          entity.RoomID,
		LatestSeq:       entity.LatestSeq,
		LatestMessageID: entity.LatestMessageID,
	}
}
