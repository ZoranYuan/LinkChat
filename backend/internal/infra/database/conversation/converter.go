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

	conversation := &domainconversation.Conversation{
		ConversationID: model.ConversationID,
		ConvType:       domainconversation.ConvType(model.ConvType),
		LatestSeq:      model.LatestSeq,
	}
	if model.UserID1 != nil {
		conversation.UserID1 = *model.UserID1
	}
	if model.UserID2 != nil {
		conversation.UserID2 = *model.UserID2
	}
	if model.RoomID != nil {
		conversation.RoomID = *model.RoomID
	}
	if model.LatestMessageID != nil {
		conversation.LatestMessageID = *model.LatestMessageID
	}
	return conversation
}

func toConversationModel(entity *domainconversation.Conversation) *ConversationModel {
	if entity == nil {
		return nil
	}

	model := &ConversationModel{
		ConversationID: entity.ConversationID,
		ConvType:       int8(entity.ConvType),
		LatestSeq:      entity.LatestSeq,
	}
	if entity.ConvType == domainconversation.RoomChat {
		model.RoomID = &entity.RoomID
	} else {
		model.UserID1 = &entity.UserID1
		model.UserID2 = &entity.UserID2
	}
	if entity.LatestMessageID != "" {
		model.LatestMessageID = &entity.LatestMessageID
	}
	return model
}
