package message

import (
	messageapp "IM_backend/internal/application/message"
)

type MessageHistoryRequest struct {
	ConversationID string `form:"conversationId" binding:"required,max=64"`
	Cursor         int64  `form:"cursor"`
	Limit          int    `form:"limit"`
}

type MessageSyncRequest struct {
	ConversationID string `form:"conversationId" binding:"required,max=64"`
	AfterSeq       int64  `form:"afterSeq"`
}

type MessageOfflineRequest struct {
	ConversationID string `form:"conversationId" binding:"required,max=64"`
	AfterSeq       int64  `form:"afterSeq"`
	SnapshotSeq    int64  `form:"snapshotSeq" binding:"required,gt=0"`
	Limit          int    `form:"limit"`
}

type MessageSeqsRequest struct {
	ConversationID string `form:"conversationId" binding:"required,max=64"`
	Seqs           string `form:"seqs" binding:"required"`
}

type MessageResponse struct {
	MessageID       string  `json:"messageId"`
	ConversationID  string  `json:"conversationId"`
	SenderID        string  `json:"senderId"`
	ClientMessageID *string `json:"clientMsgId,omitempty"`
	Seq             int64   `json:"seq"`
	Type            int     `json:"cType"`
	Content         string  `json:"content"`
	Status          int8    `json:"status"`
	SendTime        int64   `json:"sendTime"`
	AttachmentID    string  `json:"attachmentId,omitempty"`
}

type MessageHistoryResponse struct {
	Messages   []MessageResponse `json:"messages"`
	NextCursor int64             `json:"nextCursor"`
	HasMore    bool              `json:"hasMore"`
}

type MessageSyncResponse struct {
	Messages []MessageResponse `json:"messages"`
}

type MessageOfflineResponse struct {
	Messages   []MessageResponse `json:"messages"`
	NextCursor int64             `json:"nextCursor"`
	HasMore    bool              `json:"hasMore"`
}

type MessageSeqsResponse struct {
	Messages []MessageResponse `json:"messages"`
}

func toHistoryMessageResponse(messages []messageapp.MessageDTO, nextCursor int64, hasMore bool) (res MessageHistoryResponse) {
	ms := toMessageResponses(messages)

	res.Messages = ms
	res.HasMore = hasMore
	res.NextCursor = nextCursor

	return
}

func toSyncMessageResponse(messages []messageapp.MessageDTO) (res MessageSyncResponse) {
	res.Messages = toMessageResponses(messages)
	return
}

func toOfflineMessageResponse(messages []messageapp.MessageDTO, nextCursor int64, hasMore bool) (res MessageOfflineResponse) {
	res.Messages = toMessageResponses(messages)
	res.NextCursor = nextCursor
	res.HasMore = hasMore
	return
}

func toMessageSeqsResponse(messages []messageapp.MessageDTO) (res MessageSeqsResponse) {
	res.Messages = toMessageResponses(messages)
	return
}

func toMessageResponses(messages []messageapp.MessageDTO) []MessageResponse {
	ms := make([]MessageResponse, 0, len(messages))

	for _, m := range messages {
		message := MessageResponse{
			MessageID:       m.MessageID,
			ConversationID:  m.ConversationID,
			SenderID:        m.SenderID,
			ClientMessageID: m.ClientMessageID,
			Seq:             m.Seq,
			Type:            m.Type,
			Content:         m.Content,
			Status:          m.Status,
			SendTime:        m.SendTime,
			AttachmentID:    m.AttachmentID,
		}

		ms = append(ms, message)
	}

	return ms
}
