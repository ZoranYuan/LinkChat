package message

import (
	"context"
	"errors"
	"time"

	inboxport "IM_backend/internal/application/ports/inbox"
	messagecache "IM_backend/internal/infra/cache/message"
)

const SendStatusNotFound = "not_found"

type SendMessageStatusDTO struct {
	ClientMsgID  string
	Status       string
	MessageID    string
	Seq          int64
	AttachmentID string
}

func (ma *MessageApplication) GetSendMessageStatus(
	ctx context.Context,
	senderID, clientMsgID string,
) (SendMessageStatusDTO, error) {
	entry, found, err := ma.messageCache.GetDedupEntry(ctx, senderID, clientMsgID)
	if err != nil {
		return SendMessageStatusDTO{}, err
	}
	if found {
		return sendMessageStatusFromEntry(clientMsgID, entry), nil
	}

	record, err := ma.inboxRepository.FindByKey(ctx, inboxport.Key{
		SenderID:    senderID,
		ClientMsgID: clientMsgID,
	})
	if err != nil {
		return SendMessageStatusDTO{}, err
	}
	if record == nil {
		return SendMessageStatusDTO{ClientMsgID: clientMsgID, Status: SendStatusNotFound}, nil
	}

	entry = messagecache.DedupEntry{
		SenderID:    senderID,
		RequestHash: record.RequestHash,
	}
	ttl := time.Duration(ma.config.Cache.MessageDedup.ProcessingTTLSeconds) * time.Second
	switch record.Status {
	case inboxport.StatusProcessing:
		entry.Status = messagecache.StatusProcessing
	case inboxport.StatusDead:
		entry.Status = messagecache.StatusFailed
		entry.Error = record.LastError
		ttl = time.Duration(ma.config.Cache.MessageDedup.TTLSeconds) * time.Second
	case inboxport.StatusCompleted:
		entry.Status = messagecache.StatusCompleted
		ttl = time.Duration(ma.config.Cache.MessageDedup.TTLSeconds) * time.Second
		persisted, err := ma.messageRepository.FindMessageBySenderIDAndClientMsgID(ctx, senderID, clientMsgID)
		if err != nil {
			return SendMessageStatusDTO{}, err
		}
		if persisted == nil {
			return SendMessageStatusDTO{}, errors.New("Inbox 已完成但对应消息不存在")
		}
		entry.MessageID = persisted.MessageID
		entry.Seq = persisted.Seq
		attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{persisted.MessageID}, false)
		if err != nil {
			return SendMessageStatusDTO{}, err
		}
		if attachment := attachments[persisted.MessageID]; attachment != nil {
			entry.AttachmentID = attachment.AttachmentID
		}
	default:
		return SendMessageStatusDTO{}, errors.New("Inbox 消息状态无效")
	}

	if err := ma.messageCache.UpdateDedupEntries(ctx, map[string]messagecache.DedupEntry{
		messagecache.MessageDedupKey(senderID, clientMsgID): entry,
	}, ttl); err != nil {
		return SendMessageStatusDTO{}, err
	}
	return sendMessageStatusFromEntry(clientMsgID, entry), nil
}

func sendMessageStatusFromEntry(clientMsgID string, entry messagecache.DedupEntry) SendMessageStatusDTO {
	return SendMessageStatusDTO{
		ClientMsgID:  clientMsgID,
		Status:       entry.Status,
		MessageID:    entry.MessageID,
		Seq:          entry.Seq,
		AttachmentID: entry.AttachmentID,
	}
}
