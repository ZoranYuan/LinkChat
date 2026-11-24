package message

import (
	"context"
	"errors"
	"log"
	"time"

	eventport "IM_backend/core/application/ports/events"
	inboxport "IM_backend/core/application/ports/inbox"
	domainmessage "IM_backend/core/domain/message"
	messagecache "IM_backend/core/infra/cache/message"
)

const SendStatusNotFound = "not_found"

type SendMessageStatusDTO struct {
	ClientMsgID  string
	Status       string
	MessageID    string
	Seq          int64
	SendTime     int64
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

	entry, found, err = ma.buildEntry(ctx, senderID, clientMsgID)

	if err != nil {
		return SendMessageStatusDTO{}, err
	}

	if !found {
		return SendMessageStatusDTO{ClientMsgID: clientMsgID, Status: SendStatusNotFound}, nil
	}

	ma.rebuildDedupCache(ctx, senderID, clientMsgID, entry)
	return sendMessageStatusFromEntry(clientMsgID, entry), nil
}

func (ma *MessageApplication) rebuildDedupCache(ctx context.Context, senderID, clientMsgID string, entry messagecache.DedupEntry) {
	ttl := time.Duration(ma.config.Cache.MessageDedup.TTLSeconds) * time.Second
	if err := ma.messageCache.UpdateDedupEntries(ctx, map[string]messagecache.DedupEntry{
		messagecache.MessageDedupKey(senderID, clientMsgID): entry,
	}, ttl); err != nil {
		log.Printf("回写发送状态缓存失败：sender=%s clientMsgID=%s err=%v", senderID, clientMsgID, err)
	}
}

func (ma *MessageApplication) buildEntry(
	ctx context.Context,
	senderID, clientMsgID string,
) (messagecache.DedupEntry, bool, error) {
	record, err := ma.inboxRepository.FindByKey(ctx, inboxport.Key{
		EventType: eventport.KafkaEventMessageSend,
		EventID:   clientMsgID,
	})

	if err != nil {
		return messagecache.DedupEntry{}, false, err
	}

	if record == nil {
		return messagecache.DedupEntry{}, false, nil
	}

	// 已经查到了，则从 message 表中获取对应的实际消息
	message, err := ma.messageRepository.FindMessageBySenderIDAndClientMsgID(ctx, senderID, clientMsgID)
	if err != nil {
		return messagecache.DedupEntry{}, false, err
	}

	if message == nil {
		return messagecache.DedupEntry{}, false, errors.New("Inbox 已完成但对应消息不存在")
	}

	entry := messagecache.DedupEntry{
		RequestHash: record.RequestHash,
		Status:      messagecache.StatusCompleted,
		MessageID:   message.MessageID,
		Seq:         message.Seq,
		SendTime:    message.SendTime,
	}

	// 构造媒体消息
	if message.Type != domainmessage.Text && message.Type != domainmessage.Sticker {
		attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{message.MessageID}, false)
		if err != nil {
			return messagecache.DedupEntry{}, false, err
		}
		if attachment := attachments[message.MessageID]; attachment != nil {
			entry.AttachmentID = attachment.AttachmentID
		}
	}

	return entry, true, nil
}

func sendMessageStatusFromEntry(clientMsgID string, entry messagecache.DedupEntry) SendMessageStatusDTO {
	return SendMessageStatusDTO{
		ClientMsgID:  clientMsgID,
		Status:       entry.Status,
		MessageID:    entry.MessageID,
		Seq:          entry.Seq,
		SendTime:     entry.SendTime,
		AttachmentID: entry.AttachmentID,
	}
}
