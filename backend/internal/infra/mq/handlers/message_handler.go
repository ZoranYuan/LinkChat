package handlers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	filecache "IM_backend/internal/application/ports/cache/file"
	roomcache "IM_backend/internal/application/ports/cache/room"
	idport "IM_backend/internal/application/ports/id"
	inboxport "IM_backend/internal/application/ports/inbox"
	"IM_backend/internal/application/ports/messaging"
	"IM_backend/internal/application/ports/transaction"
	roomapp "IM_backend/internal/application/room"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfile "IM_backend/internal/domain/file"
	domainmessage "IM_backend/internal/domain/message"
)

type MessageHandler struct {
	delivery                     *MessageDelivery
	inbox                        inboxport.InboxRepository
	txManager                    transaction.Manager
	config                       configs.Config
	conversationApplication      *conversationapp.ConversationApplication
	conversationRepository       domainconversation.ConversationRepository
	messageRepository            domainmessage.MessageRepository
	userConversationRepository   domainconversation.UserConversationRepository
	fileRepository               domainfile.FileRepository
	fileCache                    filecache.FileCache
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository
	messageStickerRepository     domainmessage.MessageStickerRepository
	idGenerator                  idport.Generator
	roomCache                    roomcache.RoomCache
	roomApplication              *roomapp.RoomApplication
}

func NewMessageHandler(
	inbox inboxport.InboxRepository,
	txManager transaction.Manager,
	config configs.Config,
	conversationApplication *conversationapp.ConversationApplication,
	conversationRepository domainconversation.ConversationRepository,
	messageRepository domainmessage.MessageRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	fileRepository domainfile.FileRepository,
	fileCache filecache.FileCache,
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository,
	messageStickerRepository domainmessage.MessageStickerRepository,
	idGenerator idport.Generator,
	roomCache roomcache.RoomCache,
	roomApplication *roomapp.RoomApplication,
	delivery *MessageDelivery,
) *MessageHandler {
	return &MessageHandler{
		delivery: delivery,
		inbox:    inbox, txManager: txManager, config: config,
		conversationApplication: conversationApplication, conversationRepository: conversationRepository,
		messageRepository: messageRepository, userConversationRepository: userConversationRepository,
		fileRepository: fileRepository, fileCache: fileCache,
		messageAttachmentsRepository: messageAttachmentsRepository, messageStickerRepository: messageStickerRepository,
		idGenerator: idGenerator, roomCache: roomCache, roomApplication: roomApplication,
	}
}

func (m *MessageHandler) claimBatch(
	ctx context.Context,
	events []messaging.IncomingEvent,
) (string, []string, error) {
	requests := make([]inboxport.ClaimRequest, len(events))
	for i, event := range events {
		requests[i] = inboxport.ClaimRequest{
			EventID:   event.EventID,
			EventType: string(event.Name),
		}
	}

	lockToken := uuid.NewString()
	now := time.Now()
	var claimedEventIDs []string
	err := m.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		claimedEventIDs, err = m.inbox.WithTx(tx).TryClaimBatch(
			ctx,
			requests,
			lockToken,
			now,
		)
		return err
	})
	if err != nil {
		return "", nil, err
	}

	return lockToken, claimedEventIDs, nil
}

func (m *MessageHandler) HandleSendMessages(
	ctx context.Context,
	conversationID string,
	events []messaging.IncomingEvent,
) (successMessages, failedMessages []messaging.IncomingEvent, err error) {
	lockToken, claimedEventIDs, err := m.claimBatch(ctx, events)
	if err != nil {
		return nil, events, err
	}

	claimedIDs := make(map[string]struct{}, len(claimedEventIDs))
	for _, eventID := range claimedEventIDs {
		claimedIDs[eventID] = struct{}{}
	}

	successMessages = make([]messaging.IncomingEvent, 0, len(claimedEventIDs))
	seen := make(map[string]struct{}, len(claimedEventIDs))
	for _, event := range events {
		if _, claimed := claimedIDs[event.EventID]; !claimed {
			failedMessages = append(failedMessages, event)
			continue
		}
		if _, exists := seen[event.EventID]; exists {
			continue
		}
		seen[event.EventID] = struct{}{}
		successMessages = append(successMessages, event)
	}

	log.Printf("Inbox 抢占成功：conversation=%s count=%d", conversationID, len(successMessages))
	if len(successMessages) == 0 {
		return successMessages, failedMessages, nil
	}

	info, err := m.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		failedMessages = append(failedMessages, successMessages...)
		return nil, failedMessages, err
	}
	pushEvents := make([]messaging.MessagePushEvent, len(successMessages))

	err = m.txManager.WithinTransaction(ctx, func(tx any) error {
		messages := make([]*domainmessage.Message, len(successMessages))
		attachments := make([]*domainmessage.MessageAttachment, 0)
		stickers := make([]*domainmessage.MessageSticker, 0)
		now := time.Now().UnixMilli()
		expireAt := now + int64(m.config.Message.AttachmentTTLSeconds)*1000

		for i, event := range successMessages {
			var dto messaging.SendMessageDTO
			if err := json.Unmarshal(event.Payload, &dto); err != nil {
				return err
			}

			message := domainmessage.NewMessage(
				event.EventID,
				conversationID,
				dto.SenderID,
				0,
				domainmessage.MessageType(dto.Type),
				dto.Content,
			)
			message.ClientMsgID = &dto.ClientMessageID
			messages[i] = message

			switch message.Type {
			case domainmessage.Image, domainmessage.Video, domainmessage.File:
				attachmentID, err := m.idGenerator.Generate()
				if err != nil {
					return err
				}
				dto.AttachmentID = attachmentID
				attachments = append(attachments, domainmessage.NewMessageAttachment(
					attachmentID,
					message.MessageID,
					conversationID,
					dto.FileID,
					int8(message.Type),
					expireAt,
					now,
				))
			case domainmessage.Sticker:
				stickers = append(stickers, domainmessage.NewMessageSticker(
					message.MessageID,
					dto.StickerID,
					dto.PackID,
					dto.Width,
					dto.Height,
				))
			}

			receiverID := info.RoomID
			if domainconversation.ConvType(info.ConversationType) == domainconversation.PrivateChat {
				receiverID = info.UserID1
				if message.SenderID == info.UserID1 {
					receiverID = info.UserID2
				}
			}
			pushEvents[i] = messaging.MessagePushEvent{
				MessageID:      message.MessageID,
				ConversationID: conversationID,
				SenderID:       message.SenderID,
				RecvID:         receiverID,
				ConvType:       messaging.ConvType(info.ConversationType),
				MessageType:    int(message.Type),
				Content:        message.Content,
				SendTime:       message.SendTime,
				ClientMsgID:    dto.ClientMessageID,
				Status:         int8(message.Status),
				AttachmentID:   dto.AttachmentID,
				Width:          dto.Width,
				Height:         dto.Height,
				DurationMs:     dto.DurationMs,
				StickerID:      dto.StickerID,
				PackID:         dto.PackID,
			}
		}

		firstSeq, err := m.conversationRepository.WithTx(tx).ReserveSequenceRange(
			ctx,
			conversationID,
			len(messages),
			messages[len(messages)-1].MessageID,
		)
		if err != nil {
			return err
		}

		updates := make([]domainconversation.ReadSeqUpdate, len(messages))
		for i, message := range messages {
			message.Seq = firstSeq + int64(i)
			pushEvents[i].Seq = message.Seq
			updates[i] = domainconversation.ReadSeqUpdate{
				UserID:         message.SenderID,
				ConversationID: conversationID,
				LastReadSeq:    message.Seq,
			}
		}

		if err := m.messageRepository.WithTx(tx).CreateNewMessages(ctx, messages); err != nil {
			return err
		}
		if err := m.messageAttachmentsRepository.WithTx(tx).CreateBatch(ctx, attachments); err != nil {
			return err
		}
		if err := m.messageStickerRepository.WithTx(tx).CreateBatch(ctx, stickers); err != nil {
			return err
		}

		if err := m.userConversationRepository.WithTx(tx).UpdateReadSeqBatch(ctx, updates); err != nil {
			return err
		}

		eventIDs := make([]string, len(successMessages))
		for i, event := range successMessages {
			eventIDs[i] = event.EventID
		}
		return m.inbox.WithTx(tx).MarkCompletedBatch(ctx, eventIDs, lockToken, time.Now())
	})

	if err != nil {
		failedMessages = append(failedMessages, successMessages...)
		return nil, failedMessages, err
	}

	// 计算房间聊天的活跃度
	active := false
	if messaging.ConvType(info.ConversationType) == messaging.RoomChat {
		onlineCount := m.delivery.OnlineRoomConnectionCount(info.RoomID)
		level, err := m.roomCache.UpdateRoomActivity(ctx, info.RoomID, len(pushEvents), onlineCount, time.Now())
		if err != nil {
			log.Printf("更新房间活跃度失败：room=%s error=%v", info.RoomID, err)
		} else {
			active = level == roomcache.RoomActivityActive
			if level >= roomcache.RoomActivityWarn {
				if err := m.roomCache.WarmRecentMessageEvents(ctx, info.RoomID, pushEvents); err != nil {
					log.Printf("预热房间消息缓存失败：room=%s error=%v", info.RoomID, err)
				}
			}
		}
	}

	for _, event := range pushEvents {
		payload, err := json.Marshal(event)
		if err == nil {
			err = m.delivery.Deliver(ctx, messaging.TypeMessagePush, event, payload, active)
		}
		if err != nil {
			log.Printf("消息推送失败：message=%s conversation=%s error=%v", event.MessageID, conversationID, err)
		}
	}

	return successMessages, failedMessages, nil
}
