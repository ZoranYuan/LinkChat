package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	filecache "IM_backend/internal/application/ports/cache/file"
	messagecacheport "IM_backend/internal/application/ports/cache/message"
	roomcache "IM_backend/internal/application/ports/cache/room"
	idport "IM_backend/internal/application/ports/id"
	inboxport "IM_backend/internal/application/ports/inbox"
	"IM_backend/internal/application/ports/messaging"
	"IM_backend/internal/application/ports/transaction"
	roomapp "IM_backend/internal/application/room"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfile "IM_backend/internal/domain/file"
	domainmessage "IM_backend/internal/domain/message"
	messagecache "IM_backend/internal/infra/cache/message"
	"IM_backend/internal/infra/metrics"
)

type MessageHandler struct {
	messageCache                 messagecacheport.MessageCache
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
	messageCache messagecacheport.MessageCache,
) *MessageHandler {
	return &MessageHandler{
		delivery:     delivery,
		messageCache: messageCache,
		inbox:        inbox, txManager: txManager, config: config,
		conversationApplication: conversationApplication, conversationRepository: conversationRepository,
		messageRepository: messageRepository, userConversationRepository: userConversationRepository,
		fileRepository: fileRepository, fileCache: fileCache,
		messageAttachmentsRepository: messageAttachmentsRepository, messageStickerRepository: messageStickerRepository,
		idGenerator: idGenerator, roomCache: roomCache, roomApplication: roomApplication,
	}
}

func (m *MessageHandler) claimBatch(
	ctx context.Context,
	requests []inboxport.ClaimRequest,
) (string, []string, error) {
	lockToken := uuid.NewString()
	now := time.Now()
	var claimedEventIDs []string
	transactionStartedAt := time.Now()
	err := m.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		claimedEventIDs, err = m.inbox.WithTx(tx).TryClaimBatch(
			ctx,
			requests,
			lockToken,
			now,
			time.Now().Add(-time.Duration(m.config.Kafka.Consumer.LeaseStaleAfterSecs)*time.Second),
		)
		return err
	})
	metrics.ObserveKafkaDBTransaction("inbox_claim", len(requests), transactionStartedAt, err)
	if err != nil {
		return "", nil, err
	}

	return lockToken, claimedEventIDs, nil
}

func (m *MessageHandler) HandleSendMessages(
	ctx context.Context,
	conversationID string,
	records []*sarama.ConsumerMessage,
) (persistedCount, skippedCount int, err error) {
	decoded := make(map[string]messaging.SendMessageDTO, len(records))
	hashes := make(map[string]string, len(records))
	keys := make([][2]string, 0, len(records))
	requests := make([]inboxport.ClaimRequest, 0, len(records))
	for _, record := range records {
		var eventID, eventType string
		for _, header := range record.Headers {
			switch string(header.Key) {
			case "event_id":
				eventID = string(header.Value)
			case "event_type":
				eventType = string(header.Value)
			}
		}
		if eventID == "" {
			return 0, 0, errors.New("Kafka 消息缺少 event_id")
		}
		if eventType == "" {
			return 0, 0, errors.New("Kafka 消息缺少 event_type")
		}
		if eventType != messaging.TypeMessageSend {
			return 0, 0, errors.New("消息处理器不支持事件类型：" + eventType)
		}
		if _, exists := decoded[eventID]; exists {
			continue
		}
		var dto messaging.SendMessageDTO
		if err := json.Unmarshal(record.Value, &dto); err != nil {
			return 0, 0, err
		}
		decoded[eventID] = dto
		sum := sha256.Sum256(record.Value)
		hashes[eventID] = hex.EncodeToString(sum[:])
		keys = append(keys, [2]string{dto.SenderID, dto.ClientMessageID})
		requests = append(requests, inboxport.ClaimRequest{EventID: eventID, EventType: eventType})
	}
	lockToken, claimedEventIDs, err := m.claimBatch(ctx, requests)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err != nil {
			if releaseErr := m.inbox.MarkRetryBatch(ctx, claimedEventIDs, lockToken, err.Error()); releaseErr != nil {
				log.Printf("释放消息批次租约失败：conversation=%s error=%v", conversationID, releaseErr)
			}
		}
	}()
	claimed := make(map[string]struct{}, len(claimedEventIDs))
	for _, id := range claimedEventIDs {
		claimed[id] = struct{}{}
	}
	existing := make(map[[2]string]*domainmessage.Message)
	attachmentsByMessage := make(map[string]*domainmessage.MessageAttachment)
	if len(claimedEventIDs) < len(requests) {
		existing, attachmentsByMessage, err = m.loadExistingMessages(ctx, keys)
		if err != nil {
			return 0, 0, err
		}
	}
	info, err := m.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		return 0, 0, err
	}
	pull := false
	if len(claimedEventIDs) > 0 && messaging.ConvType(info.ConversationType) == messaging.RoomChat {
		room, roomErr := m.roomApplication.GetActiveRoom(ctx, info.RoomID)
		if roomErr != nil {
			return 0, 0, roomErr
		}
		pull = room.MemberCount > m.config.Message.RoomRealtimeFanoutLimit
	}
	var pushEvents []messaging.MessagePushEvent
	var dedupEntries map[string]messagecache.DedupEntry
	for {
		pushEvents = nil
		persistedCount = 0
		skippedCount = 0
		dedupEntries = make(map[string]messagecache.DedupEntry, len(requests))
		transactionStartedAt := time.Now()
		err = m.txManager.WithinTransaction(ctx, func(tx any) error {
			messages := make([]*domainmessage.Message, 0, len(claimedEventIDs))
			attachments := make([]*domainmessage.MessageAttachment, 0)
			stickers := make([]*domainmessage.MessageSticker, 0)
			resolved := make(map[[2]string]*domainmessage.Message, len(existing)+len(requests))
			for key, message := range existing {
				resolved[key] = message
			}
			messageByEvent := make(map[string]*domainmessage.Message, len(requests))
			var completedIDs []string
			now := time.Now().UnixMilli()
			expireAt := now + int64(m.config.Message.AttachmentTTLSeconds)*1000
			for _, request := range requests {
				dto := decoded[request.EventID]
				key := [2]string{dto.SenderID, dto.ClientMessageID}
				message := resolved[key]
				_, owned := claimed[request.EventID]
				if message != nil && (message.RequestHash != hashes[request.EventID] || message.ConversationID != conversationID) {
					if owned {
						if err := m.inbox.WithTx(tx).MarkDead(ctx, request.EventID, lockToken, domainmessage.ErrClientMessageConflict.Error(), time.Now()); err != nil {
							return err
						}
					}
					dedupEntries[messagecache.MessageDedupKey(dto.SenderID, dto.ClientMessageID)] = messagecache.DedupEntry{
						SenderID: dto.SenderID, MessageID: request.EventID, RequestHash: hashes[request.EventID], Status: messagecache.StatusFailed, Error: domainmessage.ErrClientMessageConflict.Error(),
					}
					skippedCount++
					continue
				}
				if message == nil && !owned {
					reason := "消息已终止处理"
					dedupEntries[messagecache.MessageDedupKey(dto.SenderID, dto.ClientMessageID)] = messagecache.DedupEntry{SenderID: dto.SenderID, MessageID: request.EventID, RequestHash: hashes[request.EventID], Status: messagecache.StatusFailed, Error: reason}
					skippedCount++
					continue
				}
				if owned {
					completedIDs = append(completedIDs, request.EventID)
				}
				if message != nil {
					messageByEvent[request.EventID] = message
					skippedCount++
					continue
				}
				message = domainmessage.NewMessage(request.EventID, conversationID, dto.SenderID, 0, domainmessage.MessageType(dto.Type), dto.Content)
				clientMsgID := dto.ClientMessageID
				message.ClientMsgID = &clientMsgID
				message.RequestHash = hashes[request.EventID]
				messages = append(messages, message)
				resolved[key] = message
				messageByEvent[request.EventID] = message
				persistedCount++
				switch message.Type {
				case domainmessage.Image, domainmessage.Video, domainmessage.File:
					attachmentID, err := m.idGenerator.Generate()
					if err != nil {
						return err
					}
					dto.AttachmentID = attachmentID
					attachments = append(attachments, domainmessage.NewMessageAttachment(attachmentID, message.MessageID, conversationID, dto.FileID, int8(message.Type), expireAt, now))
				case domainmessage.Sticker:
					stickers = append(stickers, domainmessage.NewMessageSticker(message.MessageID, dto.StickerID, dto.PackID, dto.Width, dto.Height))
				}
				pushEvents = append(pushEvents, messageEventFor(dto, message, info))
			}
			if len(messages) > 0 {
				firstSeq, err := m.conversationRepository.WithTx(tx).ReserveSequenceRange(ctx, conversationID, len(messages), messages[len(messages)-1].MessageID)
				if err != nil {
					return err
				}
				updates := make([]domainconversation.ReadSeqUpdate, len(messages))
				for i, message := range messages {
					message.Seq = firstSeq + int64(i)
					pushEvents[i].Seq = message.Seq
					updates[i] = domainconversation.ReadSeqUpdate{UserID: message.SenderID, ConversationID: conversationID, LastReadSeq: message.Seq}
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
			}
			if err := m.inbox.WithTx(tx).MarkCompletedBatch(ctx, completedIDs, lockToken, time.Now()); err != nil {
				return err
			}
			attachmentIDs := make(map[string]string, len(attachments)+len(attachmentsByMessage))
			for id, attachment := range attachmentsByMessage {
				attachmentIDs[id] = attachment.AttachmentID
			}
			for _, attachment := range attachments {
				attachmentIDs[attachment.MessageID] = attachment.AttachmentID
			}
			for _, request := range requests {
				message := messageByEvent[request.EventID]
				if message == nil {
					continue
				}
				dto := decoded[request.EventID]
				cacheKey := messagecache.MessageDedupKey(dto.SenderID, dto.ClientMessageID)
				dedupEntries[cacheKey] = messagecache.DedupEntry{SenderID: message.SenderID, MessageID: message.MessageID, RequestHash: message.RequestHash, Status: messagecache.StatusCompleted, Seq: message.Seq, AttachmentID: attachmentIDs[message.MessageID]}
			}
			return nil
		})
		metrics.ObserveKafkaDBTransaction("message_persist", persistedCount, transactionStartedAt, err)
		if !errors.Is(err, domainmessage.ErrDuplicateClientMessage) {
			break
		}
		existing, attachmentsByMessage, err = m.loadExistingMessages(ctx, keys)
		if err != nil {
			return 0, 0, err
		}
		if len(existing) == 0 {
			return 0, 0, domainmessage.ErrDuplicateClientMessage
		}
	}
	if err != nil {
		return 0, 0, err
	}
	if cacheErr := m.messageCache.UpdateDedupEntries(ctx, dedupEntries, time.Duration(m.config.Cache.MessageDedup.TTLSeconds)*time.Second); cacheErr != nil {
		log.Printf("回写消息幂等结果失败：conversation=%s error=%v", conversationID, cacheErr)
	}
	if len(pushEvents) == 0 {
		return persistedCount, skippedCount, nil
	}
	// 大房间直接缓存并走拉模式，其他房间根据活跃度决定是否预热和切换。
	if messaging.ConvType(info.ConversationType) == messaging.RoomChat {
		warm := pull
		if !pull {
			onlineCount := m.delivery.OnlineRoomConnectionCount(info.RoomID)
			level, err := m.roomCache.UpdateRoomActivity(ctx, info.RoomID, len(pushEvents), onlineCount, time.Now())
			if err != nil {
				log.Printf("更新房间活跃度失败：room=%s error=%v", info.RoomID, err)
			} else {
				pull = level == roomcache.RoomActivityActive
				warm = level >= roomcache.RoomActivityWarn
			}
		}
		if warm {
			if err := m.roomCache.WarmRecentMessageEvents(ctx, info.RoomID, pushEvents); err != nil {
				log.Printf("预热房间消息缓存失败：room=%s error=%v", info.RoomID, err)
			}
		}
	}

	for _, event := range pushEvents {
		err := m.delivery.Deliver(event, pull)
		if err != nil {
			log.Printf("消息推送失败：message=%s conversation=%s error=%v", event.MessageID, conversationID, err)
		}
	}

	return persistedCount, skippedCount, nil
}

func (m *MessageHandler) loadExistingMessages(ctx context.Context, keys [][2]string) (map[[2]string]*domainmessage.Message, map[string]*domainmessage.MessageAttachment, error) {
	rows, err := m.messageRepository.ListMessagesBySenderAndClientMsgIDs(ctx, keys)
	if err != nil {
		return nil, nil, err
	}
	existing := make(map[[2]string]*domainmessage.Message, len(rows))
	var mediaIDs []string
	for _, message := range rows {
		existing[[2]string{message.SenderID, *message.ClientMsgID}] = message
		if message.Type == domainmessage.Image || message.Type == domainmessage.Video || message.Type == domainmessage.File {
			mediaIDs = append(mediaIDs, message.MessageID)
		}
	}
	attachments, err := m.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, mediaIDs, false)
	return existing, attachments, err
}

func messageEventFor(dto messaging.SendMessageDTO, message *domainmessage.Message, info *conversationcache.ConversationInfo) messaging.MessagePushEvent {
	receiverID := info.RoomID
	if domainconversation.ConvType(info.ConversationType) == domainconversation.PrivateChat {
		receiverID = info.UserID1
		if message.SenderID == info.UserID1 {
			receiverID = info.UserID2
		}
	}
	return messaging.MessagePushEvent{
		MessageID: message.MessageID, ConversationID: message.ConversationID, SenderID: message.SenderID, RecvID: receiverID, Seq: message.Seq,
		ConvType: messaging.ConvType(info.ConversationType), MessageType: int(message.Type), Content: message.Content, SendTime: message.SendTime, ClientMsgID: dto.ClientMessageID, Status: int8(message.Status),
		AttachmentID: dto.AttachmentID, Width: dto.Width, Height: dto.Height, DurationMs: dto.DurationMs, StickerID: dto.StickerID, PackID: dto.PackID,
	}
}
