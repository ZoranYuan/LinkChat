package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type decodedMessage struct {
	dto         messaging.SendMessageDTO
	key         inboxport.Key
	requestHash string
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

func (m *MessageHandler) HandleSendMessages(
	ctx context.Context,
	conversationID string,
	records []*sarama.ConsumerMessage,
) error {
	if len(records) == 0 {
		return nil
	}

	items := make([]*decodedMessage, 0, len(records))
	itemsByKey := make(map[inboxport.Key]*decodedMessage, len(records))
	requests := make([]inboxport.ClaimRequest, 0, len(records))
	conflicts := make([]*decodedMessage, 0)
	for _, record := range records {
		var dto messaging.SendMessageDTO
		if err := json.Unmarshal(record.Value, &dto); err != nil {
			return err
		}
		sum := sha256.Sum256(record.Value)
		item := &decodedMessage{
			dto:         dto,
			key:         inboxport.Key{SenderID: dto.SenderID, ClientMsgID: dto.ClientMessageID},
			requestHash: hex.EncodeToString(sum[:]),
		}
		if previous := itemsByKey[item.key]; previous != nil {
			if previous.requestHash != item.requestHash {
				conflicts = append(conflicts, item)
			}
			continue
		}
		item.dto.ConversationID = conversationID
		items = append(items, item)
		itemsByKey[item.key] = item
		requests = append(requests, inboxport.ClaimRequest{
			SenderID: item.key.SenderID, ClientMsgID: item.key.ClientMsgID, RequestHash: item.requestHash,
		})
	}

	var info *conversationcache.ConversationInfo
	pull := false
	if len(items) > 0 {
		var err error
		info, err = m.conversationApplication.GetConversationInfo(ctx, conversationID)
		if err != nil {
			return err
		}
		if domainconversation.ConvType(info.ConversationType) == domainconversation.RoomChat {
			room, roomErr := m.roomApplication.GetActiveRoom(ctx, info.RoomID)
			if roomErr != nil {
				return roomErr
			}
			pull = room.MemberCount > m.config.Message.RoomRealtimeFanoutLimit
		}
	}

	type pendingMessage struct {
		item    *decodedMessage
		message *domainmessage.Message
	}
	var pushEvents []messaging.MessagePushEvent
	cacheEntries := make(map[string]messagecache.DedupEntry, len(items))
	batchToken := uuid.NewString()
	claimStartedAt := time.Now()
	claimNow := claimStartedAt
	staleBefore := claimNow.Add(-time.Duration(m.config.Kafka.Consumer.LeaseStaleAfterSecs) * time.Second)
	var claimed []inboxport.Key
	var claimConflicts []inboxport.Key
	err := m.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		claimed, claimConflicts, err = m.inbox.WithTx(tx).ClaimBatch(
			ctx, requests, batchToken, claimNow, staleBefore,
		)
		return err
	})
	metrics.ObserveKafkaDBTransaction("inbox_claim", len(requests), claimStartedAt, err)
	if err != nil {
		return err
	}
	for _, key := range claimConflicts {
		item := itemsByKey[key]

		// 标记冲突任务的状态
		cacheEntries[messagecache.MessageDedupKey(key.SenderID, key.ClientMsgID)] = messagecache.DedupEntry{
			SenderID: key.SenderID, RequestHash: item.requestHash,
			Status: messagecache.StatusFailed, Error: domainmessage.ErrClientMessageConflict.Error(),
		}
	}

	claimedItems := make([]*decodedMessage, len(claimed))
	for i, key := range claimed {
		claimedItems[i] = itemsByKey[key]
	}

	validItems := make([]*decodedMessage, 0, len(claimedItems))
	deadKeys := make([]inboxport.Key, 0)
	for _, item := range claimedItems {
		if item.dto.Type < int(domainmessage.Text) || item.dto.Type > int(domainmessage.File) {
			deadKeys = append(deadKeys, item.key)
			cacheEntries[messagecache.MessageDedupKey(item.key.SenderID, item.key.ClientMsgID)] = messagecache.DedupEntry{
				SenderID: item.key.SenderID, RequestHash: item.requestHash,
				Status: messagecache.StatusFailed, Error: "不支持的消息类型",
			}
			continue
		}
		validItems = append(validItems, item)
	}

	if len(claimedItems) > 0 {
		transactionStartedAt := time.Now()
		err = m.txManager.WithinTransaction(ctx, func(tx any) error {
			inboxTx := m.inbox.WithTx(tx)
			pending := make([]pendingMessage, 0, len(validItems))
			messages := make([]*domainmessage.Message, 0, len(validItems))
			attachments := make([]*domainmessage.MessageAttachment, 0)
			stickers := make([]*domainmessage.MessageSticker, 0)
			attachmentIDs := make(map[string]string, len(validItems))
			nowMillis := time.Now().UnixMilli()
			expireAt := nowMillis + m.config.Message.AttachmentTTLSeconds*1000
			for _, item := range validItems {
				messageID, err := m.idGenerator.Generate()
				if err != nil {
					return err
				}
				message := domainmessage.NewMessage(messageID, conversationID, item.dto.SenderID, 0,
					domainmessage.MessageType(item.dto.Type), item.dto.Content)
				clientMsgID := item.dto.ClientMessageID
				message.ClientMsgID = &clientMsgID
				messages = append(messages, message)
				pending = append(pending, pendingMessage{item: item, message: message})

				switch message.Type {
				case domainmessage.Image, domainmessage.Video, domainmessage.File:
					attachmentID, err := m.idGenerator.Generate()
					if err != nil {
						return err
					}
					item.dto.AttachmentID = attachmentID
					attachmentIDs[message.MessageID] = attachmentID
					attachments = append(attachments, domainmessage.NewMessageAttachment(
						attachmentID, message.MessageID, conversationID, item.dto.FileID,
						int8(message.Type), expireAt, nowMillis,
					))
				case domainmessage.Sticker:
					stickers = append(stickers, domainmessage.NewMessageSticker(
						message.MessageID, item.dto.StickerID, item.dto.PackID, item.dto.Width, item.dto.Height,
					))
				}
			}

			readSeqByUser := make(map[string]int64, len(pending))
			if len(messages) > 0 {
				firstSeq, err := m.conversationRepository.WithTx(tx).ReserveSequenceRange(
					ctx,
					conversationID,
					len(messages),
					messages[len(messages)-1].MessageID,
				)
				if err != nil {
					return err
				}
				for i, item := range pending {
					item.message.Seq = firstSeq + int64(i)
					if item.message.Seq > readSeqByUser[item.message.SenderID] {
						readSeqByUser[item.message.SenderID] = item.message.Seq
					}
					pushEvents = append(pushEvents, messageEventFor(item.item.dto, item.message, info))
					cacheEntries[messagecache.MessageDedupKey(item.item.key.SenderID, item.item.key.ClientMsgID)] = messagecache.DedupEntry{
						SenderID: item.item.key.SenderID, MessageID: item.message.MessageID,
						RequestHash: item.item.requestHash, Status: messagecache.StatusCompleted,
						Seq: item.message.Seq, AttachmentID: attachmentIDs[item.message.MessageID],
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
				readUpdates := make([]domainconversation.ReadSeqUpdate, 0, len(readSeqByUser))
				for userID, seq := range readSeqByUser {
					readUpdates = append(readUpdates, domainconversation.ReadSeqUpdate{
						UserID: userID, ConversationID: conversationID, LastReadSeq: seq,
					})
				}
				if err := m.userConversationRepository.WithTx(tx).UpdateReadSeqBatch(ctx, readUpdates); err != nil {
					return err
				}
			}

			if err := inboxTx.MarkDeadBatch(ctx, deadKeys, batchToken, "不支持的消息类型"); err != nil {
				return err
			}
			validKeys := make([]inboxport.Key, 0, len(validItems))
			for _, item := range validItems {
				validKeys = append(validKeys, item.key)
			}
			return inboxTx.MarkCompletedBatch(ctx, validKeys, batchToken)
		})
		metrics.ObserveKafkaDBTransaction("message_persist", len(claimedItems), transactionStartedAt, err)
	}
	if err != nil {
		return err
	}

	cacheTTL := time.Duration(m.config.Cache.MessageDedup.TTLSeconds) * time.Second
	if err := m.messageCache.UpdateDedupEntries(ctx, cacheEntries, cacheTTL); err != nil {
		log.Printf("回写消息幂等状态失败：conversation=%s error=%v", conversationID, err)
	}
	for _, item := range conflicts {
		entry := map[string]messagecache.DedupEntry{
			messagecache.MessageDedupKey(item.key.SenderID, item.key.ClientMsgID): {
				SenderID: item.key.SenderID, RequestHash: item.requestHash,
				Status: messagecache.StatusFailed, Error: domainmessage.ErrClientMessageConflict.Error(),
			},
		}
		if err := m.messageCache.UpdateDedupEntries(ctx, entry, cacheTTL); err != nil {
			log.Printf("回写冲突消息状态失败：sender=%s client_msg_id=%s error=%v", item.key.SenderID, item.key.ClientMsgID, err)
		}
	}

	if len(pushEvents) == 0 {
		return nil
	}

	conversationType := messaging.ConvType(info.ConversationType)
	warm := conversationType != messaging.RoomChat || pull
	if conversationType == messaging.RoomChat {
		if !pull {
			onlineCount := m.delivery.OnlineRoomConnectionCount(info.RoomID)
			level, activityErr := m.roomCache.UpdateRoomActivity(ctx, info.RoomID, len(pushEvents), onlineCount, time.Now())
			if activityErr != nil {
				log.Printf("更新房间活跃度失败：room=%s error=%v", info.RoomID, activityErr)
			} else {
				pull = level == roomcache.RoomActivityActive
				warm = level >= roomcache.RoomActivityWarn
			}
		}
	}
	if conversationType != messaging.RoomChat || warm {
		if err := m.roomCache.WarmRecentMessageEvents(ctx, conversationID, pushEvents); err != nil {
			log.Printf("预热会话消息缓存失败：conversation=%s error=%v", conversationID, err)
		}
	}

	for _, event := range pushEvents {
		if err := m.delivery.Deliver(event, pull); err != nil {
			log.Printf("消息推送失败：message=%s conversation=%s error=%v", event.MessageID, conversationID, err)
		}
	}
	return nil
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
