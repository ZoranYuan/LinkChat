package send

import (
	messagecache "IM_backend/core/infra/cache/message"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/core/application/conversation"
	groupapp "IM_backend/core/application/group"
	"IM_backend/core/application/message"
	filecache "IM_backend/core/application/ports/cache/file"
	groupcache "IM_backend/core/application/ports/cache/group"
	messagecacheport "IM_backend/core/application/ports/cache/message"
	eventport "IM_backend/core/application/ports/events"
	idport "IM_backend/core/application/ports/id"
	inboxport "IM_backend/core/application/ports/inbox"
	"IM_backend/core/application/ports/transaction"
	domainconversation "IM_backend/core/domain/conversation"
	domainfile "IM_backend/core/domain/file"
	domainmessage "IM_backend/core/domain/message"
	"IM_backend/core/infra/metrics"
	"IM_backend/core/infra/mq/handlers"
	"IM_backend/core/infra/mq/kafka"
)

type MessageHandler struct {
	messageCache                 messagecacheport.MessageCache
	delivery                     *handlers.MessageDelivery
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
	groupCache                   groupcache.GroupCache
	groupApplication             *groupapp.GroupApplication
	batchAggregator              *batchAggregator
	producer                     eventport.Publisher
}

func NewMessageHandler(
	ctx context.Context,
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
	groupCache groupcache.GroupCache,
	groupApplication *groupapp.GroupApplication,
	delivery *handlers.MessageDelivery,
	messageCache messagecacheport.MessageCache,
	producer eventport.Publisher,
) *MessageHandler {
	m := &MessageHandler{
		delivery:     delivery,
		messageCache: messageCache,
		inbox:        inbox, txManager: txManager, config: config,
		conversationApplication: conversationApplication, conversationRepository: conversationRepository,
		messageRepository: messageRepository, userConversationRepository: userConversationRepository,
		fileRepository: fileRepository, fileCache: fileCache,
		messageAttachmentsRepository: messageAttachmentsRepository, messageStickerRepository: messageStickerRepository,
		idGenerator: idGenerator,
		groupCache:  groupCache, groupApplication: groupApplication,
		producer: producer,
	}

	batchSize := config.Kafka.Consumer.Batch.Size
	if batchSize <= 0 {
		batchSize = 500
	}
	flushInterval := time.Duration(config.Kafka.Consumer.Batch.FlushIntervalMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}
	m.batchAggregator = newBatchAggregator(ctx, batchSize, flushInterval, m.flushBatch)

	return m
}

type pendingPublish struct {
	topic   string
	key     string
	payload any
}

func (m *MessageHandler) Topic() string { return m.config.Kafka.Topics.MessageSendPersist }

func (m *MessageHandler) Handle(ctx context.Context, item *kafka.MessageItem) error {
	m.batchAggregator.Submit(item)
	return nil
}

func getHeaderFromConsumeMessage(item *kafka.MessageItem, key string) string {
	for _, h := range item.Record.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (m *MessageHandler) buildResendPending(
	ctx context.Context, dto *message.SendMessageDTO, cid, hash string,
) ([]pendingPublish, error) {
	msg, err := m.messageRepository.FindMessageBySenderIDAndClientMsgID(ctx, dto.SenderID, cid)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, fmt.Errorf("inbox 已完成但消息不存在：%s", cid)
	}

	entry := messagecache.DedupEntry{
		SenderID:    dto.SenderID,
		MessageID:   msg.MessageID,
		RequestHash: hash,
		Status:      messagecache.StatusCompleted,
		SendTime:    msg.SendTime,
		Seq:         msg.Seq,
		StickerID:   dto.StickerID,
		PackID:      dto.PackID,
	}

	if msg.Type != domainmessage.Text && msg.Type != domainmessage.Sticker {
		atts, err := m.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{msg.MessageID}, false)
		if err != nil {
			return nil, err
		}
		if a := atts[msg.MessageID]; a != nil {
			entry.AttachmentID = a.AttachmentID
		}
	}

	return []pendingPublish{
		{topic: m.config.Kafka.Topics.MessageSendCache, key: cid, payload: entry},
	}, nil
}

func (m *MessageHandler) flushBatch(ctx context.Context, convID string, items []*kafka.MessageItem) {

	var validItems []*kafka.MessageItem

	for _, item := range items {
		var probe message.SendMessageDTO
		if err := json.Unmarshal(item.Record.Value, &probe); err != nil {
			log.Printf("毒消息跳过：topic=%s partition=%d offset=%d err=%v",
				item.Record.Topic, item.Record.Partition, item.Record.Offset, err)
			item.Complete()
			continue
		}
		validItems = append(validItems, item)
	}

	if len(validItems) == 0 {
		return
	}

	ids := make([]string, len(validItems))
	hashOf := make(map[string]string, len(validItems))

	for i, item := range validItems {
		ids[i] = getHeaderFromConsumeMessage(item, "event_id")
		if ids[i] == "" {
			continue
		}
		sum := sha256.Sum256(item.Record.Value)
		hashOf[ids[i]] = hex.EncodeToString(sum[:])
	}

	// 事务内收集待投递和待回填缓存，事务提交后统一发
	var pending []pendingPublish
	var duplicates []*kafka.MessageItem
	var conflicts []*kafka.MessageItem

	txStartedAt := time.Now()
	err := m.txManager.WithinTransaction(ctx, func(tx any) error {
		inboxTx := m.inbox.WithTx(tx)
		existing, err := inboxTx.FindByEventIDs(ctx, eventport.KafkaEventMessageSend, ids)
		if err != nil {
			return err
		}

		fresh := make([]*kafka.MessageItem, 0, len(validItems))
		for _, item := range validItems {
			id := getHeaderFromConsumeMessage(item, "event_id")
			switch old, found := existing[id]; {
			case !found:
				fresh = append(fresh, item)
			case old == hashOf[id]:
				// duplicate：合法重投，跳过
				duplicates = append(duplicates, item)
			default:
				// conflict：内容不一致，跳过
				conflicts = append(conflicts, item)
			}
		}

		if len(fresh) == 0 {
			return nil
		}

		reqs := make([]inboxport.CreateRequest, 0, len(fresh))
		for _, item := range fresh {
			id := getHeaderFromConsumeMessage(item, "event_id")
			reqs = append(reqs, inboxport.CreateRequest{
				EventType: eventport.KafkaEventMessageSend, EventID: id, RequestHash: hashOf[id],
			})
		}
		if err := inboxTx.CreateBatch(ctx, reqs); err != nil {
			return err
		}

		dtos := make([]message.SendMessageDTO, len(fresh))
		now := time.Now().UnixMilli()
		expireAt := now + m.config.Message.AttachmentTTLSeconds*1000
		messages := make([]*domainmessage.Message, 0, len(fresh))
		attachments := make([]*domainmessage.MessageAttachment, 0, len(fresh))
		stickers := make([]*domainmessage.MessageSticker, 0, len(fresh))
		attachmentIDs := make([]string, len(fresh))

		for i, item := range fresh {
			if err := json.Unmarshal(item.Record.Value, &dtos[i]); err != nil {
				return err
			}

			dto := &dtos[i]

			mid, err := m.idGenerator.Generate()
			if err != nil {
				return err
			}
			msg := domainmessage.NewMessage(mid, convID, dto.SenderID, 0,
				domainmessage.MessageType(dto.Type), dto.Content)
			// 消息的 event_id 就是 clientMsgID
			cid := getHeaderFromConsumeMessage(item, "event_id")
			msg.ClientMsgID = &cid
			messages = append(messages, msg)

			// 收集媒体消息
			switch msg.Type {
			case domainmessage.Image, domainmessage.Video, domainmessage.File:
				aid, err := m.idGenerator.Generate()
				if err != nil {
					return err
				}
				attachmentIDs[i] = aid
				attachments = append(attachments, domainmessage.NewMessageAttachment(
					aid, mid, convID, dto.FileID, expireAt, now))
			case domainmessage.Sticker:
				stickers = append(stickers, domainmessage.NewMessageSticker(
					mid, dto.StickerID, dto.PackID, 0, 0))
			}
		}

		// 预分配 seq、消息入库
		firstSeq, err := m.conversationRepository.WithTx(tx).ReserveSequenceRange(
			ctx, convID, len(messages), messages[len(messages)-1].MessageID)
		if err != nil {
			return err
		}

		// 构建用户已读水位
		readSeq := make(map[string]int64, len(fresh))

		for i, msg := range messages {
			dto := &dtos[i]
			msg.Seq = firstSeq + int64(i)
			if msg.Seq > readSeq[msg.SenderID] {
				readSeq[msg.SenderID] = msg.Seq
			}

			cid := getHeaderFromConsumeMessage(fresh[i], "event_id")

			pending = append(pending,
				pendingPublish{
					topic: m.config.Kafka.Topics.MessageSendCache,
					key:   cid,
					payload: messagecache.DedupEntry{
						SenderID:     dto.SenderID,
						MessageID:    msg.MessageID,
						RequestHash:  hashOf[cid],
						Status:       messagecache.StatusCompleted,
						SendTime:     msg.SendTime,
						Seq:          msg.Seq,
						AttachmentID: attachmentIDs[i],
						StickerID:    dto.StickerID,
						PackID:       dto.PackID,
					},
				},
				pendingPublish{
					topic: m.config.Kafka.Topics.MessageSendPush,
					key:   msg.MessageID,
					payload: transferPayload{
						MessageID:    msg.MessageID,
						ConvID:       convID,
						SenderID:     dto.SenderID,
						Seq:          msg.Seq,
						Type:         dto.Type,
						Content:      dto.Content,
						SendTime:     msg.SendTime,
						AttachmentID: attachmentIDs[i],
						ClientMsgID:  cid,
						StickerID:    dto.StickerID,
						PackID:       dto.PackID,
					},
				},
			)
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

		updates := make([]domainconversation.ReadSeqUpdate, 0, len(readSeq))
		for uid, seq := range readSeq {
			updates = append(updates, domainconversation.ReadSeqUpdate{
				UserID: uid, ConversationID: convID, LastReadSeq: seq,
			})
		}

		return m.userConversationRepository.WithTx(tx).UpdateReadSeqBatch(ctx, updates)
	})
	metrics.ObserveKafkaDBTransaction("flush_batch", len(validItems), txStartedAt, err)
	if err != nil {
		// 批量写入失败，降级为逐条写入
		log.Printf("批落库失败，降级逐条：conv=%s n=%d err=%v", convID, len(validItems), err)
		for _, item := range validItems {
			cid := getHeaderFromConsumeMessage(item, "event_id")

			var dto message.SendMessageDTO
			if e := json.Unmarshal(item.Record.Value, &dto); e != nil {
				log.Printf("毒消息跳过：clientMsgID=%s err=%v", cid, e)
				item.Complete()
				continue
			}

			mid, e := m.idGenerator.Generate()
			if e != nil {
				log.Printf("生成 ID 失败：clientMsgID=%s err=%v", cid, e)
				item.Complete()
				continue
			}
			msg := domainmessage.NewMessage(mid, convID, dto.SenderID, 0,
				domainmessage.MessageType(dto.Type), dto.Content)
			msg.ClientMsgID = &cid

			e = m.txManager.WithinTransaction(ctx, func(tx any) error {
				if e := m.inbox.WithTx(tx).CreateBatch(ctx, []inboxport.CreateRequest{{
					EventType: eventport.KafkaEventMessageSend, EventID: cid,
					RequestHash: hashOf[cid],
				}}); e != nil {
					return e
				}
				firstSeq, e := m.conversationRepository.WithTx(tx).ReserveSequenceRange(ctx, convID, 1, mid)
				if e != nil {
					return e
				}
				msg.Seq = firstSeq
				if e := m.messageRepository.WithTx(tx).CreateNewMessages(ctx,
					[]*domainmessage.Message{msg}); e != nil {
					return e
				}
				return m.userConversationRepository.WithTx(tx).UpdateReadSeqBatch(ctx,
					[]domainconversation.ReadSeqUpdate{{
						UserID: dto.SenderID, ConversationID: convID, LastReadSeq: firstSeq,
					}})
			})

			if e != nil {
				log.Printf("逐条落库失败：clientMsgID=%s err=%v", cid, e)
				// 后期可以换成投入到死信之后再提交
				item.Complete()
				continue
			}

			cacheBody, _ := json.Marshal(messagecache.DedupEntry{
				SenderID:    dto.SenderID,
				MessageID:   msg.MessageID,
				RequestHash: hashOf[cid],
				Status:      messagecache.StatusCompleted,
				SendTime:    msg.SendTime,
				Seq:         msg.Seq,
			})
			pushBody, _ := json.Marshal(transferPayload{
				MessageID:   msg.MessageID,
				ConvID:      convID,
				SenderID:    dto.SenderID,
				Seq:         msg.Seq,
				Type:        dto.Type,
				Content:     dto.Content,
				SendTime:    msg.SendTime,
				ClientMsgID: cid,
			})
			pmsgs := []eventport.PublishMessage{
				{
					Topic:        m.config.Kafka.Topics.MessageSendCache,
					EventType:    eventport.KafkaEventMessageSend,
					EventID:      cid,
					PartitionKey: cid,
					Payload:      cacheBody,
				},
				{
					Topic:        m.config.Kafka.Topics.MessageSendPush,
					EventType:    eventport.KafkaEventMessageSend,
					EventID:      msg.MessageID,
					PartitionKey: msg.MessageID,
					Payload:      pushBody,
				},
			}
			if err := m.producer.PublishBatch(ctx, pmsgs); err != nil {
				log.Printf("批量投递失败：clientMsgID=%s err=%v", cid, err)
			}
			item.Complete()
		}
		return
	}

	// 处理冲突， 直接告警，不能影响数据库已经成功的状态，后续可以进一步处理，比如设置一些防御的 redis 缓存
	for _, item := range conflicts {
		log.Printf("消息内容冲突，跳过：clientMsgID=%s topic=%s offset=%d",
			getHeaderFromConsumeMessage(item, "event_id"), item.Record.Topic, item.Record.Offset)
	}

	// 处理重复
	for _, item := range duplicates {
		var dto message.SendMessageDTO
		if err := json.Unmarshal(item.Record.Value, &dto); err != nil {
			continue
		}
		cid := getHeaderFromConsumeMessage(item, "event_id")
		extra, err := m.buildResendPending(ctx, &dto, cid, hashOf[cid])
		if err != nil {
			log.Printf("duplicate 回填失败：clientMsgID=%s err=%v", cid, err)
			continue
		}
		pending = append(pending, extra...)
	}

	publishStartedAt := time.Now()

	// 按照 topic 分组
	grouped := make(map[string][]eventport.PublishMessage)
	for _, p := range pending {
		body, err := json.Marshal(p.payload)
		if err != nil {
			log.Printf("序列化投递失败：topic=%s key=%s err=%v", p.topic, p.key, err)
			continue
		}
		grouped[p.topic] = append(grouped[p.topic], eventport.PublishMessage{
			Topic:        p.topic,
			EventType:    eventport.KafkaEventMessageSend,
			EventID:      p.key,
			PartitionKey: p.key,
			Payload:      body,
		})
	}

	for topic, msgs := range grouped {
		start := time.Now()
		if err := m.producer.PublishBatch(ctx, msgs); err != nil {
			log.Printf("批量投递失败：topic=%s n=%d err=%v", topic, len(msgs), err)
		}
		perMsg := time.Since(start).Seconds() / float64(len(msgs))
		for range msgs {
			metrics.KafkaPublishMessageDuration.WithLabelValues(topic).Observe(perMsg)
		}
	}

	metrics.KafkaPublishBatchDuration.Observe(time.Since(publishStartedAt).Seconds())
	metrics.KafkaPublishBatchMessages.Observe(float64(len(pending)))

	for _, item := range validItems {
		item.Complete()
	}
}

func (m *MessageHandler) Drain() {
	m.batchAggregator.Drain()
}
