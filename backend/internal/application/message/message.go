package message

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	roomunreadcache "IM_backend/internal/application/ports/cache/agent"
	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	filecache "IM_backend/internal/application/ports/cache/file"
	messagecache "IM_backend/internal/application/ports/cache/message"
	roomcache "IM_backend/internal/application/ports/cache/room"
	idport "IM_backend/internal/application/ports/id"
	"IM_backend/internal/application/ports/messaging"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/application/ports/realtime"
	"IM_backend/internal/application/ports/transaction"
	roomapp "IM_backend/internal/application/room"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfile "IM_backend/internal/domain/file"
	domainfriend "IM_backend/internal/domain/friend"
	domainmessage "IM_backend/internal/domain/message"
	domainroom "IM_backend/internal/domain/room"
)

type MessageApplication struct {
	messageCache                 messagecache.MessageCache
	roomMemberCache              roomcache.RoomMemberCache
	roomCache                    roomcache.RoomCache
	roomApplication              *roomapp.RoomApplication
	roomActivityState            roomcache.ActivityState
	roomUnreadSnapshotCache      roomunreadcache.RoomUnreadSnapshotCache
	messageRepository            domainmessage.MessageRepository
	txManager                    transaction.Manager
	userConversationRepository   domainconversation.UserConversationRepository
	conversationRepository       domainconversation.ConversationRepository
	conversationApplication      *conversationapp.ConversationApplication
	friendRepository             domainfriend.FriendRepository
	fileRepository               domainfile.FileRepository
	fileCache                    filecache.FileCache
	messageStickerRepository     domainmessage.MessageStickerRepository
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository
	messageOutboxRepository      outboxport.OutboxRepository
	producerNotifier             messaging.ProducerNotifier
	roomOnlineConnectionCounter  realtime.RoomOnlineConnectionCounter
	roomMemberRepository         domainroom.RoomMemberRepository
	sf                           singleflight.Group
	idGenerator                  idport.Generator
	config                       configs.Config
}

// SetProducerNotifier 在事务提交后唤醒 Producer；定时扫描仍负责兜底。
func (ma *MessageApplication) SetProducerNotifier(notifier messaging.ProducerNotifier) {
	if ma == nil {
		return
	}
	ma.producerNotifier = notifier
}

// SetRoomOnlineConnectionCounter 提供当前节点在线连接数，用于房间消息缓存策略。
func (ma *MessageApplication) SetRoomOnlineConnectionCounter(counter realtime.RoomOnlineConnectionCounter) {
	ma.roomOnlineConnectionCounter = counter
}

func (ma *MessageApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	ma.conversationApplication = application
}

func (ma *MessageApplication) notifyProducer() {
	if ma != nil && ma.producerNotifier != nil {
		ma.producerNotifier.Notify()
	}
}

func NewMessageApplication(
	messageCache messagecache.MessageCache,
	roomMemberCache roomcache.RoomMemberCache,
	roomCache roomcache.RoomCache,
	roomActivityState roomcache.ActivityState,
	txManager transaction.Manager,
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	messageOutboxRepository outboxport.OutboxRepository,
	friendRepository domainfriend.FriendRepository,
	fileRepository domainfile.FileRepository,
	fileCache filecache.FileCache,
	messageStickerRepository domainmessage.MessageStickerRepository,
	messageRepository domainmessage.MessageRepository,
	roomMemberRepository domainroom.RoomMemberRepository,
	roomApplication *roomapp.RoomApplication,
	idGenerator idport.Generator,
	config configs.Config,
	roomUnreadSnapshotCache roomunreadcache.RoomUnreadSnapshotCache,
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository,

) *MessageApplication {
	if roomActivityState == nil {
		panic("房间 ACTIVE 状态未配置")
	}
	app := &MessageApplication{
		messageCache:                 messageCache,
		roomMemberCache:              roomMemberCache,
		roomCache:                    roomCache,
		roomActivityState:            roomActivityState,
		roomUnreadSnapshotCache:      roomUnreadSnapshotCache,
		txManager:                    txManager,
		messageOutboxRepository:      messageOutboxRepository,
		messageRepository:            messageRepository,
		conversationRepository:       conversationRepository,
		userConversationRepository:   userConversationRepository,
		roomApplication:              roomApplication,
		roomMemberRepository:         roomMemberRepository,
		friendRepository:             friendRepository,
		fileRepository:               fileRepository,
		fileCache:                    fileCache,
		messageStickerRepository:     messageStickerRepository,
		idGenerator:                  idGenerator,
		config:                       config,
		messageAttachmentsRepository: messageAttachmentsRepository,
	}
	return app
}

func applyConversationTarget(dto *SendMessageDTO, info *conversationcache.ConversationInfo) error {
	dto.ConversationType = info.ConversationType
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.PrivateChat:
		switch dto.SenderID {
		case info.UserID1:
			dto.ReceiverID = info.UserID2
		case info.UserID2:
			dto.ReceiverID = info.UserID1
		default:
			return ErrForbidden
		}
	case domainconversation.Room:
		dto.ReceiverID = info.RoomID
	default:
		return ErrUnknownConversationType
	}
	return nil
}

func (ma *MessageApplication) validateFileReference(dto SendMessageDTO) error {
	if dto.FileID == "" {
		return errors.New("文件 ID 不能为空")
	}

	if len(dto.FileID) > ma.config.Message.MaxIdentifierLength {
		return errors.New("文件 ID 无效")
	}

	return nil
}

// 根据消息类型检验消息内容
func (ma *MessageApplication) validateMessageContent(dto SendMessageDTO) error {
	if dto.SenderID == "" || dto.ReceiverID == "" {
		return errors.New("发送方或接收方不能为空")
	}

	switch domainmessage.MessageType(dto.Type) {
	case domainmessage.Text:
		content := strings.TrimSpace(dto.Content)
		if content == "" {
			return errors.New("文本内容不能为空")
		}

		if utf8.RuneCountInString(content) > ma.config.Message.MaxTextRunes {
			return errors.New("文本内容过长")
		}

		// 文本消息，其他的媒体消息字段都为空
		if dto.FileID != "" ||
			dto.StickerID != "" ||
			dto.PackID != "" ||
			dto.Width != 0 ||
			dto.Height != 0 ||
			dto.DurationMs != nil {
			return errors.New("文本消息包含非法媒体字段")
		}
	case domainmessage.Image:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" ||
			dto.PackID != "" {
			return errors.New("图片消息包含非法字段")
		}
	case domainmessage.Video:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" || dto.PackID != "" {
			return errors.New("视频消息包含非法字段")
		}
	case domainmessage.File:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" ||
			dto.PackID != "" {
			return errors.New("文件消息包含非法字段")
		}

	case domainmessage.Sticker:
		if dto.StickerID == "" {
			return errors.New("表情 ID 不能为空")
		}
		if len(dto.StickerID) > ma.config.Message.MaxIdentifierLength || len(dto.PackID) > ma.config.Message.MaxIdentifierLength {
			return errors.New("表情标识过长")
		}
	}

	return nil
}

func (ma *MessageApplication) HandleSendMessages(ctx context.Context, conversationID string, dtos []SendMessageDTO) ([]*MessageAckDTO, error) {
	if len(dtos) == 0 {
		return []*MessageAckDTO{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ma == nil || ma.txManager == nil || ma.messageOutboxRepository == nil {
		return nil, errors.New("消息出箱组件未配置")
	}

	type preparedMessage struct {
		index       int
		dto         SendMessageDTO
		message     *domainmessage.Message
		event       messaging.MessageEvent
		mediaWriter func(context.Context, any) error
	}

	info, err := ma.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	acks := make([]*MessageAckDTO, len(dtos))
	prepared := make([]preparedMessage, 0, len(dtos))
	fillUnresolvedAcks := func(cause error) {
		for i := range acks {
			if acks[i] == nil {
				acks[i] = &MessageAckDTO{ClientMessageID: dtos[i].ClientMessageID, Status: string(messaging.AckStatusFailed), Error: cause.Error()}
			}
		}
	}

	type sendCandidate struct {
		index       int
		dto         SendMessageDTO
		requestHash string
	}
	candidates := make([]sendCandidate, 0, len(dtos))
	for i := range dtos {
		dto := dtos[i]
		if dto.ConversationID != conversationID {
			return nil, errors.New("一个批次只能包含同一会话的消息")
		}
		if err := applyConversationTarget(&dto, info); err != nil {
			acks[i] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		candidates = append(candidates, sendCandidate{index: i, dto: dto})
	}

	if len(candidates) == 0 {
		return acks, nil
	}

	authorizationErrors, err := ma.authorizeSendBatch(ctx, info, dtos)
	if err != nil {
		fillUnresolvedAcks(err)
		return acks, err
	}

	// 原地筛选候选消息，保留原始下标以对应每条 ACK。
	valid := candidates[:0]
	keys := make([]messagecache.DedupKey, 0, len(candidates))
	for _, candidate := range candidates {
		if authErr := authorizationErrors[candidate.index]; authErr != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: candidate.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: authErr.Error()}
			continue
		}
		if err := ma.validateMessageContent(candidate.dto); err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: candidate.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		requestHash, err := buildMessageRequestHash(candidate.dto)
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: candidate.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		candidate.requestHash = requestHash
		valid = append(valid, candidate)
		if candidate.dto.ClientMessageID != "" {
			key := messagecache.DedupKey{SenderID: candidate.dto.SenderID, ClientMsgID: candidate.dto.ClientMessageID}
			keys = append(keys, key)
		}
	}
	if len(valid) == 0 {
		return acks, nil
	}

	var dedupEntries map[messagecache.DedupKey]messagecache.DedupEntry
	if ma.messageCache != nil && len(keys) > 0 {
		entries, cacheErr := ma.messageCache.GetDedupEntries(ctx, keys)
		if cacheErr != nil {
			log.Printf("批量读取消息幂等缓存失败，继续由数据库唯一约束兜底：conversation=%s err=%v", conversationID, cacheErr)
		} else {
			dedupEntries = entries
		}
	}

	for _, candidate := range valid {
		dto := candidate.dto
		entry := dedupEntries[messagecache.DedupKey{SenderID: dto.SenderID, ClientMsgID: dto.ClientMessageID}]
		if entry.MessageID != "" {
			if entry.RequestHash != candidate.requestHash {
				acks[candidate.index] = &MessageAckDTO{
					ClientMessageID: dto.ClientMessageID,
					Status:          string(messaging.AckStatusFailed),
					Error:           domainmessage.ErrClientMessageConflict.Error(),
				}
				continue
			}
			if entry.ConversationID != "" && entry.Seq > 0 {
				acks[candidate.index] = &MessageAckDTO{
					ClientMessageID: dto.ClientMessageID,
					ConversationID:  entry.ConversationID,
					MessageID:       entry.MessageID,
					Seq:             entry.Seq,
					AttachmentID:    entry.AttachmentID,
					SendTime:        entry.SendTime,
					Status:          string(messaging.AckStatusSent),
				}
				continue
			}
		}
		if err := ma.normalizeMediaDTO(ctx, &dto); err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		messageID, err := ma.idGenerator.Generate()
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		message := domainmessage.NewMessage(messageID, conversationID, dto.SenderID, 0, domainmessage.MessageType(dto.Type), dto.Content)
		if dto.ClientMessageID != "" {
			clientMessageID := dto.ClientMessageID
			message.ClientMsgID = &clientMessageID
		}
		message.RequestHash = candidate.requestHash
		mediaWriter, err := ma.buildMediaWriter(&dto, messageID)
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
			continue
		}
		prepared = append(prepared, preparedMessage{
			index:       candidate.index,
			dto:         dto,
			message:     message,
			mediaWriter: mediaWriter,
		})
	}

	if len(prepared) == 0 {
		return acks, nil
	}
	err = ma.txManager.WithinTransaction(ctx, func(tx any) error {
		msgRepo := ma.messageRepository.WithTx(tx)
		convRepo := ma.conversationRepository.WithTx(tx)
		userConvRepo := ma.userConversationRepository.WithTx(tx)
		outboxRepo := ma.messageOutboxRepository.WithTx(tx)

		for i := range prepared {
			item := &prepared[i]
			if domainmessage.MessageType(item.dto.Type) != domainmessage.Image &&
				domainmessage.MessageType(item.dto.Type) != domainmessage.Video &&
				domainmessage.MessageType(item.dto.Type) != domainmessage.File {
				continue
			}
			file, lockErr := ma.fileRepository.WithTx(tx).
				FindUploadedFileByIDAndUploaderForUpdate(ctx, item.dto.FileID, item.dto.SenderID)
			if lockErr != nil {
				return lockErr
			}
			if file == nil {
				return fmt.Errorf("file is missing or being deleted: %s", item.dto.FileID)
			}
			item.dto.Content = file.FileName
			item.message.Content = file.FileName
		}

		messages := make([]*domainmessage.Message, len(prepared))
		for i := range prepared {
			messages[i] = prepared[i].message
		}
		firstSeq, reserveErr := convRepo.ReserveSequenceRange(
			ctx,
			conversationID,
			len(messages),
			messages[len(messages)-1].MessageID,
		)
		if reserveErr != nil {
			if errors.Is(reserveErr, domainconversation.ErrConversationNotCreated) {
				return ErrConversationNotFound
			}
			return ErrConversationSequenceUpdate
		}

		outboxes := make([]*outboxport.Entry, 0, len(prepared))
		updates := make([]domainconversation.ReadSeqUpdate, 0, len(prepared))
		for i := range prepared {
			item := &prepared[i]
			item.message.Seq = firstSeq + int64(i)
			event := messaging.MessageEvent{
				MessageID:      item.message.MessageID,
				ConversationID: conversationID,
				SenderID:       item.dto.SenderID,
				RecvID:         item.dto.ReceiverID,
				ConvType:       messaging.ConvType(item.dto.ConversationType),
				MessageType:    item.dto.Type,
				Content:        item.message.Content,
				SendTime:       item.message.SendTime,
				ClientMsgID:    item.dto.ClientMessageID,
				Status:         int8(item.message.Status),
				Seq:            item.message.Seq,
				AttachmentID:   item.dto.AttachmentID,
				Width:          item.dto.Width,
				Height:         item.dto.Height,
				DurationMs:     item.dto.DurationMs,
				StickerID:      item.dto.StickerID,
				PackID:         item.dto.PackID,
			}
			item.event = event
			outbox, outboxErr := messageOutboxEntry(
				messaging.TypeMessage,
				conversationID,
				event,
			)
			if outboxErr != nil {
				return outboxErr
			}
			outboxes = append(outboxes, outbox)

			if item.mediaWriter != nil {
				if writeErr := item.mediaWriter(ctx, tx); writeErr != nil {
					return writeErr
				}
			}
			updates = append(updates, domainconversation.ReadSeqUpdate{
				UserID:         item.dto.SenderID,
				ConversationID: conversationID,
				LastReadSeq:    item.message.Seq,
			})
		}

		if err := msgRepo.CreateNewMessages(ctx, messages); err != nil {
			return err
		}
		if err := userConvRepo.UpdateReadSeqBatch(ctx, updates); err != nil {
			return err
		}
		return outboxRepo.CreateBatch(ctx, outboxes)
	})
	if err != nil {
		for _, item := range prepared {
			acks[item.index] = &MessageAckDTO{ClientMessageID: item.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: err.Error()}
		}
		if errors.Is(err, domainmessage.ErrDuplicateClientMessage) && len(prepared) == 1 {
			item := prepared[0]
			existing, findErr := ma.messageRepository.FindMessageBySenderIDAndClientMsgID(ctx, item.dto.SenderID, item.dto.ClientMessageID)
			if findErr == nil && existing != nil && sameClientMessage(item.dto, existing, item.message.RequestHash) {
				acks[item.index] = ma.existingMessageResult(ctx, item.dto, existing)
				return acks, nil
			}
			if findErr == nil && existing != nil {
				acks[item.index] = &MessageAckDTO{ClientMessageID: item.dto.ClientMessageID, Status: string(messaging.AckStatusFailed), Error: domainmessage.ErrClientMessageConflict.Error()}
				return acks, nil
			}
			if findErr != nil {
				return acks, findErr
			}
		}
		return acks, err
	}

	dedupWrites := make([]messagecache.DedupEntryWrite, 0, len(prepared))
	roomEvents := make([]messaging.MessageEvent, 0, len(prepared))
	for i := range prepared {
		item := prepared[i]
		acks[item.index] = &MessageAckDTO{
			ClientMessageID: item.dto.ClientMessageID,
			ConversationID:  conversationID,
			MessageID:       item.message.MessageID,
			Seq:             item.message.Seq,
			AttachmentID:    item.dto.AttachmentID,
			SendTime:        item.message.SendTime,
			Status:          string(messaging.AckStatusSent),
		}
		if item.event.ConvType == messaging.Room {
			roomEvents = append(roomEvents, item.event)
		}
		if item.dto.ClientMessageID != "" && ma.messageCache != nil {
			dedupWrites = append(dedupWrites, messagecache.DedupEntryWrite{
				Key: messagecache.DedupKey{SenderID: item.dto.SenderID, ClientMsgID: item.dto.ClientMessageID},
				Entry: messagecache.DedupEntry{
					MessageID:      item.message.MessageID,
					RequestHash:    item.message.RequestHash,
					ConversationID: conversationID,
					Seq:            item.message.Seq,
					AttachmentID:   item.dto.AttachmentID,
					SendTime:       item.message.SendTime,
				},
			})
		}
	}
	if len(roomEvents) > 0 {
		ma.warmRoomMessagesCache(ctx, info.RoomID, roomEvents)
	}
	if len(dedupWrites) > 0 {
		if err := ma.messageCache.SetDedupEntries(ctx, dedupWrites, time.Duration(ma.config.Cache.MessageDedup.TTLSeconds)*time.Second); err != nil {
			log.Printf("批量写入消息幂等缓存失败：conversation=%s count=%d err=%v", conversationID, len(dedupWrites), err)
		}
	}
	ma.notifyProducer()
	return acks, nil
}

// existingMessageResult 将数据库中的已有消息转换成幂等请求的返回值。
func (ma *MessageApplication) existingMessageResult(
	ctx context.Context,
	dto SendMessageDTO,
	existing *domainmessage.Message,
) *MessageAckDTO {
	if existing == nil {
		return nil
	}

	attachmentID := ""
	if ma.messageAttachmentsRepository != nil {
		attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{existing.MessageID}, false)
		if err == nil {
			if attachment := attachments[existing.MessageID]; attachment != nil {
				attachmentID = attachment.AttachmentID
			}
		}
	}

	return &MessageAckDTO{
		ClientMessageID: dto.ClientMessageID,
		ConversationID:  existing.ConversationID,
		MessageID:       existing.MessageID,
		Seq:             existing.Seq,
		AttachmentID:    attachmentID,
		SendTime:        existing.SendTime,
		Status:          string(messaging.AckStatusSent),
	}
}

type messageRequestFingerprint struct {
	ConversationID string `json:"conversationId"`
	MessageType    int    `json:"cType"`
	Content        string `json:"content"`
	FileID         string `json:"fileId"`
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	DurationMs     *int64 `json:"durationMs,omitempty"`
	StickerID      string `json:"stickerId"`
	PackID         string `json:"packId"`
}

func buildMessageRequestHash(dto SendMessageDTO) (string, error) {
	content := strings.TrimSpace(dto.Content)
	switch domainmessage.MessageType(dto.Type) {
	case domainmessage.Image, domainmessage.Video, domainmessage.File:
		// 媒体消息的身份由 FileId 表示，不依赖文件名等数据库元数据。
		content = ""
		dto.Width = 0
		dto.Height = 0
		dto.DurationMs = nil
	}

	payload, err := json.Marshal(messageRequestFingerprint{
		ConversationID: dto.ConversationID,
		MessageType:    dto.Type,
		Content:        content,
		FileID:         dto.FileID,
		Width:          dto.Width,
		Height:         dto.Height,
		DurationMs:     dto.DurationMs,
		StickerID:      dto.StickerID,
		PackID:         dto.PackID,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func sameClientMessage(dto SendMessageDTO, existing *domainmessage.Message, requestHash string) bool {
	if existing == nil {
		return false
	}
	if existing.RequestHash != "" {
		return existing.RequestHash == requestHash
	}
	return existing.ConversationID == dto.ConversationID &&
		existing.Type == domainmessage.MessageType(dto.Type) &&
		existing.Content == dto.Content
}

func messageOutboxEntry(eventType, partitionKey string, event any) (*outboxport.Entry, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return &outboxport.Entry{EventType: eventType, PartitionKey: partitionKey, Payload: payload}, nil
}

func (ma *MessageApplication) isRoomConvMember(ctx context.Context, userID, roomID string) error {
	key := "room_member:" + roomID + ":" + userID
	resultCh := ma.sf.DoChan(key, func() (any, error) {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ma.config.Message.MemberQueryTimeoutSeconds)*time.Second)
		defer cancel()
		var state *roomcache.MemberState
		var hit bool
		if ma.roomMemberCache != nil {
			var err error
			state, hit, err = ma.roomMemberCache.GetMember(queryCtx, roomID, userID)
			if err != nil {
				hit = false
			}
		}
		if !hit {
			member, err := ma.roomMemberRepository.FindRoomMemberByUserIDAndRoomID(userID, roomID)
			if err != nil && !errors.Is(err, domainroom.ErrMemberNotFound) {
				return nil, err
			}
			if member == nil || errors.Is(err, domainroom.ErrMemberNotFound) {
				if ma.roomMemberCache != nil {
					_ = ma.roomMemberCache.SetMemberNotFound(queryCtx, roomID, userID)
				}
				return nil, ErrNotRoomMember
			}
			state = &roomcache.MemberState{Status: member.Status, Role: member.Role, MuteUntil: member.MuteUntil, Version: member.Version}
			if ma.roomMemberCache != nil {
				_, _ = ma.roomMemberCache.SetMemberIfVersionGreater(queryCtx, roomID, userID, state)
			}
		}
		switch {
		case state == nil:
			return nil, ErrNotRoomMember
		case state.Status == domainroom.Active:
			return nil, nil
		case state.Status == domainroom.Muted && state.MuteUntil != nil && *state.MuteUntil <= time.Now().UnixMilli():
			return nil, nil
		case state.Status == domainroom.Kicked || state.Status == domainroom.Muted:
			return nil, ErrForbidden
		default:
			return nil, ErrNotRoomMember
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		return result.Err
	}
}

func (ma *MessageApplication) authorizeSendBatch(ctx context.Context, info *conversationcache.ConversationInfo, dtos []SendMessageDTO) ([]error, error) {
	results := make([]error, len(dtos))
	if len(dtos) == 0 {
		return results, nil
	}
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.Room:
		if ma.roomMemberRepository == nil {
			return nil, errors.New("房间成员仓储未配置")
		}
		userIDs := make([]string, 0, len(dtos))
		seen := make(map[string]struct{}, len(dtos))
		for _, dto := range dtos {
			if _, exists := seen[dto.SenderID]; exists {
				continue
			}
			seen[dto.SenderID] = struct{}{}
			userIDs = append(userIDs, dto.SenderID)
		}
		members, err := ma.roomMemberRepository.ListRoomMembersByRoomIDAndUserIDs(ctx, info.RoomID, userIDs)
		if err != nil {
			return nil, err
		}
		byUserID := make(map[string]*domainroom.RoomMember, len(members))
		for _, member := range members {
			if member != nil {
				byUserID[member.UserID] = member
			}
		}
		now := time.Now().UnixMilli()
		for i, dto := range dtos {
			member := byUserID[dto.SenderID]
			switch {
			case member == nil:
				results[i] = ErrNotRoomMember
			case member.Status == domainroom.Kicked:
				results[i] = ErrForbidden
			case member.Status == domainroom.Active:
			case member.Status == domainroom.Muted && member.MuteUntil != nil && *member.MuteUntil <= now:
			default:
				results[i] = ErrNotRoomMember
				if member.Status == domainroom.Muted {
					results[i] = ErrForbidden
				}
			}
		}
	case domainconversation.PrivateChat:
		if ma.friendRepository == nil {
			return nil, errors.New("好友关系仓储未配置")
		}
		pairs := []domainfriend.RelationPair{
			{UserID: info.UserID1, FriendUserID: info.UserID2},
			{UserID: info.UserID2, FriendUserID: info.UserID1},
		}
		relations, err := ma.friendRepository.ListFriendsByUserPairs(ctx, pairs)
		if err != nil {
			return nil, err
		}
		byPair := make(map[domainfriend.RelationPair]*domainfriend.Friend, len(relations))
		for _, relation := range relations {
			if relation != nil {
				byPair[domainfriend.RelationPair{UserID: relation.UserID, FriendUserID: relation.FriendUserID}] = relation
			}
		}
		first := byPair[pairs[0]]
		second := byPair[pairs[1]]
		if first == nil || second == nil || first.Status != domainfriend.StatusFriend || second.Status != domainfriend.StatusFriend {
			for i := range results {
				results[i] = ErrNotFriends
			}
		}
	default:
		return nil, ErrUnknownConversationType
	}
	return results, nil
}

func (ma *MessageApplication) ListActiveRoomIDsByUserID(userID string) ([]string, error) {
	if ma.roomMemberRepository == nil {
		return nil, errors.New("房间成员仓储未配置")
	}
	return ma.roomMemberRepository.ListActiveRoomIDsByUserID(userID)
}

func (ma *MessageApplication) checkConversationAccess(ctx context.Context, info *conversationcache.ConversationInfo, userID string) error {
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.PrivateChat:
		if info.UserID1 != userID && info.UserID2 != userID {
			return ErrForbidden
		}
		return nil
	case domainconversation.Room:
		err := ma.isRoomConvMember(ctx, userID, info.RoomID)
		if errors.Is(err, domainroom.ErrMemberNotFound) || errors.Is(err, ErrNotRoomMember) || errors.Is(err, ErrForbidden) {
			return ErrForbidden
		}
		return err
	default:
		return ErrUnknownConversationType
	}
}

func (ma *MessageApplication) normalizeMediaDTO(ctx context.Context, dto *SendMessageDTO) error {
	switch domainmessage.MessageType(dto.Type) {
	case domainmessage.Image, domainmessage.Video, domainmessage.File:
		file, err := ma.findUploadedFile(ctx, dto.FileID, dto.SenderID)
		if err != nil {
			return err
		}
		if file == nil {
			return fmt.Errorf("文件不存在或不可用：%s", dto.FileID)
		}

		// 文件名、大小和 MIME 以服务端文件记录为准；客户端媒体元数据全部忽略。
		dto.Content = file.FileName
		dto.Width = 0
		dto.Height = 0
		dto.DurationMs = nil

		switch domainmessage.MessageType(dto.Type) {
		case domainmessage.Image:
			if !strings.HasPrefix(strings.ToLower(file.ContentType), "image/") {
				return fmt.Errorf("图片消息引用的文件类型不匹配")
			}
		case domainmessage.Video:
			if !strings.HasPrefix(strings.ToLower(file.ContentType), "video/") {
				return fmt.Errorf("视频消息引用的文件类型不匹配")
			}
		}
	}
	return nil
}

// findUploadedFile 优先读取按 fileId 预热的文件缓存，缓存未命中或不满足
// 当前用户归属校验时再回源数据库。事务内仍会通过 FOR UPDATE 再次确认，
// 防止文件清理与发送消息并发导致引用到已删除文件。
func (ma *MessageApplication) findUploadedFile(ctx context.Context, fileID, uploaderID string) (*domainfile.File, error) {
	if ma.fileCache != nil {
		cached, cacheErr := ma.fileCache.GetFileMetadata(ctx, fileID)
		if cacheErr != nil {
			log.Printf("读取文件元数据缓存失败: fileId=%s err=%v", fileID, cacheErr)
		} else if cached != nil &&
			cached.Status == domainfile.FileStatusUploaded &&
			cached.UploaderID == uploaderID {
			return cached, nil
		}
	}

	if ma.fileRepository == nil {
		return nil, errors.New("文件仓储未配置")
	}
	file, err := ma.fileRepository.FindUploadedFileByIDAndUploader(ctx, fileID, uploaderID)
	if err != nil || file == nil {
		return file, err
	}
	if ma.fileCache != nil {
		_ = ma.fileCache.SetFileMetadata(ctx, file, time.Duration(ma.config.Storage.MinIO.CacheTTLSeconds)*time.Second)
	}
	return file, nil
}

func (ma *MessageApplication) buildMediaWriter(dto *SendMessageDTO, messageID string) (func(context.Context, any) error, error) {
	switch domainmessage.MessageType(dto.Type) {
	case domainmessage.Image, domainmessage.File, domainmessage.Video:
		if ma.messageAttachmentsRepository == nil {
			return nil, fmt.Errorf("消息附件仓储未配置")
		}
		attachmentID, err := ma.idGenerator.Generate()
		if err != nil {
			return nil, err
		}
		dto.AttachmentID = attachmentID
		attachmentTTL := ma.config.Message.AttachmentTTLSeconds
		if attachmentTTL <= 0 {
			return nil, errors.New("消息附件有效期配置无效")
		}
		now := time.Now().UnixMilli()
		attachment := domainmessage.NewMessageAttachment(
			attachmentID,
			messageID,
			dto.ConversationID,
			dto.FileID,
			int8(dto.Type),
			now+attachmentTTL*int64(time.Second/time.Millisecond),
			now,
		)
		return func(ctx context.Context, tx any) error {
			return ma.messageAttachmentsRepository.WithTx(tx).Create(ctx, attachment)
		}, nil
	case domainmessage.Sticker:
		if ma.messageStickerRepository == nil {
			return nil, fmt.Errorf("消息表情仓储未配置")
		}
		item := domainmessage.NewMessageSticker(
			messageID,
			dto.StickerID,
			dto.PackID,
			dto.Width,
			dto.Height,
		)
		return func(ctx context.Context, tx any) error {
			return ma.messageStickerRepository.WithTx(tx).Create(ctx, item)
		}, nil
	default:
		return nil, nil
	}
}

func (ma *MessageApplication) fillAttachmentIDs(ctx context.Context, messages []MessageDTO) error {
	if len(messages) == 0 || ma.messageAttachmentsRepository == nil {
		return nil
	}

	messageIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		if message.MessageID != "" {
			messageIDs = append(messageIDs, message.MessageID)
		}
	}
	attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, messageIDs, true)
	if err != nil {
		return err
	}

	for i := range messages {
		if attachment := attachments[messages[i].MessageID]; attachment != nil {
			messages[i].AttachmentID = attachment.AttachmentID
		}
	}
	return nil
}

// warmRoomMessagesCache 批量统计已提交的房间消息并按需回填近期消息缓存。
func (ma *MessageApplication) warmRoomMessagesCache(ctx context.Context, roomID string, events []messaging.MessageEvent) {
	if len(events) == 0 || ma.roomCache == nil {
		return
	}
	room, err := ma.roomApplication.GetActiveRoom(ctx, roomID)
	if err != nil {
		log.Printf("读取房间消息缓存策略失败：room=%s error=%v", roomID, err)
		return
	}
	onlineConnections := room.MemberCount
	if ma.roomOnlineConnectionCounter != nil {
		if count, err := ma.roomOnlineConnectionCounter.OnlineRoomConnectionCount(ctx, roomID); err != nil {
			log.Printf("读取房间在线连接数失败，使用成员数估算：room=%s error=%v", roomID, err)
		} else {
			onlineConnections = count
		}
	}
	now := time.Now()
	level, err := ma.roomCache.RecordActivityBatchAndGetLevel(ctx, roomID, len(events), onlineConnections, now)
	if err != nil {
		log.Printf("记录房间活跃度失败：room=%s count=%d error=%v", roomID, len(events), err)
		level = roomcache.RoomActivityNormal
	}
	if level == roomcache.RoomActivityActive {
		ma.roomActivityState.MarkActive(roomID, now)
	}
	// ACTIVE 保持期间即使窗口计数下降，也继续预热供 Consumer 拉模式使用的消息。
	if room.MemberCount <= ma.config.Message.RoomRealtimeFanoutLimit && level < roomcache.RoomActivityWarn &&
		!ma.roomActivityState.IsActive(roomID, now) {
		return
	}
	if err := ma.roomCache.WarmRecentMessageEvents(ctx, roomID, events); err != nil {
		log.Printf("批量回填已提交房间消息缓存失败：room=%s count=%d error=%v", roomID, len(events), err)
	}
}

// messageEventsToAppDTO 将近期消息缓存中的内部事件转换为应用层消息 DTO。
// 转换时补齐会话 ID、状态和附件 ID，供历史或同步接口复用统一的消息响应链路。
func messageEventsToAppDTO(events []messaging.MessageEvent, conversationID string) []MessageDTO {
	if len(events) == 0 {
		return []MessageDTO{}
	}

	result := make([]MessageDTO, 0, len(events))
	for _, event := range events {
		if event.MessageID == "" || event.Seq <= 0 {
			continue
		}

		resolvedID := event.ConversationID
		if resolvedID == "" {
			resolvedID = conversationID
		}

		item := MessageDTO{
			SenderID:       event.SenderID,
			ConversationID: resolvedID,
			MessageID:      event.MessageID,
			Seq:            event.Seq,
			Type:           event.MessageType,
			Content:        event.Content,
			SendTime:       event.SendTime,
			Status:         event.Status,
			AttachmentID:   event.AttachmentID,
			Width:          event.Width,
			Height:         event.Height,
			DurationMs:     event.DurationMs,
			StickerID:      event.StickerID,
			PackID:         event.PackID,
		}
		if item.Status == 0 {
			item.Status = int8(domainmessage.Normal)
		}
		if event.ClientMsgID != "" {
			clientMsgID := event.ClientMsgID
			item.ClientMessageID = &clientMsgID
		}
		result = append(result, item)
	}

	return result
}

// messageAppDTOsToEvents 将应用层消息 DTO 转换为内部事件，用于回填房间近期消息缓存。
// 内部事件可以携带后端投递所需的路由信息，但这些字段会在 WebSocket 编码时被过滤。
func messageAppDTOsToEvents(messages []MessageDTO, conversationID, roomID string) []messaging.MessageEvent {
	if len(messages) == 0 {
		return []messaging.MessageEvent{}
	}

	events := make([]messaging.MessageEvent, 0, len(messages))
	for _, message := range messages {
		if message.MessageID == "" || message.Seq <= 0 {
			continue
		}

		resolvedID := message.ConversationID
		if resolvedID == "" {
			resolvedID = conversationID
		}

		clientMsgID := ""
		if message.ClientMessageID != nil {
			clientMsgID = *message.ClientMessageID
		}

		events = append(events, messaging.MessageEvent{
			MessageID:      message.MessageID,
			ConversationID: resolvedID,
			SenderID:       message.SenderID,
			RecvID:         roomID,
			Seq:            message.Seq,
			MessageType:    message.Type,
			Content:        message.Content,
			SendTime:       message.SendTime,
			ClientMsgID:    clientMsgID,
			Status:         message.Status,
			AttachmentID:   message.AttachmentID,
			Width:          message.Width,
			Height:         message.Height,
			DurationMs:     message.DurationMs,
			StickerID:      message.StickerID,
			PackID:         message.PackID,
		})
	}

	return events
}
