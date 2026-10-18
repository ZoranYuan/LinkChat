package message

import (
	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	eventbus "IM_backend/internal/application/ports/eventbus"
	idport "IM_backend/internal/application/ports/id"
	outboxport "IM_backend/internal/application/ports/outbox"
	conversationcache "IM_backend/internal/application/ports/persistence/cache/conversation"
	filecache "IM_backend/internal/application/ports/persistence/cache/file"
	messagecache "IM_backend/internal/application/ports/persistence/cache/message"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomunreadcache "IM_backend/internal/application/ports/persistence/cache/summary"
	usercach "IM_backend/internal/application/ports/persistence/cache/user"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	filerepo "IM_backend/internal/application/ports/persistence/repository/file"
	friendrepo "IM_backend/internal/application/ports/persistence/repository/friend"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	userrepo "IM_backend/internal/application/ports/persistence/repository/user"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	"IM_backend/internal/application/ports/realtime"
	objectstorage "IM_backend/internal/application/ports/storage/object"
	roomapp "IM_backend/internal/application/room"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	fileentity "IM_backend/internal/domain/file/entity"
	friendentity "IM_backend/internal/domain/friend/entity"
	friendvo "IM_backend/internal/domain/friend/value_object"
	messageentity "IM_backend/internal/domain/message/entity"
	messagevo "IM_backend/internal/domain/message/value_object"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"unicode/utf8"

	"IM_backend/internal/shared/diagnostics"
	"IM_backend/internal/shared/protocol"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

type MessageApplication struct {
	messageCache                 messagecache.MessageCache
	roomMemberCache              roomcache.RoomMemberCache
	roomCache                    roomcache.RoomCache
	roomApplication              *roomapp.RoomApplication
	roomActivityState            *roomapp.RoomActivityState
	roomUnreadSnapshotCache      roomunreadcache.RoomUnreadSnapshotCache
	messageRepository            messagerepo.MessageRepository
	txManager                    txmanager.TxManager
	userCache                    usercach.UserCache
	userConversationRepository   conversationrepo.UserConversationRepository
	conversationRepository       conversationrepo.ConversationRepository
	conversationApplication      *conversationapp.ConversationApplication
	friendRepository             friendrepo.FriendRepository
	fileRepository               filerepo.FileRepository
	fileCache                    filecache.FileCache
	objectStorage                objectstorage.ObjectStorage
	messageStickerRepository     messagerepo.MessageStickerRepository
	messageAttachmentsRepository messagerepo.MessageAttachmentRepository
	messageOutboxRepository      outboxport.OutboxRepository
	producerNotifier             eventbus.ProducerNotifier
	roomOnlineConnectionCounter  realtime.RoomOnlineConnectionCounter
	userRepository               userrepo.UserRepository
	roomUserRepository           roomrepo.RoomUserRepository
	roomRepository               roomrepo.RoomRepository
	sf                           singleflight.Group
	idGenerator                  idport.Generator
	config                       configs.Config
}

// SetProducerNotifier 在事务提交后唤醒 Producer；定时扫描仍负责兜底。
func (ma *MessageApplication) SetProducerNotifier(notifier eventbus.ProducerNotifier) {
	if ma == nil {
		return
	}
	ma.producerNotifier = notifier
}

// SetRoomOnlineConnectionCounter 提供当前节点在线连接数，用于群消息缓存策略。
func (ma *MessageApplication) SetRoomOnlineConnectionCounter(counter realtime.RoomOnlineConnectionCounter) {
	ma.roomOnlineConnectionCounter = counter
}

func (ma *MessageApplication) SetRoomApplication(application *roomapp.RoomApplication) {
	if ma != nil {
		ma.roomApplication = application
	}
}

func (ma *MessageApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	ma.conversationApplication = application
}

func applyConversationTarget(dto *SendMessageDTO, info *conversationcache.ConversationInfo) error {
	dto.ConversationType = info.ConversationType
	switch conversationvo.ConvType(info.ConversationType) {
	case conversationvo.PrivateChat:
		switch dto.SenderID {
		case info.UserID1:
			dto.ReceiverID = info.UserID2
		case info.UserID2:
			dto.ReceiverID = info.UserID1
		default:
			return ErrForbidden
		}
	case conversationvo.RoomChat:
		dto.ReceiverID = info.RoomID
	default:
		return ErrUnknownConversationType
	}
	return nil
}

// warmRoomMessagesCache 批量统计已提交的群消息并按需回填近期消息缓存。
func (ma *MessageApplication) warmRoomMessagesCache(ctx context.Context, roomID string, events []protocol.MessageEvent) {
	if len(events) == 0 || ma.roomCache == nil || ma.roomRepository == nil {
		return
	}
	var room *roomentity.Room
	var err error
	if ma.roomApplication != nil {
		room, err = ma.roomApplication.GetRoomInfo(ctx, roomID)
		if err == nil && room != nil && room.Status != roomvo.Normal {
			err = roomentity.ErrRoomNotFound
		}
	} else {
		room, err = ma.roomRepository.FindActiveRoom(roomID, int(roomvo.Normal))
	}
	if err != nil || room == nil {
		log.Printf("读取群消息缓存策略失败：room=%s error=%v", roomID, err)
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
		log.Printf("批量回填已提交群消息缓存失败：room=%s count=%d error=%v", roomID, len(events), err)
	}
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
	roomActivityState *roomapp.RoomActivityState,
	userCache usercach.UserCache,
	txManager txmanager.TxManager,
	userConversationRepository conversationrepo.UserConversationRepository,
	conversationRepository conversationrepo.ConversationRepository,
	messageOutboxRepository outboxport.OutboxRepository,
	friendRepository friendrepo.FriendRepository,
	fileRepository filerepo.FileRepository,
	fileCache filecache.FileCache,
	objectStorage objectstorage.ObjectStorage,
	messageStickerRepository messagerepo.MessageStickerRepository,
	userRepository userrepo.UserRepository,
	messageRepository messagerepo.MessageRepository,
	roomUserRepository roomrepo.RoomUserRepository,
	roomRepository roomrepo.RoomRepository,
	idGenerator idport.Generator,
	config configs.Config,
	roomUnreadSnapshotCache roomunreadcache.RoomUnreadSnapshotCache,
	messageAttachmentsRepository messagerepo.MessageAttachmentRepository,

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
		userCache:                    userCache,
		txManager:                    txManager,
		messageOutboxRepository:      messageOutboxRepository,
		messageRepository:            messageRepository,
		conversationRepository:       conversationRepository,
		userConversationRepository:   userConversationRepository,
		userRepository:               userRepository,
		roomRepository:               roomRepository,
		roomUserRepository:           roomUserRepository,
		friendRepository:             friendRepository,
		fileRepository:               fileRepository,
		fileCache:                    fileCache,
		objectStorage:                objectStorage,
		messageStickerRepository:     messageStickerRepository,
		idGenerator:                  idGenerator,
		config:                       config,
		messageAttachmentsRepository: messageAttachmentsRepository,
	}
	return app
}

func (ma *MessageApplication) HandleReadMessage(
	ctx context.Context,
	userId string,
	messageId string,
) error {
	message, err := ma.messageRepository.FindByMessageID(ctx, messageId)
	if err != nil {
		return err
	}
	if message == nil {
		return ErrMessageNotFound
	}

	lastReadSeq := message.Seq
	if lastReadSeq <= 0 {
		return ErrMessageSeq
	}

	conversationId := message.ConversationId

	info, err := ma.conversationApplication.GetConversationInfo(ctx, conversationId)
	if err != nil {
		return err
	}

	conv := info.Conversation()
	canAccess, err := ma.canAccessConversation(ctx, conv, userId)
	if err != nil {
		return err
	}
	if !canAccess {
		return ErrForbidden
	}
	conv.LatestSeq, err = ma.conversationRepository.GetConversationSeq(ctx, conversationId)
	if err != nil {
		return err
	}

	uconv, err := ma.userConversationRepository.GetUserConversation(ctx, userId, conversationId)
	if err != nil {
		return ErrConversationNotFound
	}

	if uconv == nil {
		return ErrUserConversationNotFound
	}

	oldLastReadSeq := uconv.LastReadSeq
	uconv.UpdateReadSeq(lastReadSeq)
	outboxCreated := false

	if lastReadSeq > conv.LatestSeq {
		return ErrMessageSeq
	}

	if err := ma.txManager.WithinTransaction(ctx, func(tx any) error {
		advanced, err := ma.userConversationRepository.WithTx(tx).AdvanceReadSeq(ctx, uconv)
		if err != nil {
			return err
		}
		if !advanced {
			return nil
		}

		if message.SenderId != "" && message.SenderId != userId {
			readEvent := protocol.MessageReadCommittedEvent{
				ReaderId:        userId,
				RecipientUserId: message.SenderId,
				ConversationId:  conversationId,
				LastReadSeq:     lastReadSeq,
				ConvType:        protocol.ConvType(conv.Convtype),
			}

			payload, err := json.Marshal(readEvent)
			if err != nil {
				return err
			}

			outbox := &outboxport.Entry{
				EventType:    string(protocol.EventReadMessageCommitted),
				PartitionKey: userId + ":" + conversationId,
				Payload:      payload,
			}
			outboxCreated = true
			return ma.messageOutboxRepository.WithTx(tx).Create(ctx, outbox)
		}
		return nil
	}); err != nil {
		return err
	}
	if outboxCreated {
		ma.notifyProducer()
	}

	if conv.Convtype != conversationvo.RoomChat {
		return nil
	}
	fromSeq := oldLastReadSeq + 1
	snapshot := roomunreadcache.RoomUnreadSnapshot{
		RoomID:  conv.RoomId,
		FromSeq: fromSeq,
		ToSeq:   lastReadSeq,
	}
	if err := ma.roomUnreadSnapshotCache.SetActive(
		ctx,
		userId,
		conv.RoomId,
		snapshot,
		time.Duration(ma.config.Cache.RoomUnreadSnapshot.TTLSeconds)*time.Second,
	); err != nil {
		log.Printf("设置房间未读摘要范围缓存失败：user=%s room=%s err=%v", userId, conv.RoomId, err)
	}

	return nil
}

func (ma *MessageApplication) normalizeMediaDTO(ctx context.Context, dto *SendMessageDTO) error {
	if dto == nil {
		return nil
	}
	switch messagevo.CType(dto.Type) {
	case messagevo.Image, messagevo.Video, messagevo.File:
		if dto.FileID == "" || ma.fileRepository == nil {
			return fmt.Errorf("文件引用不能为空")
		}
		file, err := ma.findUploadedFile(ctx, dto.FileID, dto.SenderID)
		if err != nil {
			return err
		}
		if file == nil {
			return fmt.Errorf("文件不存在或不可用：%s", dto.FileID)
		}

		// 文件名、大小和 MIME 以服务端文件记录为准；客户端媒体元数据全部忽略。
		dto.FileName = file.FileName
		dto.FileSize = file.Size
		dto.MimeType = file.ContentType
		dto.Content = file.FileName
		dto.Width = 0
		dto.Height = 0
		dto.DurationMs = nil

		switch messagevo.CType(dto.Type) {
		case messagevo.Image:
			if !strings.HasPrefix(strings.ToLower(file.ContentType), "image/") {
				return fmt.Errorf("图片消息引用的文件类型不匹配")
			}
		case messagevo.Video:
			if !strings.HasPrefix(strings.ToLower(file.ContentType), "video/") {
				return fmt.Errorf("视频消息引用的文件类型不匹配")
			}
		}
	case messagevo.Sticker:
		if dto.StickerID == "" {
			return fmt.Errorf("表情内容不能为空")
		}
	}
	return nil
}

// findUploadedFile 优先读取按 fileId 预热的文件缓存，缓存未命中或不满足
// 当前用户归属校验时再回源数据库。事务内仍会通过 FOR UPDATE 再次确认，
// 防止文件清理与发送消息并发导致引用到已删除文件。
func (ma *MessageApplication) findUploadedFile(ctx context.Context, fileID, uploaderID string) (*fileentity.File, error) {
	if ma.fileCache != nil {
		cached, cacheErr := ma.fileCache.GetFileMetadata(ctx, fileID)
		if cacheErr != nil {
			log.Printf("读取文件元数据缓存失败: fileId=%s err=%v", fileID, cacheErr)
		} else if cached != nil &&
			cached.Status == fileentity.FileStatusUploaded &&
			cached.UploaderId == uploaderID {
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
		_ = ma.fileCache.SetFileMetadata(ctx, file, ma.fileMetadataCacheTTL())
	}
	return file, nil
}

func (ma *MessageApplication) fileMetadataCacheTTL() time.Duration {
	if ma.config.Storage.MinIO.CacheTTLSeconds > 0 {
		return time.Duration(ma.config.Storage.MinIO.CacheTTLSeconds) * time.Second
	}
	return 24 * time.Hour
}

func (ma *MessageApplication) buildAttachment(dto *SendMessageDTO, messageId string) (*messageentity.MessageAttachment, error) {
	if ma.messageAttachmentsRepository == nil {
		return nil, fmt.Errorf("消息附件仓储未配置")
	}
	attachmentId, err := ma.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	dto.AttachmentID = attachmentId
	attachmentTTL := ma.config.Message.AttachmentTTLSeconds
	if attachmentTTL <= 0 {
		return nil, errors.New("消息附件有效期配置无效")
	}
	now := time.Now().UnixMilli()
	return messageentity.NewMessageAttachment(
		attachmentId,
		messageId,
		dto.ConversationID,
		dto.FileID,
		int8(dto.Type),
		now+attachmentTTL*int64(time.Second/time.Millisecond),
		now,
	), nil
}

func (ma *MessageApplication) buildMediaWriter(dto *SendMessageDTO, messageId string) (func(context.Context, any) error, error) {
	if dto == nil {
		return nil, nil
	}

	switch messagevo.CType(dto.Type) {
	case messagevo.Image, messagevo.File, messagevo.Video:
		if dto.FileID == "" {
			return nil, fmt.Errorf("媒体文件不能为空")
		}
		attachment, err := ma.buildAttachment(dto, messageId)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, tx any) error {
			return ma.messageAttachmentsRepository.WithTx(tx).Create(ctx, attachment)
		}, nil
	case messagevo.Sticker:
		if ma.messageStickerRepository == nil {
			return nil, fmt.Errorf("消息表情仓储未配置")
		}
		if dto.StickerID == "" {
			return nil, fmt.Errorf("表情内容不能为空")
		}
		item := messageentity.NewMessageSticker(
			messageId,
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

func (ma *MessageApplication) isRoomConvMember(ctx context.Context, userID, roomID string) error {
	key := "room_member:" + roomID + ":" + userID
	// 使用 sf 来减少房间内成员频繁发送消息时造成缓存的频繁查询
	resultCh := ma.sf.DoChan(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			3*time.Second,
		)
		defer cancel()

		var state *roomcache.MemberState
		var hit bool
		var cacheErr error
		if ma.roomMemberCache != nil {
			state, hit, cacheErr = ma.roomMemberCache.GetMember(ctx, roomID, userID)
		}
		if cacheErr == nil && hit {
			if state == nil || state.Status == roomvo.Left {
				return nil, ErrNotRoomMember
			}
			if state.Status == roomvo.BeKicked {
				return nil, ErrForbidden
			}
			if state.Status != roomvo.Activate && state.Status != roomvo.BeMuted {
				return nil, ErrNotRoomMember
			}
			return state, nil
		}

		member, err := ma.roomUserRepository.GetRelationByIDs(userID, roomID)
		if err != nil {
			if errors.Is(err, roomentity.ErrMemberNotFound) {
				return nil, ErrNotRoomMember
			}
			return nil, err
		}
		if member.Status != roomvo.Activate && member.Status != roomvo.BeMuted {
			if ma.roomMemberCache != nil {
				_, _ = ma.roomMemberCache.SetMemberIfVersionGreater(ctx, roomID, userID, &roomcache.MemberState{
					Status: member.Status, Role: member.Role,
					MuteUntil: member.MuteUtil, Version: member.Version,
				})
			}
			return nil, ErrNotRoomMember
		}

		state = &roomcache.MemberState{
			Status:    member.Status,
			Role:      member.Role,
			MuteUntil: member.MuteUtil,
			Version:   member.Version,
		}
		if ma.roomMemberCache != nil {
			_, _ = ma.roomMemberCache.SetMemberIfVersionGreater(ctx, roomID, userID, state)
		}
		return state, nil
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return result.Err
		}

		state, ok := result.Val.(*roomcache.MemberState)
		if !ok || state == nil {
			return ErrNotRoomMember
		}

		if state.Status == roomvo.BeKicked {
			return ErrForbidden
		}
		if state.Status == roomvo.BeMuted && (state.MuteUntil == nil || *state.MuteUntil > time.Now().UnixMilli()) {
			return ErrForbidden
		}
	}

	return nil
}

func (ma *MessageApplication) authorizeSendBatch(ctx context.Context, info *conversationcache.ConversationInfo, dtos []SendMessageDTO) ([]error, error) {
	results := make([]error, len(dtos))
	if len(dtos) == 0 {
		return results, nil
	}
	switch conversationvo.ConvType(info.ConversationType) {
	case conversationvo.RoomChat:
		if ma.roomUserRepository == nil {
			return nil, errors.New("群成员仓储未配置")
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
		members, err := ma.roomUserRepository.ListRelationsByRoomAndUserIDs(ctx, info.RoomID, userIDs)
		if err != nil {
			return nil, err
		}
		byUserID := make(map[string]*roomentity.RoomUser, len(members))
		for _, member := range members {
			if member != nil {
				byUserID[member.UserId] = member
			}
		}
		now := time.Now().UnixMilli()
		for i, dto := range dtos {
			member := byUserID[dto.SenderID]
			switch {
			case member == nil:
				results[i] = ErrNotRoomMember
			case member.Status == roomvo.BeKicked:
				results[i] = ErrForbidden
			case member.Status == roomvo.Activate:
			case member.Status == roomvo.BeMuted && member.MuteUtil != nil && *member.MuteUtil <= now:
			default:
				results[i] = ErrNotRoomMember
				if member.Status == roomvo.BeMuted {
					results[i] = ErrForbidden
				}
			}
		}
	case conversationvo.PrivateChat:
		if ma.friendRepository == nil {
			return nil, errors.New("好友关系仓储未配置")
		}
		pairs := []friendrepo.RelationPair{
			{UserID: info.UserID1, FriendUserID: info.UserID2},
			{UserID: info.UserID2, FriendUserID: info.UserID1},
		}
		relations, err := ma.friendRepository.FindRelationsByPairs(ctx, pairs)
		if err != nil {
			return nil, err
		}
		byPair := make(map[friendrepo.RelationPair]*friendentity.Friend, len(relations))
		for _, relation := range relations {
			if relation != nil {
				byPair[friendrepo.RelationPair{UserID: relation.UserId, FriendUserID: relation.FriendUserId}] = relation
			}
		}
		first := byPair[pairs[0]]
		second := byPair[pairs[1]]
		if first == nil || second == nil || first.Status != friendvo.Friend || second.Status != friendvo.Friend {
			for i := range results {
				results[i] = ErrNotFriends
			}
		}
	default:
		return nil, ErrUnknownConversationType
	}
	return results, nil
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
func (ma *MessageApplication) validateMessageCType(dto SendMessageDTO) error {
	if dto.SenderID == "" || dto.ReceiverID == "" {
		return errors.New("发送方或接收方不能为空")
	}

	switch messagevo.CType(dto.Type) {
	case messagevo.Text:
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
	case messagevo.Image:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" ||
			dto.PackID != "" {
			return errors.New("图片消息包含非法字段")
		}
	case messagevo.Video:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" || dto.PackID != "" {
			return errors.New("视频消息包含非法字段")
		}
	case messagevo.File:
		if err := ma.validateFileReference(dto); err != nil {
			return err
		}

		if dto.StickerID != "" ||
			dto.PackID != "" {
			return errors.New("文件消息包含非法字段")
		}

	case messagevo.Sticker:
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
		message     *messageentity.Message
		event       protocol.MessageEvent
		mediaWriter func(context.Context, any) error
		requestHash string
		username    string
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
				acks[i] = &MessageAckDTO{ClientMessageID: dtos[i].ClientMessageID, Status: string(protocol.AckStatusFailed), Error: cause.Error()}
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
			acks[i] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		candidates = append(candidates, sendCandidate{index: i, dto: dto})
	}

	authorizationDTOs := make([]SendMessageDTO, len(candidates))
	for i := range candidates {
		authorizationDTOs[i] = candidates[i].dto
	}
	authorizationErrors, err := ma.authorizeSendBatch(ctx, info, authorizationDTOs)
	if err != nil {
		fillUnresolvedAcks(err)
		return acks, err
	}
	authorized := make([]sendCandidate, 0, len(candidates))
	for candidateIndex, candidate := range candidates {
		dto := candidate.dto
		if authErr := authorizationErrors[candidateIndex]; authErr != nil {
			acks[candidate.index] = &MessageAckDTO{
				ClientMessageID: dto.ClientMessageID,
				Status:          string(protocol.AckStatusFailed),
				Error:           authErr.Error(),
			}
			continue
		}
		authorized = append(authorized, candidate)
	}
	if len(authorized) == 0 {
		return acks, nil
	}

	valid := make([]sendCandidate, 0, len(authorized))
	keys := make([]messagecache.DedupKey, 0, len(authorized))
	seenKeys := make(map[messagecache.DedupKey]struct{}, len(authorized))
	for _, candidate := range authorized {
		if err := ma.validateMessageCType(candidate.dto); err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: candidate.dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		requestHash, err := buildMessageRequestHash(candidate.dto)
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: candidate.dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		candidate.requestHash = requestHash
		valid = append(valid, candidate)
		if candidate.dto.ClientMessageID != "" {
			key := messagecache.DedupKey{SenderID: candidate.dto.SenderID, ClientMsgID: candidate.dto.ClientMessageID}
			if _, exists := seenKeys[key]; !exists {
				seenKeys[key] = struct{}{}
				keys = append(keys, key)
			}
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
					Status:          string(protocol.AckStatusFailed),
					Error:           messageentity.ErrClientMessageConflict.Error(),
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
					Status:          string(protocol.AckStatusSent),
				}
				continue
			}
		}
		if err := ma.normalizeMediaDTO(ctx, &dto); err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		messageID, err := ma.idGenerator.Generate()
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		message := messageentity.NewMessage(messageID, conversationID, dto.SenderID, 0, messagevo.CType(dto.Type), dto.Content)
		if dto.ClientMessageID != "" {
			clientMessageID := dto.ClientMessageID
			message.ClientMsgId = &clientMessageID
		}
		message.RequestHash = candidate.requestHash
		mediaWriter, err := ma.buildMediaWriter(&dto, messageID)
		if err != nil {
			acks[candidate.index] = &MessageAckDTO{ClientMessageID: dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
			continue
		}
		prepared = append(prepared, preparedMessage{
			index:       candidate.index,
			dto:         dto,
			message:     message,
			mediaWriter: mediaWriter,
			requestHash: candidate.requestHash,
			username:    ma.getUsername(dto.SenderID),
		})
	}

	if len(prepared) == 0 {
		return acks, nil
	}
	transactionStartedAt := time.Now()
	var sequenceDuration time.Duration
	err = ma.txManager.WithinTransaction(ctx, func(tx any) error {
		msgRepo := ma.messageRepository.WithTx(tx)
		convRepo := ma.conversationRepository.WithTx(tx)
		userConvRepo := ma.userConversationRepository.WithTx(tx)
		outboxRepo := ma.messageOutboxRepository.WithTx(tx)

		for i := range prepared {
			item := &prepared[i]
			if messagevo.CType(item.dto.Type) != messagevo.Image &&
				messagevo.CType(item.dto.Type) != messagevo.Video &&
				messagevo.CType(item.dto.Type) != messagevo.File {
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
			item.dto.FileName = file.FileName
			item.dto.FileSize = file.Size
			item.dto.MimeType = file.ContentType
			item.dto.Content = file.FileName
			item.message.Content = file.FileName
		}

		messages := make([]*messageentity.Message, len(prepared))
		for i := range prepared {
			messages[i] = prepared[i].message
		}
		sequenceStartedAt := time.Now()
		firstSeq, reserveErr := convRepo.ReserveSequenceRange(
			ctx,
			conversationID,
			len(messages),
			messages[len(messages)-1].MessageId,
		)
		sequenceDuration = time.Since(sequenceStartedAt)
		if reserveErr != nil {
			if errors.Is(reserveErr, conversationentity.ErrConversationNotCreated) {
				return ErrConversationNotFound
			}
			return ErrConversationSequenceUpdate
		}

		outboxes := make([]*outboxport.Entry, 0, len(prepared))
		updates := make([]conversationrepo.ReadSeqUpdate, 0, len(prepared))
		for i := range prepared {
			item := &prepared[i]
			item.message.Seq = firstSeq + int64(i)
			event := protocol.MessageEvent{
				MessageId:      item.message.MessageId,
				ConversationId: conversationID,
				SenderId:       item.dto.SenderID,
				SenderUsername: item.username,
				RecvId:         item.dto.ReceiverID,
				ConvType:       protocol.ConvType(item.dto.ConversationType),
				CType:          item.dto.Type,
				Content:        item.message.Content,
				SendTime:       item.message.SendTime,
				ClientMsgId:    item.dto.ClientMessageID,
				Status:         int8(item.message.Status),
				Seq:            item.message.Seq,
				AttachmentId:   item.dto.AttachmentID,
				Width:          item.dto.Width,
				Height:         item.dto.Height,
				DurationMs:     item.dto.DurationMs,
				StickerId:      item.dto.StickerID,
				PackId:         item.dto.PackID,
			}
			item.event = event
			outbox, outboxErr := messageOutboxEntry(
				protocol.EventTypeSendMessage,
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
			updates = append(updates, conversationrepo.ReadSeqUpdate{
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
	diagnostics.Logf("stage=mysql_message_transaction conversation_id=%s messages=%d reserve_seq_us=%d transaction_us=%d error=%v",
		conversationID, len(prepared), sequenceDuration.Microseconds(), time.Since(transactionStartedAt).Microseconds(), err)
	if err != nil {
		for _, item := range prepared {
			acks[item.index] = &MessageAckDTO{ClientMessageID: item.dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: err.Error()}
		}
		if errors.Is(err, messageentity.ErrDuplicateClientMessage) && len(prepared) == 1 {
			item := prepared[0]
			existing, findErr := ma.messageRepository.FindByClientMsgID(ctx, item.dto.SenderID, item.dto.ClientMessageID)
			if findErr == nil && existing != nil && sameClientMessage(item.dto, existing, item.requestHash) {
				acks[item.index] = ma.existingMessageResult(ctx, item.dto, existing)
				return acks, nil
			}
			if findErr == nil && existing != nil {
				acks[item.index] = &MessageAckDTO{ClientMessageID: item.dto.ClientMessageID, Status: string(protocol.AckStatusFailed), Error: messageentity.ErrClientMessageConflict.Error()}
				return acks, nil
			}
			if findErr != nil {
				return acks, findErr
			}
		}
		return acks, err
	}

	dedupWrites := make([]messagecache.DedupEntryWrite, 0, len(prepared))
	roomEvents := make([]protocol.MessageEvent, 0, len(prepared))
	for i := range prepared {
		item := prepared[i]
		acks[item.index] = &MessageAckDTO{
			ClientMessageID: item.dto.ClientMessageID,
			ConversationID:  conversationID,
			MessageID:       item.message.MessageId,
			Seq:             item.message.Seq,
			AttachmentID:    item.dto.AttachmentID,
			SendTime:        item.message.SendTime,
			Status:          string(protocol.AckStatusSent),
		}
		if item.event.ConvType == protocol.RoomChat {
			roomEvents = append(roomEvents, item.event)
		}
		if item.dto.ClientMessageID != "" && ma.messageCache != nil {
			dedupWrites = append(dedupWrites, messagecache.DedupEntryWrite{
				Key: messagecache.DedupKey{SenderID: item.dto.SenderID, ClientMsgID: item.dto.ClientMessageID},
				Entry: messagecache.DedupEntry{
					MessageID:      item.message.MessageId,
					RequestHash:    item.requestHash,
					ConversationID: conversationID,
					Seq:            item.message.Seq,
					AttachmentID:   item.dto.AttachmentID,
					SendTime:       item.message.SendTime,
					SenderUsername: item.username,
				},
			})
		}
	}
	if len(roomEvents) > 0 {
		ma.warmRoomMessagesCache(ctx, info.RoomID, roomEvents)
	}
	if len(dedupWrites) > 0 {
		if err := ma.messageCache.SetDedupEntries(ctx, dedupWrites, 5*time.Minute); err != nil {
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
	existing *messageentity.Message,
) *MessageAckDTO {
	if existing == nil {
		return nil
	}

	attachmentID := ""
	if ma.messageAttachmentsRepository != nil {
		attachments, err := ma.messageAttachmentsRepository.BatchGetByMessageIDs(ctx, []string{existing.MessageId}, false)
		if err == nil {
			if attachment := attachments[existing.MessageId]; attachment != nil {
				attachmentID = attachment.AttachmentId
			}
		}
	}

	return &MessageAckDTO{
		ClientMessageID: dto.ClientMessageID,
		ConversationID:  existing.ConversationId,
		MessageID:       existing.MessageId,
		Seq:             existing.Seq,
		AttachmentID:    attachmentID,
		SendTime:        existing.SendTime,
		Status:          string(protocol.AckStatusSent),
	}
}

type messageRequestFingerprint struct {
	ConversationID string `json:"conversationId"`
	CType          int    `json:"cType"`
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
	switch messagevo.CType(dto.Type) {
	case messagevo.Image, messagevo.Video, messagevo.File:
		// 媒体消息的身份由 FileId 表示，不依赖文件名等数据库元数据。
		content = ""
		dto.Width = 0
		dto.Height = 0
		dto.DurationMs = nil
	}

	payload, err := json.Marshal(messageRequestFingerprint{
		ConversationID: dto.ConversationID,
		CType:          dto.Type,
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

func sameClientMessage(dto SendMessageDTO, existing *messageentity.Message, requestHash string) bool {
	if existing == nil {
		return false
	}
	if existing.RequestHash != "" {
		return existing.RequestHash == requestHash
	}
	return existing.ConversationId == dto.ConversationID &&
		existing.Type == messagevo.CType(dto.Type) &&
		existing.Content == dto.Content
}

func messageOutboxEntry(eventType, partitionKey string, event any) (*outboxport.Entry, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return &outboxport.Entry{EventType: eventType, PartitionKey: partitionKey, Payload: payload}, nil
}

// 通过游标的方式来获取历史记录
func (ma *MessageApplication) GetHistoryMessages(
	ctx context.Context,
	conversationId string,
	userId string,
	limit int,
	cursor int64,
) ([]MessageDTO, int64, bool, error) {
	limit = ma.normalizeHistoryLimit(limit)

	resolvedConversationID, _, err := ma.resolveHistoryConversation(ctx, conversationId, userId)
	if err != nil {
		return nil, -1, false, err
	}
	conversationId = resolvedConversationID

	var maxSeq int64
	switch cursor {
	case 0:
		// 表示从最新的消息开始读取
		maxSeq = math.MaxInt64
	case -1:
		// 没有消息了
		return []MessageDTO{}, -1, false, nil
	default:
		maxSeq = cursor
	}

	msgs, err := ma.messageRepository.GetHistoryMessage(
		ctx,
		conversationId,
		maxSeq,
		limit+1,
	)
	if err != nil {
		return nil, -1, false, err
	}

	var nextCursor int64 = -1
	var hasMore bool = len(msgs) > limit

	if hasMore {
		msgs = msgs[:limit]
		nextCursor = msgs[len(msgs)-1].Seq
	}

	msgsApp := toMessageDTOs(msgs)
	if err := ma.fillAttachmentIDs(ctx, msgsApp); err != nil {
		return nil, -1, false, err
	}

	sort.Slice(msgsApp, func(i, j int) bool {
		return msgsApp[i].Seq < msgsApp[j].Seq
	})

	return msgsApp, nextCursor, hasMore, nil
}

// GetMessagesBySeqs 按指定 seq 批量读取消息，用于修复客户端本地消息索引中的缺口。
// 它不是增量同步：seq 可能是离散的，查询结果只返回数据库中实际存在的消息。
func (ma *MessageApplication) GetMessagesBySeqs(
	ctx context.Context,
	conversationId string,
	userId string,
	seqs []int64,
) ([]MessageDTO, error) {
	if len(seqs) == 0 {
		return []MessageDTO{}, nil
	}
	if len(seqs) > 500 {
		return nil, ErrTooManySeqs
	}

	resolvedConversationID, _, err := ma.resolveHistoryConversation(ctx, conversationId, userId)
	if err != nil {
		return nil, err
	}

	uniqueSeqs := make([]int64, 0, len(seqs))
	seen := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		if seq <= 0 {
			continue
		}
		if _, ok := seen[seq]; ok {
			continue
		}
		seen[seq] = struct{}{}
		uniqueSeqs = append(uniqueSeqs, seq)
	}
	if len(uniqueSeqs) == 0 {
		return []MessageDTO{}, nil
	}

	msgs, err := ma.messageRepository.ListBySeqs(ctx, resolvedConversationID, uniqueSeqs)
	if err != nil {
		return nil, err
	}

	msgsApp := toMessageDTOs(msgs)
	if err := ma.fillAttachmentIDs(ctx, msgsApp); err != nil {
		return nil, err
	}
	sort.SliceStable(msgsApp, func(i, j int) bool {
		return msgsApp[i].Seq < msgsApp[j].Seq
	})
	return msgsApp, nil
}

// GetOfflineMessages 按固定快照水位分页补齐本地缺失的消息。
func (ma *MessageApplication) GetOfflineMessages(
	ctx context.Context,
	conversationId string,
	userId string,
	afterSeq int64,
	snapshotSeq int64,
	limit int,
) ([]MessageDTO, int64, bool, error) {
	if afterSeq < 0 {
		afterSeq = 0
	}
	if snapshotSeq <= 0 {
		return nil, afterSeq, false, ErrMessageSeq
	}

	resolvedConversationID, conversation, err := ma.resolveHistoryConversation(ctx, conversationId, userId)
	if err != nil {
		return nil, afterSeq, false, err
	}

	// 客户端传入的快照水位只能向下收敛，不能超过服务端当前水位。
	if snapshotSeq > conversation.LatestSeq {
		snapshotSeq = conversation.LatestSeq
	}
	if snapshotSeq <= afterSeq {
		return []MessageDTO{}, afterSeq, false, nil
	}

	limit = ma.normalizeOfflineLimit(limit)
	messages, err := ma.messageRepository.ListAfterSeqUntil(
		ctx,
		resolvedConversationID,
		afterSeq,
		snapshotSeq,
		limit+1,
	)
	if err != nil {
		return nil, afterSeq, false, err
	}

	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}

	nextCursor := afterSeq
	if len(messages) > 0 {
		nextCursor = messages[len(messages)-1].Seq
	}
	if !hasMore {
		// 最后一页确认扫完整个固定快照，推进离线补齐水位。
		nextCursor = snapshotSeq
	}

	result := toMessageDTOs(messages)
	if err := ma.fillAttachmentIDs(ctx, result); err != nil {
		return nil, afterSeq, false, err
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Seq < result[j].Seq
	})
	return result, nextCursor, hasMore, nil
}

func (ma *MessageApplication) SyncMessages(
	ctx context.Context,
	conversationId string,
	userId string,
	afterSeq int64,
) ([]MessageDTO, error) {
	if afterSeq < 0 {
		afterSeq = 0
	}

	resolvedConversationID, conversation, err := ma.resolveHistoryConversation(ctx, conversationId, userId)
	if err != nil {
		return nil, err
	}
	latestSeq := conversation.LatestSeq
	if latestSeq <= afterSeq {
		return []MessageDTO{}, nil
	}

	// 新群消息提交后按策略写入 Redis 的 seq ZSET，优先从缓存同步。缓存未覆盖完整区间或读取失败时回源 MySQL。
	if conversation.Convtype == conversationvo.RoomChat && ma.roomCache != nil {
		page, cacheErr := ma.roomCache.ListMessageAfterSeq(
			ctx,
			conversation.RoomId,
			afterSeq,
			latestSeq,
		)
		if cacheErr != nil {
			log.Printf(
				"读取房间近期消息缓存失败: conversation=%s afterSeq=%d err=%v",
				resolvedConversationID,
				afterSeq,
				cacheErr,
			)
		} else if page.Covered {
			return messageEventsToAppDTO(page.Events, resolvedConversationID), nil
		}
	}

	syncKey := fmt.Sprintf(
		"message-sync-all:%s:%d:%d",
		resolvedConversationID,
		afterSeq,
		latestSeq,
	)

	result, err, _ := ma.sf.Do(syncKey, func() (any, error) {
		msgs, err := ma.messageRepository.ListAfterSeq(
			ctx,
			resolvedConversationID,
			afterSeq,
		)
		if err != nil {
			return nil, err
		}

		filtered := make([]*messageentity.Message, 0, len(msgs))
		for _, msg := range msgs {
			if msg == nil || msg.Seq > latestSeq {
				continue
			}
			filtered = append(filtered, msg)
		}
		msgs = filtered

		msgsApp := toMessageDTOs(msgs)
		if err := ma.fillAttachmentIDs(ctx, msgsApp); err != nil {
			return nil, err
		}
		sort.SliceStable(msgsApp, func(i, j int) bool {
			return msgsApp[i].Seq < msgsApp[j].Seq
		})

		if conversation.Convtype == conversationvo.RoomChat && ma.roomCache != nil {
			events := messageAppDTOsToEvents(msgsApp, resolvedConversationID, conversation.RoomId)
			if err := ma.roomCache.WarmRecentMessageEvents(ctx, conversation.RoomId, events); err != nil {
				log.Printf(
					"回填房间近期消息缓存失败: conversation=%s afterSeq=%d latestSeq=%d err=%v",
					resolvedConversationID,
					afterSeq,
					latestSeq,
					err,
				)
			}
		}

		return msgsApp, nil
	})
	if err != nil {
		return nil, err
	}

	syncResult, ok := result.([]MessageDTO)
	if !ok {
		return nil, errors.New("同步消息结果类型错误")
	}

	return syncResult, nil
}

// messageEventsToAppDTO 将近期消息缓存中的内部事件转换为应用层消息 DTO。
// 转换时补齐会话 ID、状态和附件 ID，供历史或同步接口复用统一的消息响应链路。
func messageEventsToAppDTO(events []protocol.MessageEvent, conversationID string) []MessageDTO {
	if len(events) == 0 {
		return []MessageDTO{}
	}

	result := make([]MessageDTO, 0, len(events))
	for _, event := range events {
		if event.MessageId == "" || event.Seq <= 0 {
			continue
		}

		resolvedID := event.ConversationId
		if resolvedID == "" {
			resolvedID = conversationID
		}

		item := MessageDTO{
			SenderID:       event.SenderId,
			ConversationID: resolvedID,
			MessageID:      event.MessageId,
			Seq:            event.Seq,
			Type:           event.CType,
			Content:        event.Content,
			SendTime:       event.SendTime,
			Status:         event.Status,
			AttachmentID:   event.AttachmentId,
			Width:          event.Width,
			Height:         event.Height,
			DurationMs:     event.DurationMs,
			StickerID:      event.StickerId,
			PackID:         event.PackId,
		}
		if item.Status == 0 {
			item.Status = int8(messagevo.Normal)
		}
		if event.ClientMsgId != "" {
			clientMsgID := event.ClientMsgId
			item.ClientMessageID = &clientMsgID
		}
		result = append(result, item)
	}

	return result
}

// messageAppDTOsToEvents 将应用层消息 DTO 转换为内部事件，用于回填房间近期消息缓存。
// 内部事件可以携带后端投递所需的路由信息，但这些字段会在 WebSocket 编码时被过滤。
func messageAppDTOsToEvents(messages []MessageDTO, conversationID, roomID string) []protocol.MessageEvent {
	if len(messages) == 0 {
		return []protocol.MessageEvent{}
	}

	events := make([]protocol.MessageEvent, 0, len(messages))
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

		events = append(events, protocol.MessageEvent{
			MessageId:      message.MessageID,
			ConversationId: resolvedID,
			SenderId:       message.SenderID,
			RecvId:         roomID,
			Seq:            message.Seq,
			CType:          message.Type,
			Content:        message.Content,
			SendTime:       message.SendTime,
			ClientMsgId:    clientMsgID,
			Status:         message.Status,
			AttachmentId:   message.AttachmentID,
			Width:          message.Width,
			Height:         message.Height,
			DurationMs:     message.DurationMs,
			StickerId:      message.StickerID,
			PackId:         message.PackID,
		})
	}

	return events
}

func (ma *MessageApplication) normalizeHistoryLimit(limit int) int {
	defaultLimit := ma.config.Message.HistoryDefaultLimit
	if defaultLimit <= 0 {
		defaultLimit = 20
	}
	maxLimit := ma.config.Message.HistoryMaxLimit
	if maxLimit <= 0 {
		maxLimit = 30
	}
	if limit <= 0 || limit > maxLimit {
		return defaultLimit
	}
	return limit
}

func (ma *MessageApplication) normalizeOfflineLimit(limit int) int {
	const maxOfflinePageSize = 10
	if limit <= 0 || limit > maxOfflinePageSize {
		return maxOfflinePageSize
	}
	return limit
}

func (ma *MessageApplication) resolveHistoryConversation(
	ctx context.Context,
	conversationId string,
	userId string,
) (string, *conversationentity.Conversation, error) {
	resolvedID := conversationId
	info, err := ma.conversationApplication.GetConversationInfo(ctx, resolvedID)
	if err != nil {
		return "", nil, err
	}

	conv := info.Conversation()
	allowed, err := ma.canAccessConversation(ctx, conv, userId)
	if err != nil {
		return "", nil, err
	}
	if !allowed {
		return "", nil, ErrForbidden
	}
	conv.LatestSeq, err = ma.conversationRepository.GetConversationSeq(ctx, resolvedID)
	if err != nil {
		return "", nil, err
	}

	return resolvedID, conv, nil
}

func (ma *MessageApplication) ListActiveRoomIDs(userID string) ([]string, error) {
	if ma.roomUserRepository == nil {
		return nil, errors.New("房间成员仓储未配置")
	}
	return ma.roomUserRepository.ListActiveRoomIDs(userID)
}

func (ma *MessageApplication) canAccessConversation(
	ctx context.Context,
	conv *conversationentity.Conversation,
	userId string,
) (bool, error) {
	switch conv.Convtype {
	case conversationvo.PrivateChat:
		return conv.UserId1 == userId || conv.UserId2 == userId, nil
	case conversationvo.RoomChat:
		if ma.roomUserRepository == nil {
			return false, ErrForbidden
		}
		err := ma.isRoomConvMember(ctx, userId, conv.RoomId)
		if err != nil {
			if errors.Is(err, roomentity.ErrMemberNotFound) || errors.Is(err, ErrNotRoomMember) {
				return false, nil
			}
			if errors.Is(err, ErrForbidden) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	default:
		return false, ErrUnknownConversationType
	}
}

func (ma *MessageApplication) getUsername(userId string) string {
	if userId == "" || ma.userRepository == nil {
		return ""
	}
	user, err := ma.userRepository.FindByUserID(userId)
	if err != nil || user == nil {
		return ""
	}
	return user.UserName
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
			messages[i].AttachmentID = attachment.AttachmentId
		}
	}
	return nil
}
