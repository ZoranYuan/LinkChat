package message

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"log"
	"math"
	"sort"
	"time"

	roomunreadcache "IM_backend/internal/application/ports/cache/agent"
	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	domainconversation "IM_backend/internal/domain/conversation"
)

// 通过游标的方式来获取历史记录
func (ma *MessageApplication) GetHistoryMessages(
	ctx context.Context,
	conversationID string,
	userID string,
	limit int,
	cursor int64,
) ([]MessageDTO, int64, bool, error) {
	limit = ma.normalizeHistoryLimit(limit)

	_, err := ma.findAuthorizedConversation(ctx, conversationID, userID)
	if err != nil {
		return nil, -1, false, err
	}

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
		conversationID,
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

func (ma *MessageApplication) GetMessagesBySeqs(
	ctx context.Context,
	conversationID string,
	userID string,
	seqs []int64,
) ([]MessageDTO, error) {
	if len(seqs) == 0 {
		return []MessageDTO{}, nil
	}
	if len(seqs) > ma.config.Message.MaxSyncSeqs {
		return nil, ErrTooManySeqs
	}

	_, err := ma.findAuthorizedConversation(ctx, conversationID, userID)
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

	msgs, err := ma.messageRepository.ListBySeqs(ctx, conversationID, uniqueSeqs)
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
	conversationID string,
	userID string,
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

	_, err := ma.findAuthorizedConversation(ctx, conversationID, userID)
	if err != nil {
		return nil, afterSeq, false, err
	}

	latestSeq, err := ma.conversationRepository.GetConversationSeq(ctx, conversationID)
	if err != nil {
		return nil, afterSeq, false, err
	}

	// 客户端传入的快照水位只能向下收敛，不能超过服务端当前水位。
	if snapshotSeq > latestSeq {
		snapshotSeq = latestSeq
	}
	if snapshotSeq <= afterSeq {
		return []MessageDTO{}, afterSeq, false, nil
	}

	limit = ma.normalizeOfflineLimit(limit)
	messages, err := ma.messageRepository.ListAfterSeqUntil(
		ctx,
		conversationID,
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
	conversationID string,
	userID string,
	afterSeq int64,
) ([]MessageDTO, error) {
	if afterSeq < 0 {
		return nil, ErrMessageSeq
	}

	info, err := ma.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if domainconversation.ConvType(info.ConversationType) != domainconversation.RoomChat {
		return nil, ErrInvalidSyncConversation
	}

	if err := ma.checkConversationAccess(ctx, info, userID); err != nil {
		return nil, err
	}

	cached, err := ma.roomCache.ListMessageAfterSeq(ctx, info.RoomID, afterSeq)
	if err == nil && cached.Covered {
		return messageEventsToAppDTO(cached.Events, conversationID), nil
	}

	resultCh := ma.sf.DoChan("message_sync:"+conversationID, func() (any, error) {
		messages, err := ma.messageRepository.GetHistoryMessage(
			ctx,
			conversationID,
			math.MaxInt64,
			ma.config.Cache.RoomMessages.MaxEntries,
		)
		if err != nil {
			return nil, err
		}

		items := toMessageDTOs(messages)
		sort.Slice(items, func(i, j int) bool {
			return items[i].Seq < items[j].Seq
		})
		if err := ma.fillAttachmentIDs(ctx, items); err != nil {
			return nil, err
		}

		events := messageAppDTOsToEvents(items, conversationID, info.RoomID)
		if err := ma.roomCache.WarmRecentMessageEvents(ctx, info.RoomID, events); err != nil {
			log.Printf("回填消息缓存失败：conversation=%s error=%v", conversationID, err)
		}
		return items, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		items := result.Val.([]MessageDTO)
		messages := make([]MessageDTO, 0, len(items))
		for _, item := range items {
			if item.Seq > afterSeq {
				messages = append(messages, item)
			}
		}
		return messages, nil
	}
}

func (ma *MessageApplication) normalizeHistoryLimit(limit int) int {
	defaultLimit := ma.config.Message.HistoryDefaultLimit
	maxLimit := ma.config.Message.HistoryMaxLimit
	if limit <= 0 || limit > maxLimit {
		return defaultLimit
	}
	return limit
}

func (ma *MessageApplication) normalizeOfflineLimit(limit int) int {
	maxOfflinePageSize := ma.config.Message.OfflinePageSize
	if limit <= 0 || limit > maxOfflinePageSize {
		return maxOfflinePageSize
	}
	return limit
}

func (ma *MessageApplication) findAuthorizedConversation(ctx context.Context, conversationID, userID string) (*conversationcache.ConversationInfo, error) {
	info, err := ma.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if err := ma.checkConversationAccess(ctx, info, userID); err != nil {
		return nil, err
	}
	return info, nil
}

func (ma *MessageApplication) HandleReadMessage(
	ctx context.Context,
	userID string,
	messageID string,
) error {
	message, err := ma.messageRepository.FindMessageByMessageID(ctx, messageID)
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

	conversationID := message.ConversationID

	info, err := ma.conversationApplication.GetConversationInfo(ctx, conversationID)
	if err != nil {
		return err
	}

	if err := ma.checkConversationAccess(ctx, info, userID); err != nil {
		return err
	}
	latestSeq, err := ma.conversationRepository.GetConversationSeq(ctx, conversationID)
	if err != nil {
		return err
	}

	uconv, err := ma.userConversationRepository.FindUserConversationByUserIDAndConversationID(ctx, userID, conversationID)
	if err != nil {
		return ErrConversationNotFound
	}

	if uconv == nil {
		return ErrUserConversationNotFound
	}

	uconv.UpdateReadSeq(lastReadSeq)
	var notify *messaging.MessageReadNoticeEvent

	if lastReadSeq > latestSeq {
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

		if message.SenderID != "" && message.SenderID != userID {
			notify = &messaging.MessageReadNoticeEvent{
				UserID:         userID,
				ConversationID: conversationID,
				LastReadSeq:    lastReadSeq,
				ConvType:       messaging.ConvType(info.ConversationType),
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if notify != nil && ma.userDelivery != nil {
		err := ma.userDelivery.DeliverMessageReadNotice(message.SenderID, *notify)
		if err != nil {
			log.Printf("已读通知推送失败：recipient=%s conversation=%s seq=%d error=%v", message.SenderID, conversationID, lastReadSeq, err)
		}
	}

	return nil
}

// HandleRoomEntry 在用户进入房间时固定个人未读范围，并在同一事务中推进已读水位；快照按用户和房间共享，不区分设备。
func (ma *MessageApplication) HandleRoomEntry(ctx context.Context, userID, roomID string) error {
	if userID == "" || roomID == "" {
		return ErrForbidden
	}
	conv, err := ma.conversationRepository.FindConversationByRoomID(ctx, roomID)
	if err != nil {
		return err
	}
	if conv == nil || conv.ConvType != domainconversation.RoomChat {
		return ErrConversationNotFound
	}
	if err := ma.checkConversationAccess(ctx, conversationcache.FromConversation(conv), userID); err != nil {
		return err
	}

	var snapshot *domainconversation.UserConversation
	if err := ma.txManager.WithinTransaction(ctx, func(tx any) error {
		conversationTx := ma.conversationRepository.WithTx(tx)
		userConversationTx := ma.userConversationRepository.WithTx(tx)
		latestSeq, err := conversationTx.GetConversationSeq(ctx, conv.ConversationID)
		if err != nil {
			return err
		}
		snapshot, err = userConversationTx.CaptureRoomUnreadSnapshot(ctx, userID, conv.ConversationID, latestSeq)
		return err
	}); err != nil {
		return err
	}
	if snapshot == nil || ma.roomUnreadSnapshotCache == nil {
		return nil
	}
	cacheSnapshot := roomunreadcache.RoomUnreadSnapshot{
		RoomID:      roomID,
		UnreadBegin: snapshot.SummaryUnreadBeginSeq,
		UnreadEnd:   snapshot.SummaryUnreadEndSeq,
	}
	if err := ma.roomUnreadSnapshotCache.SetActive(
		ctx,
		userID,
		roomID,
		cacheSnapshot,
		time.Duration(ma.config.Cache.RoomUnreadSnapshot.TTLSeconds)*time.Second,
	); err != nil {
		log.Printf("设置房间未读摘要范围缓存失败：user=%s room=%s err=%v", userID, roomID, err)
	}
	return nil
}
