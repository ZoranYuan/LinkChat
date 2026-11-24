package read

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"IM_backend/configs"
	eventport "IM_backend/core/application/ports/events"
	inboxport "IM_backend/core/application/ports/inbox"
	realtimeport "IM_backend/core/application/ports/realtime"
	"IM_backend/core/application/ports/transaction"
	domainconversation "IM_backend/core/domain/conversation"
	domainmessage "IM_backend/core/domain/message"
	"IM_backend/core/infra/mq/kafka"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReadReceiptHandler：消费 message.read.receipt，逐条处理。
// 实现 kafka.ManagedHandler：Handle 不调 Complete，由 consumer 统一收口
// 「重试 → 死信 → Complete」。确定性失败用 NonRetryable 包装直接进死信，
// 瞬时失败返回普通 error 触发分区内退避重试。
// ─────────────────────────────────────────────────────────────────────────────

type ReadReceiptHandler struct {
	config                     configs.Config
	inbox                      inboxport.InboxRepository
	txManager                  transaction.Manager
	messageRepository          domainmessage.MessageRepository
	conversationRepository     domainconversation.ConversationRepository
	userConversationRepository domainconversation.UserConversationRepository
	userDelivery               realtimeport.UserDelivery
}

func NewReadReceiptHandler(
	config configs.Config,
	inbox inboxport.InboxRepository,
	txManager transaction.Manager,
	messageRepository domainmessage.MessageRepository,
	conversationRepository domainconversation.ConversationRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	userDelivery realtimeport.UserDelivery,
) *ReadReceiptHandler {
	return &ReadReceiptHandler{
		config:                     config,
		inbox:                      inbox,
		txManager:                  txManager,
		messageRepository:          messageRepository,
		conversationRepository:     conversationRepository,
		userConversationRepository: userConversationRepository,
		userDelivery:               userDelivery,
	}
}

func (h *ReadReceiptHandler) Topic() string { return h.config.Kafka.Topics.MessageReadReceipt }

// Managed 标记本 handler 由 consumer 统一管理 Complete / 重试 / 死信。
func (h *ReadReceiptHandler) Managed() {}

// Handle 处理一条已读事件。约定：成功返回 nil，失败返回 error，绝不自行 Complete。
func (h *ReadReceiptHandler) Handle(ctx context.Context, item *kafka.MessageItem) error {
	var e eventport.ReadReceiptEvent
	if err := json.Unmarshal(item.Record.Value, &e); err != nil || e.UserID == "" || e.MessageID == "" {
		return eventport.NonRetryable(fmt.Errorf("已读事件毒消息：userId=%s messageId=%s err=%v",
			e.UserID, e.MessageID, err))
	}

	// ① 反查消息：瞬时失败 return err 触发重试。
	msg, err := h.messageRepository.FindMessageByMessageID(ctx, e.MessageID)
	if err != nil {
		return err
	}
	if msg == nil || msg.Seq <= 0 {
		// 消息不存在（已删）——确定性失败，重试无意义，直接死信。
		return eventport.NonRetryable(fmt.Errorf("已读事件消息不存在：message=%s", e.MessageID))
	}

	conv, err := h.conversationRepository.FindConversationByID(ctx, msg.ConversationID)
	if err != nil {
		return err
	}
	if conv == nil {
		return eventport.NonRetryable(fmt.Errorf("已读事件会话不存在：conv=%s", msg.ConversationID))
	}

	// ② seq 上限校验：拒绝异常数据把水位推到未来。确定性失败。
	latestSeq, err := h.conversationRepository.GetConversationSeq(ctx, msg.ConversationID)
	if err != nil {
		return err
	}
	if msg.Seq > latestSeq {
		return eventport.NonRetryable(fmt.Errorf("已读事件 seq 越界：message=%s seq=%d latest=%d",
			e.MessageID, msg.Seq, latestSeq))
	}

	eventID := readEventID(e)
	hash := fmt.Sprintf("%s:%d", msg.ConversationID, msg.Seq)

	// ③ 三态幂等 + 写水位，同一事务原子。
	var notify bool
	err = h.txManager.WithinTransaction(ctx, func(tx any) error {
		inboxTx := h.inbox.WithTx(tx)

		existing, err := inboxTx.FindByEventIDs(ctx, eventport.KafkaEventReadReceipt, []string{eventID})
		if err != nil {
			return err
		}
		old, found := existing[eventID]
		switch {
		case found && old == hash:
			return nil // duplicate：合法重投
		case found && old != hash:
			// conflict：确定性失败但无害，告警后进死信留痕，不推进水位。
			return eventport.NonRetryable(fmt.Errorf("已读事件 inbox 冲突：eventID=%s old=%s new=%s",
				eventID, old, hash))
		}

		if err := h.userConversationRepository.WithTx(tx).UpdateReadSeqBatch(ctx,
			[]domainconversation.ReadSeqUpdate{{
				UserID: e.UserID, ConversationID: msg.ConversationID, LastReadSeq: msg.Seq,
			}}); err != nil {
			return err
		}
		if err := inboxTx.CreateBatch(ctx, []inboxport.CreateRequest{{
			EventType: eventport.KafkaEventReadReceipt, EventID: eventID, RequestHash: hash,
		}}); err != nil {
			return err
		}
		notify = msg.SenderID != "" && msg.SenderID != e.UserID
		return nil
	})
	if err != nil {
		// 事务内返回的 NonRetryable 需要透传（保留"不重试"语义）；
		// 其余（DB 抖动等）是瞬时错误，交给 consumer 重试。
		return err
	}

	// ④ 事务外推回执，失败只记日志，不影响已落库水位。
	if notify && h.userDelivery != nil {
		ev := eventport.MessageReadNoticeEvent{
			UserID:         e.UserID,
			ConversationID: msg.ConversationID,
			LastReadSeq:    msg.Seq,
			ConvType:       conv.ConvType,
		}
		if err := h.userDelivery.DeliverMessageReadNotice(msg.SenderID, ev); err != nil {
			log.Printf("已读回执推送失败：recipient=%s reader=%s conv=%s seq=%d err=%v",
				msg.SenderID, e.UserID, msg.ConversationID, msg.Seq, err)
		}
	}
	return nil // consumer 收到 nil 后 Complete
}

// readEventID 稳定幂等键：同一读者重复已读同一条消息只处理一次。
func readEventID(e eventport.ReadReceiptEvent) string {
	return e.UserID + ":" + e.MessageID
}
