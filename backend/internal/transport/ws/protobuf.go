package ws

import (
	realtimews "IM_backend/internal/infrastructure/realtime/websocket"
	"IM_backend/internal/shared/protocol"
	wspb "IM_backend/internal/transport/ws/pb"
)

// encodeWebSocketPayload 将业务事件交给统一的 WebSocket protobuf 编码器处理。
func encodeWebSocketPayload(op string, payload []byte) ([]byte, error) {
	return realtimews.EncodePayload(op, payload)
}

// messageAckToPB 将业务层生成的消息 ACK 转换为 WebSocket protobuf 响应。
func messageAckToPB(event protocol.MessageAckEvent) *wspb.MessageAck {
	return &wspb.MessageAck{
		ClientMsgId:  event.ClientMsgId,
		MessageId:    event.MessageId,
		Status:       string(event.Status),
		Extra:        event.Extra,
		SendTime:     event.SendTime,
		AttachmentId: event.AttachmentId,
	}
}

// messageReadAckEventToPB 将已读回执事件转换为 WebSocket protobuf 响应。
func messageReadAckEventToPB(event protocol.MessageReadAckEvent) *wspb.MessageReadAckEvent {
	return &wspb.MessageReadAckEvent{
		UserId:         event.UserId,
		ConversationId: event.ConversationId,
		LastReadSeq:    event.LastReadSeq,
		ConvType:       int32(event.ConvType),
		Avatar:         event.Avatar,
	}
}

// messageEventToPB 将内部消息事件转换为只包含消息本体字段的 WebSocket 推送对象。
// 内部事件中的路由、用户展示和媒体子表字段不会通过 MessageEvent 下发给前端。
func messageEventToPB(event protocol.MessageEvent) *wspb.MessageEvent {
	return &wspb.MessageEvent{
		MessageId:      event.MessageId,
		ConversationId: event.ConversationId,
		SenderId:       event.SenderId,
		Seq:            event.Seq,
		CType:          int32(event.CType),
		Content:        event.Content,
		SendTime:       event.SendTime,
		ClientMsgId:    event.ClientMsgId,
		Status:         int32(event.Status),
		AttachmentId:   event.AttachmentId,
	}
}
