package ws

import (
	"IM_backend/internal/application/ports/messaging"
	"encoding/json"

	"google.golang.org/protobuf/proto"

	wspb "IM_backend/internal/interfaces/ws/pb"
)

// EncodePayload 按事件类型将后端 JSON 事件编码为对应的 WebSocket protobuf payload。
// 未配置 protobuf 映射的事件保持原始 payload，保证非消息事件可以继续透传。
func EncodePayload(eventType string, payload []byte) ([]byte, error) {
	switch eventType {
	case string(messaging.TypeMessage):
		var event messaging.MessageEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messageEventToPB(event))
	case string(messaging.TypeMessageAck):
		var event messaging.MessageAckEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messageAckToPB(event))
	case string(messaging.TypeReadAck), string(messaging.TypeReadNotify):
		var event messaging.MessageReadAckEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messageReadAckEventToPB(event))
	case string(messaging.TypeRoomMessageNotice):
		var event messaging.MessageNotifyEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(roomMessageNoticeToPB(event))
	default:
		return payload, nil
	}
}

// messageAckToPB 将内部 ACK 事件编码为前端使用的 protobuf 消息。
func messageAckToPB(event messaging.MessageAckEvent) *wspb.MessageAck {
	return &wspb.MessageAck{
		ClientMsgId:    event.ClientMsgID,
		MessageId:      event.MessageID,
		Status:         string(event.Status),
		Extra:          event.Extra,
		SendTime:       event.SendTime,
		ConversationId: event.ConversationID,
		Seq:            event.Seq,
		AttachmentId:   event.AttachmentID,
	}
}

// messageReadAckEventToPB 将内部已读事件编码为前端使用的 protobuf 消息。
func messageReadAckEventToPB(event messaging.MessageReadAckEvent) *wspb.MessageReadAckEvent {
	return &wspb.MessageReadAckEvent{UserId: event.UserID, ConversationId: event.ConversationID, LastReadSeq: event.LastReadSeq, ConvType: int32(event.ConvType), Avatar: event.Avatar}
}

// roomMessageNoticeToPB 将大型房间轻量通知编码为 protobuf。
// 通知只携带会话和序号，客户端随后通过同步接口拉取消息详情。
func roomMessageNoticeToPB(event messaging.MessageNotifyEvent) *wspb.RoomMessageNotice {
	return &wspb.RoomMessageNotice{
		ConversationId: event.ConversationID,
		MessageId:      event.MessageID,
		Seq:            event.Seq,
	}
}

// messageEventToPB 将内部消息事件映射为只包含消息本体的 WebSocket 推送对象。
// Kafka 投递所需的路由字段仍保留在内部事件中，但不会进入前端协议。
func messageEventToPB(event messaging.MessageEvent) *wspb.MessageEvent {
	return &wspb.MessageEvent{
		MessageId:      event.MessageID,
		ConversationId: event.ConversationID,
		SenderId:       event.SenderID,
		Seq:            event.Seq,
		CType:          int32(event.MessageType),
		Content:        event.Content,
		SendTime:       event.SendTime,
		ClientMsgId:    event.ClientMsgID,
		Status:         int32(event.Status),
		AttachmentId:   event.AttachmentID,
	}
}
