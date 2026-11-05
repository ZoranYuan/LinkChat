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
	case string(messaging.TypeMessagePush):
		var event messaging.MessagePushEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messagePushToPB(event))
	case string(messaging.TypeMessageSendResult):
		var event messaging.MessageSendResultEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messageSendResultToPB(event))
	case string(messaging.TypeMessageRead), string(messaging.TypeMessageReadNotice):
		var event messaging.MessageReadNotice
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(messageReadNoticeToPB(event))
	case string(messaging.TypeRoomMessageNotice):
		var event messaging.RoomMessageNoticeEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		return proto.Marshal(roomMessageNoticeToPB(event))
	default:
		return payload, nil
	}
}

// messageSendResultToPB 将内部 ACK 事件编码为前端使用的 protobuf 消息。
func messageSendResultToPB(event messaging.MessageSendResultEvent) *wspb.MessageSendResult {
	return &wspb.MessageSendResult{
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

// messageReadNoticeToPB 将内部已读事件编码为前端使用的 protobuf 消息。
func messageReadNoticeToPB(event messaging.MessageReadNotice) *wspb.MessageReadNotice {
	return &wspb.MessageReadNotice{UserId: event.UserID, ConversationId: event.ConversationID, LastReadSeq: event.LastReadSeq, ConvType: int32(event.ConvType), Avatar: event.Avatar}
}

// roomMessageNoticeToPB 将大型房间轻量通知编码为 protobuf。
// 通知只携带会话和序号，客户端随后通过同步接口拉取消息详情。
func roomMessageNoticeToPB(event messaging.RoomMessageNoticeEvent) *wspb.RoomMessageNotice {
	return &wspb.RoomMessageNotice{
		ConversationId: event.ConversationID,
		MessageId:      event.MessageID,
		Seq:            event.Seq,
	}
}

// messagePushToPB 将内部消息事件映射为只包含消息本体的 WebSocket 推送对象。
// Kafka 投递所需的路由字段仍保留在内部事件中，但不会进入前端协议。
func messagePushToPB(event messaging.MessagePushEvent) *wspb.MessagePush {
	return &wspb.MessagePush{
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
