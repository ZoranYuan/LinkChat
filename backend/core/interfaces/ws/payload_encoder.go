package ws

import (
	eventport "IM_backend/core/application/ports/events"

	wspb "IM_backend/core/interfaces/ws/pb"
)

// messageReadNoticeToPB 将内部已读事件编码为前端使用的 protobuf 消息。
func messageReadNoticeToPB(event eventport.MessageReadNoticeEvent) *wspb.MessageReadNotice {
	return &wspb.MessageReadNotice{UserId: event.UserID, ConversationId: event.ConversationID, LastReadSeq: event.LastReadSeq, ConvType: int32(event.ConvType), Avatar: event.Avatar}
}

// groupMessageNoticeToPB 将大型群聊轻量通知编码为 protobuf。
// 通知只携带会话和序号，客户端随后通过同步接口拉取消息详情。
func groupMessageNoticeToPB(event eventport.GroupMessageNoticeEvent) *wspb.GroupMessageNotice {
	return &wspb.GroupMessageNotice{
		ConversationId: event.ConversationID,
		Seq:            event.Seq,
	}
}

func friendRequestNoticeToPB(event eventport.FriendRequestNoticeEvent) *wspb.FriendRequestNotice {
	return &wspb.FriendRequestNotice{
		RequestId:       event.RequestID,
		ApplicantUserId: event.ApplicantUserID,
		PeerUserId:      event.PeerUserID,
		Message:         event.Message,
		ApplyTime:       event.ApplyTime,
	}
}

func errorNoticeToPB(event ErrorNoticeEvent) *wspb.ErrorNotice {
	return &wspb.ErrorNotice{
		RequestOp: event.RequestOp,
		Code:      event.Code,
		Message:   event.Message,
	}
}

// messagePushToPB 将内部消息事件映射为只包含消息本体的 WebSocket 推送对象。
// Kafka 投递所需的路由字段仍保留在内部事件中，但不会进入前端协议。
func messagePushToPB(event eventport.MessagePushEvent) *wspb.MessagePush {
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
