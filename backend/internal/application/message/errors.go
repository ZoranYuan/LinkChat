package message

import (
	"errors"

	conversationapp "IM_backend/internal/application/conversation"
	domainfriend "IM_backend/internal/domain/friend"
	domainroom "IM_backend/internal/domain/room"
)

var (
	ErrUnknownConversationType = conversationapp.ErrUnknownConversationType

	ErrMessageSeq           = errors.New("错误的消息序号")
	ErrConversationNotFound = conversationapp.ErrConversationNotFound

	ErrUserConversationNotFound = errors.New("用户会话不存在")
	ErrMessageNotFound          = errors.New("消息不存在")

	ErrNotFriends = domainfriend.ErrNotFriends

	ErrNotRoomMember = domainroom.ErrMemberNotFound

	ErrConversationSequenceUpdate = errors.New("更新会话序列失败")
	ErrTooManySeqs                = errors.New("一次查询的消息序号过多")

	ErrForbidden = errors.New("无权发送消息")

	ErrUnknown = errors.New("未知错误")
)
