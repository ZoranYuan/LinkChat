package message

import (
	"errors"

	conversationapp "IM_backend/core/application/conversation"
	domainfriend "IM_backend/core/domain/friend"
	domaingroup "IM_backend/core/domain/group"
)

var (
	ErrUnknownConversationType = conversationapp.ErrUnknownConversationType

	ErrMessageSeq              = errors.New("错误的消息序号")
	ErrConversationNotFound    = conversationapp.ErrConversationNotFound
	ErrInvalidSyncConversation = errors.New("消息同步仅支持群聊会话")

	ErrUserConversationNotFound = errors.New("用户会话不存在")
	ErrMessageNotFound          = errors.New("消息不存在")

	ErrNotFriends = domainfriend.ErrNotFriends

	ErrNotGroupMember = domaingroup.ErrMemberNotFound

	ErrTooManySeqs = errors.New("一次查询的消息序号过多")

	ErrForbidden               = errors.New("无权发送消息")
	ErrMessagePublishUncertain = errors.New("消息已登记，Kafka 投递结果待确认")
)
