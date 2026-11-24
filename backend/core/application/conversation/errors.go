package conversation

import (
	"errors"
)

var (
	ErrEmptyConversationID     = errors.New("会话 ID 不能为空")
	ErrInvalidConversationID   = errors.New("会话 ID 无效")
	ErrConversationNotFound    = errors.New("会话不存在")
	ErrUnknownConversationType = errors.New("未知会话类型")
	ErrEmptyUserID             = errors.New("用户标识不能为空")
)
