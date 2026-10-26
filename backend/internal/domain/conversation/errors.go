package domainconversation

import (
	"errors"
)

var (
	ErrDuplicateCreation      = errors.New("请勿重复创建")
	ErrConversationNotCreated = errors.New("会话尚未创建")
)
