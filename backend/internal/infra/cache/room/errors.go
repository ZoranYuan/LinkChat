package room

import (
	"errors"
)

var (
	ErrInviteCodeGenerationFailed = errors.New("生成邀请码失败")
	ErrInvalidTTL                 = errors.New("缓存有效期必须大于零")
)
