package domainuser

import (
	"errors"
)

var (
	ErrUserAlreadyExists  = errors.New("用户已存在")
	ErrUserNotFound       = errors.New("用户不存在")
	ErrIncorrectPassword  = errors.New("密码错误")
	ErrInvalidPhoneNumber = errors.New("手机号格式不正确")
)
