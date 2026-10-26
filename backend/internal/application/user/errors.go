package user

import (
	"errors"

	domainuser "IM_backend/internal/domain/user"
)

var (
	ErrUserAlreadyExists   = domainuser.ErrUserAlreadyExists
	ErrUserNotFound        = domainuser.ErrUserNotFound
	ErrIncorrectPassword   = domainuser.ErrIncorrectPassword
	ErrInvalidPhoneNumber  = domainuser.ErrInvalidPhoneNumber
	ErrInvalidLoginAccount = errors.New("登录账号不能为空")
	ErrPasswordMismatch    = errors.New("两次输入的密码不一致")
	ErrNoProfileFields     = errors.New("无用户资料更新字段")
)
