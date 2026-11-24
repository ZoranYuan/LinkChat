package domaingroup

import (
	"errors"
)

var (
	ErrDuplicateCreation = errors.New("群聊已存在")
	ErrDuplicateJoin     = errors.New("请勿重复加入群聊")
	ErrDuplicateLeave    = errors.New("请勿重复退出群聊")
	ErrInviteCodeExpired = errors.New("邀请码已过期")
	ErrGroupNameRequired  = errors.New("群聊名称不能为空")
	ErrGroupNotFound      = errors.New("群聊不存在")
	ErrGroupFull          = errors.New("群聊成员已满")
	ErrMemberNotFound    = errors.New("不是群聊成员")
	ErrPermissionDenied  = errors.New("没有操作权限")
	ErrVersionConflict   = errors.New("数据已被修改，请重试")
)
