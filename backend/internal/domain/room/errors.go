package domainroom

import (
	"errors"
)

var (
	ErrDuplicateCreation = errors.New("房间已存在")
	ErrDuplicateJoin     = errors.New("请勿重复加入房间")
	ErrDuplicateLeave    = errors.New("请勿重复退出房间")
	ErrInviteCodeExpired = errors.New("邀请码已过期")
	ErrRoomNameRequired  = errors.New("房间名称不能为空")
	ErrRoomNotFound      = errors.New("房间不存在")
	ErrRoomFull          = errors.New("房间成员已满")
	ErrMemberNotFound    = errors.New("不是房间成员")
	ErrPermissionDenied  = errors.New("没有操作权限")
	ErrVersionConflict   = errors.New("数据已被修改，请重试")
)
