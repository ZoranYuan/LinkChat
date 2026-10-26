package room

import (
	"errors"

	domainroom "IM_backend/internal/domain/room"
)

var (
	ErrRoomNameRequired          = domainroom.ErrRoomNameRequired
	ErrRoomNotFound              = domainroom.ErrRoomNotFound
	ErrRoomFull                  = domainroom.ErrRoomFull
	ErrUnknown                   = errors.New("未知错误")
	ErrNotRoomMember             = domainroom.ErrMemberNotFound
	ErrRoomUnavailable           = errors.New("房间当前不可用")
	ErrInviteCodeExpired         = domainroom.ErrInviteCodeExpired
	ErrConcurrentUpdate          = domainroom.ErrVersionConflict
	ErrPermissionDenied          = domainroom.ErrPermissionDenied
	ErrInviteCodeUnavailable     = errors.New("邀请码服务暂时不可用")
	ErrInvalidMemberChangedEvent = errors.New("房间成员变更事件无效")
)
