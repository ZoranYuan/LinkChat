package group

import (
	"errors"

	domaingroup "IM_backend/core/domain/group"
)

var (
	ErrGroupNameRequired      = domaingroup.ErrGroupNameRequired
	ErrGroupNotFound          = domaingroup.ErrGroupNotFound
	ErrGroupFull              = domaingroup.ErrGroupFull
	ErrUnknown               = errors.New("未知错误")
	ErrNotGroupMember         = domaingroup.ErrMemberNotFound
	ErrGroupUnavailable       = errors.New("群聊当前不可用")
	ErrInviteCodeExpired     = domaingroup.ErrInviteCodeExpired
	ErrConcurrentUpdate      = domaingroup.ErrVersionConflict
	ErrPermissionDenied      = domaingroup.ErrPermissionDenied
	ErrInviteCodeUnavailable = errors.New("邀请码服务暂时不可用")
)
