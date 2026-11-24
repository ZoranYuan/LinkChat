package domainfriend

import (
	"errors"
)

var (
	ErrNotFriends                = errors.New("双方不是好友")
	ErrAlreadyFriends            = errors.New("双方已经是好友")
	ErrRequestSentTooFrequently  = errors.New("重复申请")
	ErrInvalidStatusTransition   = errors.New("好友申请状态变更无效")
	ErrDuplicateRequestOperation = errors.New("请勿重复操作好友申请")
	ErrSelfRequest               = errors.New("不能向自己发送好友申请")
	ErrInvalidRequestOperation   = errors.New("好友申请操作无效")
	ErrFriendRequestNotFound     = errors.New("好友申请不存在")
)
