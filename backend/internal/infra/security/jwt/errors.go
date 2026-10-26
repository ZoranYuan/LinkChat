package jwt

import (
	"errors"
)

var (
	errInvalidToken = errors.New("令牌无效")
	errExpiredToken = errors.New("令牌已过期")
)
