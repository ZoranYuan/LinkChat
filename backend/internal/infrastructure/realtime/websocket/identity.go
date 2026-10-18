package websocket

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

var ErrInvalidConnectionIdentity = errors.New("非法的连接身份")

type Platform string

const (
	PlatfromWeb Platform = "web"

	// 后期扩展
)

// ConnectionIdentity 区分认证登录会话与具体的 WebSocket 连接。
type ConnectionIdentity struct {
	Platform      Platform
	UserID        string
	DeviceID      string
	AuthSessionID string
	ConnectionID  string
}

// NewConnectionIdentity 为每次连接生成独立 ID；AuthSessionID 仍来自 JWT 的 sid。
func NewConnectionIdentity(userID, deviceID, platform, authSessionID string) (ConnectionIdentity, error) {
	identity := ConnectionIdentity{
		UserID:        strings.TrimSpace(userID),
		DeviceID:      strings.TrimSpace(deviceID),
		Platform:      Platform(strings.ToLower(strings.TrimSpace(platform))),
		AuthSessionID: strings.TrimSpace(authSessionID),
		ConnectionID:  uuid.NewString(),
	}

	if identity.UserID == "" || identity.DeviceID == "" || identity.AuthSessionID == "" {
		return ConnectionIdentity{}, ErrInvalidConnectionIdentity
	}

	switch identity.Platform {
	case PlatfromWeb:
		return identity, nil
	default:
		return ConnectionIdentity{}, ErrInvalidConnectionIdentity
	}
}
