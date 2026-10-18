package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	conversationcache "IM_backend/internal/application/ports/persistence/cache/conversation"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	"github.com/redis/go-redis/v9"
)

type ConversationCache struct{ client *redis.Client }

var _ conversationcache.ConversationCache = (*ConversationCache)(nil)

func NewConversationCache(client *redis.Client) *ConversationCache {
	return &ConversationCache{client: client}
}

func conversationInfoKey(id string) string { return "im:conversation:info:" + id }

func validInfo(info *conversationcache.ConversationInfo) bool {
	if info == nil || info.ConversationID == "" {
		return false
	}
	switch conversationvo.ConvType(info.ConversationType) {
	case conversationvo.PrivateChat:
		return info.UserID1 != "" && info.UserID2 != "" && info.UserID1 != info.UserID2 && info.RoomID == ""
	case conversationvo.RoomChat:
		return info.RoomID != "" && info.UserID1 == "" && info.UserID2 == ""
	default:
		return false
	}
}

func (c *ConversationCache) GetConversationInfo(ctx context.Context, id string) (*conversationcache.ConversationInfo, bool, error) {
	if id == "" {
		return nil, false, errors.New("会话 ID 不能为空")
	}
	data, err := c.client.Get(ctx, conversationInfoKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var info *conversationcache.ConversationInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, false, err
	}
	if info != nil && (!validInfo(info) || info.ConversationID != id) {
		return nil, false, errors.New("会话缓存内容无效")
	}
	return info, true, nil
}

func (c *ConversationCache) SetConversationInfo(ctx context.Context, info *conversationcache.ConversationInfo, ttl time.Duration) error {
	if !validInfo(info) || ttl <= 0 {
		return errors.New("会话缓存参数无效")
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, conversationInfoKey(info.ConversationID), data, ttl).Err()
}

func (c *ConversationCache) SetConversationNotFound(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("会话 ID 不能为空")
	}
	// 不覆盖创建成功后写入的正常信息，防止较晚返回的空查询污染缓存。
	return c.client.SetNX(ctx, conversationInfoKey(id), "null", 2*time.Minute).Err()
}

func (c *ConversationCache) DeleteConversationInfo(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("会话 ID 不能为空")
	}
	return c.client.Del(ctx, conversationInfoKey(id)).Err()
}
