package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"IM_backend/configs"
	conversationcache "IM_backend/internal/application/ports/cache/conversation"
	domainconversation "IM_backend/internal/domain/conversation"
)

type ConversationCache struct {
	client *redis.Client
	config configs.ConversationInfoConfig
}

var _ conversationcache.ConversationCache = (*ConversationCache)(nil)

func NewConversationCache(client *redis.Client, config configs.ConversationInfoConfig) *ConversationCache {
	return &ConversationCache{client: client, config: config}
}

func conversationInfoKey(id string) string { return "im:conversation:info:" + id }

func validInfo(info *conversationcache.ConversationInfo) bool {
	if info == nil || info.ConversationID == "" {
		return false
	}
	switch domainconversation.ConvType(info.ConversationType) {
	case domainconversation.PrivateChat:
		return info.UserID1 != "" && info.UserID2 != "" && info.UserID1 != info.UserID2 && info.RoomID == ""
	case domainconversation.Room:
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
	return c.client.SetNX(ctx, conversationInfoKey(id), "null", time.Duration(c.config.NegativeTTLSeconds)*time.Second).Err()
}
