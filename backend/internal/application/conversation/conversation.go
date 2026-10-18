package conversation

import (
	"context"
	"log"
	"time"

	conversationcache "IM_backend/internal/application/ports/persistence/cache/conversation"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
)

type ConversationApplication struct {
	repository conversationrepo.ConversationRepository
	cache      conversationcache.ConversationCache
	cacheTTL   time.Duration
}

func NewConversationApplication(repository conversationrepo.ConversationRepository, cache conversationcache.ConversationCache, ttl time.Duration) *ConversationApplication {
	return &ConversationApplication{repository: repository, cache: cache, cacheTTL: ttl}
}

func (ca *ConversationApplication) GetConversationInfo(ctx context.Context, id string) (*conversationcache.ConversationInfo, error) {
	if id == "" {
		return nil, ErrEmptyConversationID
	}
	if len(id) > 64 {
		return nil, ErrInvalidConversationID
	}
	info, found, err := ca.cache.GetConversationInfo(ctx, id)
	if err != nil {
		log.Printf("读取会话缓存失败：conversation=%s error=%v", id, err)
	} else if found {
		if info == nil {
			return nil, ErrConversationNotFound
		}
		return info, nil
	}
	record, err := ca.repository.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		if err := ca.cache.SetConversationNotFound(ctx, id); err != nil {
			log.Printf("写入会话空缓存失败：conversation=%s error=%v", id, err)
		}
		return nil, ErrConversationNotFound
	}
	if record.Convtype != conversationvo.PrivateChat && record.Convtype != conversationvo.RoomChat {
		return nil, ErrUnknownConversationType
	}
	ca.CacheConversationInfo(ctx, record)
	return conversationcache.FromConversation(record), nil
}

// CacheConversationInfo 只在事务提交后或读取已提交记录时调用，缓存异常不改变业务结果。
func (ca *ConversationApplication) CacheConversationInfo(ctx context.Context, record *conversationentity.Conversation) {
	if err := ca.cache.SetConversationInfo(ctx, conversationcache.FromConversation(record), ca.cacheTTL); err != nil {
		log.Printf("写入会话缓存失败：conversation=%s error=%v", record.ConversationId, err)
	}
}
