package group

import (
	"context"
	"errors"
	"log"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/core/application/conversation"
	groupcache "IM_backend/core/application/ports/cache/group"
	idport "IM_backend/core/application/ports/id"
	"IM_backend/core/application/ports/transaction"
	domainconversation "IM_backend/core/domain/conversation"
	domaingroup "IM_backend/core/domain/group"
)

type GroupApplication struct {
	config                     configs.GroupConfig
	groupRepository             domaingroup.GroupRepository
	groupMemberRepository       domaingroup.GroupMemberRepository
	userConversationRepository domainconversation.UserConversationRepository
	conversationRepository     domainconversation.ConversationRepository
	conversationApplication    *conversationapp.ConversationApplication
	groupCache                  groupcache.GroupCache
	groupMemberCache            groupcache.GroupMemberCache
	groupInfoCache              groupcache.GroupInfoCache
	groupInfoCacheTTL           time.Duration
	txManager                  transaction.Manager
	idGenerator                idport.Generator
}

func NewGroupApplication(groupRepository domaingroup.GroupRepository,
	groupMemberRepository domaingroup.GroupMemberRepository,
	userConversationRepository domainconversation.UserConversationRepository,
	conversationRepository domainconversation.ConversationRepository,
	groupCache groupcache.GroupCache,
	groupMemberCache groupcache.GroupMemberCache,
	txManager transaction.Manager,
	idGenerator idport.Generator,
	config configs.GroupConfig,
) *GroupApplication {
	return &GroupApplication{
		config:                     config,
		groupRepository:             groupRepository,
		groupMemberRepository:       groupMemberRepository,
		conversationRepository:     conversationRepository,
		userConversationRepository: userConversationRepository,
		groupCache:                  groupCache,
		groupMemberCache:            groupMemberCache,
		txManager:                  txManager,
		idGenerator:                idGenerator,
	}
}

func (ra *GroupApplication) SetGroupInfoCache(cache groupcache.GroupInfoCache, ttl time.Duration) {
	if ra != nil {
		ra.groupInfoCache = cache
		ra.groupInfoCacheTTL = ttl
	}
}

func (ra *GroupApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	ra.conversationApplication = application
}

func (ra *GroupApplication) Create(ctx context.Context, userID, groupName, avatar, description string) (*GroupAppDTO, error) {
	groupID, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	conversationID, err := ra.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	group, err := domaingroup.NewGroup(groupID, userID, description, groupName, avatar, ra.config.MaxMembers)
	if err != nil {
		if errors.Is(err, domaingroup.ErrGroupNameRequired) {
			return nil, ErrGroupNameRequired
		} else {
			return nil, ErrUnknown
		}
	}
	groupMember := domaingroup.NewGroupMember(userID, groupID, domaingroup.HomeOwner)
	groupMember.Join()

	conversation := domainconversation.NewGroupConversation(conversationID, groupID)

	userConversation := domainconversation.BuildUserConversation(
		userID,
		conversationID,
		0,
	)

	inviteCode, err := ra.groupCache.UpdateInviteCode(ctx, groupID, time.Duration(ra.config.InviteCodeTTLSeconds)*time.Second)
	if err != nil {
		log.Println("生成邀请码失败：", err)
		return nil, ErrInviteCodeUnavailable
	}

	if err := ra.txManager.WithinTransaction(ctx, func(tx any) error {
		groupRepository := ra.groupRepository.WithTx(tx)
		groupMemberRepository := ra.groupMemberRepository.WithTx(tx)
		conversationRepository := ra.conversationRepository.WithTx(tx)
		userConversationRepository := ra.userConversationRepository.WithTx(tx)

		if err := groupRepository.Create(group); err != nil {
			if errors.Is(err, domaingroup.ErrDuplicateCreation) {
				return ErrConcurrentUpdate
			}
			return err
		}

		if _, err := groupMemberRepository.JoinGroup(groupMember); err != nil {
			return err
		}

		if err := conversationRepository.CreateConversation(ctx, conversation); err != nil {
			return err
		}

		if err := userConversationRepository.CreateUserConversation(ctx, userConversation); err != nil {
			return err
		}
		return nil
	}); err != nil {
		if cleanupErr := ra.groupCache.DeleteInviteCode(ctx, groupID); cleanupErr != nil {
			log.Println("清理邀请码缓存失败：", cleanupErr)
		}
		return nil, err
	}
	ra.cacheGroupInfo(ctx, group)
	ra.conversationApplication.CacheConversationInfo(ctx, conversation)

	return toGroupAppDTO(group, inviteCode), nil
}

func (ra *GroupApplication) Invite(ctx context.Context, userID, groupID string) (string, error) {
	group, err := ra.GetActiveGroup(ctx, groupID)
	if err != nil {
		if errors.Is(err, domaingroup.ErrGroupNotFound) {
			return "", ErrGroupNotFound
		}

		log.Println("查询群聊失败：", err)
		return "", ErrUnknown
	}

	groupMember, err := ra.groupMemberRepository.FindGroupMemberByUserIDAndGroupID(userID, groupID)
	if err != nil {
		if errors.Is(err, domaingroup.ErrMemberNotFound) {
			return "", ErrNotGroupMember
		}
		return "", ErrUnknown
	}

	if err := groupMember.Invite(); err != nil {
		return "", ErrPermissionDenied
	}

	if !group.Invite() {
		return "", ErrGroupUnavailable
	}

	inviteCode, err := ra.groupCache.UpdateInviteCode(ctx, groupID, time.Duration(ra.config.InviteCodeTTLSeconds)*time.Second)

	if err != nil {
		log.Println("获取邀请码失败：", err)
		return "", ErrInviteCodeUnavailable
	}

	return inviteCode, nil
}

// GetActiveGroup 复用基础信息缓存，统一拒绝已失效的群聊。
func (ra *GroupApplication) GetActiveGroup(ctx context.Context, groupID string) (*domaingroup.Group, error) {
	group, err := ra.GetGroupInfo(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if group.Status != domaingroup.Normal {
		return nil, domaingroup.ErrGroupNotFound
	}
	return group, nil
}

func (ra *GroupApplication) GetActiveGroups(ctx context.Context, groupIDs []string) (map[string]*domaingroup.Group, error) {
	groups := make(map[string]*domaingroup.Group, len(groupIDs))
	if len(groupIDs) == 0 {
		return groups, nil
	}

	groupByID := make(map[string]*domaingroup.Group, len(groupIDs))
	if ra.groupInfoCache != nil {
		cached, err := ra.groupInfoCache.GetGroupInfos(ctx, groupIDs)
		if err != nil {
			log.Printf("批量读取群聊基础信息缓存失败：group_count=%d error=%v", len(groupIDs), err)
		} else {
			groupByID = cached
		}
	}

	missIDs := make([]string, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		if groupByID[groupID] == nil {
			missIDs = append(missIDs, groupID)
		}
	}
	databaseGroups, err := ra.groupRepository.ListGroupsByIDs(ctx, missIDs)
	if err != nil {
		return nil, err
	}
	for _, group := range databaseGroups {
		groupByID[group.GroupID] = group
	}
	if ra.groupInfoCache != nil && ra.groupInfoCacheTTL > 0 && len(databaseGroups) > 0 {
		if err := ra.groupInfoCache.SetGroupInfos(ctx, databaseGroups, ra.groupInfoCacheTTL); err != nil {
			log.Printf("批量回填群聊基础信息缓存失败：group_count=%d error=%v", len(databaseGroups), err)
		}
	}

	for _, groupID := range groupIDs {
		if group := groupByID[groupID]; group != nil && group.Status == domaingroup.Normal {
			groups[groupID] = group
		}
	}
	return groups, nil
}

// GetGroupInfo 先读 Redis；缓存未命中或不可用时回源 MySQL 并尝试回填。
func (ra *GroupApplication) GetGroupInfo(ctx context.Context, groupID string) (*domaingroup.Group, error) {
	if groupID == "" {
		return nil, domaingroup.ErrGroupNotFound
	}
	if ra.groupInfoCache != nil {
		cached, found, err := ra.groupInfoCache.GetGroupInfo(ctx, groupID)
		if err == nil && found && cached != nil {
			return cached, nil
		}
		if err != nil {
			log.Printf("读取群聊基础信息缓存失败，回源 MySQL：group=%s error=%v", groupID, err)
		}
	}

	group, err := ra.groupRepository.FindGroupByGroupID(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, domaingroup.ErrGroupNotFound
	}
	ra.cacheGroupInfo(ctx, group)
	return group, nil
}

func (ra *GroupApplication) cacheGroupInfo(ctx context.Context, group *domaingroup.Group) {
	if ra == nil || ra.groupInfoCache == nil || group == nil || ra.groupInfoCacheTTL <= 0 {
		return
	}
	if err := ra.groupInfoCache.SetGroupInfo(ctx, group, ra.groupInfoCacheTTL); err != nil {
		log.Printf("写入群聊基础信息缓存失败：group=%s error=%v", group.GroupID, err)
	}
}

func (ra *GroupApplication) invalidateGroupInfo(ctx context.Context, groupID string) {
	if ra == nil || ra.groupInfoCache == nil || groupID == "" {
		return
	}
	if err := ra.groupInfoCache.DeleteGroupInfo(ctx, groupID); err != nil {
		log.Printf("删除群聊基础信息缓存失败：group=%s error=%v", groupID, err)
	}
}

func (ra *GroupApplication) invalidateGroupMember(ctx context.Context, groupID, userID string) {
	if ra == nil || ra.groupMemberCache == nil || groupID == "" || userID == "" {
		return
	}
	if err := ra.groupMemberCache.DeleteMember(ctx, groupID, userID); err != nil {
		log.Printf("删除群聊成员缓存失败：group=%s user=%s error=%v", groupID, userID, err)
	}
}
