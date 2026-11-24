package agent

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	agentapp "IM_backend/core/application/agent"
	authcache "IM_backend/core/application/ports/cache/auth"
	"IM_backend/core/application/ports/security"
	domainconversation "IM_backend/core/domain/conversation"
	groupagentpb "IM_backend/core/interfaces/grpc/pb"
)

type GroupAgentContextHandler struct {
	groupagentpb.UnimplementedGroupAgentContextServiceServer

	app            *agentapp.GroupAgentApplication
	tokenValidator security.TokenValidator
	authCache      authcache.AuthCache
}

func NewGroupAgentContextHandler(
	app *agentapp.GroupAgentApplication,
	tokenValidator security.TokenValidator,
	authCache authcache.AuthCache,
) *GroupAgentContextHandler {
	return &GroupAgentContextHandler{
		app:            app,
		tokenValidator: tokenValidator,
		authCache:      authCache,
	}
}

func toProtoMessages(messages []agentapp.ChatMessageDTO) []*groupagentpb.ChatMessage {
	result := make([]*groupagentpb.ChatMessage, len(messages))
	for i, message := range messages {
		result[i] = &groupagentpb.ChatMessage{
			MessageId:      message.MessageID,
			ConversationId: message.ConversationID,
			SenderId:       message.SenderID,
			Seq:            message.Seq,
			Type:           groupagentpb.MessageType(message.Type),
			Content:        message.Content,
			Status:         groupagentpb.MessageStatus(message.Status),
			SendTime:       message.SendTime,
			SenderName:     message.SenderName,
		}
	}
	return result
}

func (h *GroupAgentContextHandler) GetGroupUnreadMessages(req *groupagentpb.GetGroupUnreadMessagesRequest, stream groupagentpb.GroupAgentContextService_GetGroupUnreadMessagesServer) error {
	ctx := stream.Context()

	groupID := req.GetGroupId()

	userID, err := h.authenticate(ctx)
	if err != nil {
		return err
	}

	err = h.app.GetGroupUnreadMessages(
		ctx,
		groupID,
		userID,
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&groupagentpb.GetGroupUnreadMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)

	return groupAgentStatusError(err)
}

func (h *GroupAgentContextHandler) SearchGroupMessages(
	req *groupagentpb.SearchGroupMessagesRequest,
	stream groupagentpb.GroupAgentContextService_SearchGroupMessagesServer,
) error {
	ctx := stream.Context()

	groupID := strings.TrimSpace(req.GetGroupId())
	if groupID == "" {
		return status.Error(codes.InvalidArgument, "group_id 不能为空")
	}

	userID, err := h.authenticate(ctx)
	if err != nil {
		return err
	}

	ranges := make([][2]int64, len(req.GetRanges()))

	for i, item := range req.GetRanges() {
		ranges[i] = [2]int64{
			item.GetSeqStart(),
			item.GetSeqEnd(),
		}
	}

	err = h.app.SearchGroupMessages(
		ctx,
		groupID,
		userID,
		ranges,
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&groupagentpb.SearchGroupMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)
	return groupAgentStatusError(err)
}

func (h *GroupAgentContextHandler) VerifyGroupAccess(
	ctx context.Context,
	req *groupagentpb.VerifyGroupAccessRequest,
) (*groupagentpb.VerifyGroupAccessResponse, error) {
	groupID := strings.TrimSpace(req.GetGroupId())
	if groupID == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id 不能为空")
	}

	userID, err := h.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	threadID, err := h.app.VerifyGroupAccess(ctx, groupID, userID)
	if err != nil {
		return nil, groupAgentStatusError(err)
	}

	return &groupagentpb.VerifyGroupAccessResponse{
		ThreadId: threadID,
	}, nil
}

func (h *GroupAgentContextHandler) SyncGroupMessages(
	req *groupagentpb.SyncGroupMessagesRequest,
	stream groupagentpb.GroupAgentContextService_SyncGroupMessagesServer,
) error {
	ctx := stream.Context()
	groupID := strings.TrimSpace(req.GetGroupId())

	if groupID == "" {
		return status.Error(codes.InvalidArgument, "group_id 不能为空")
	}

	if req.GetAfterSeq() < 0 {
		return status.Error(codes.InvalidArgument, "after_seq 不能小于 0")
	}

	userID, err := h.authenticate(ctx)
	if err != nil {
		return err
	}

	err = h.app.SyncGroupMessages(
		ctx,
		groupID,
		userID,
		req.GetAfterSeq(),
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&groupagentpb.SyncGroupMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)

	if err != nil {
		return groupAgentStatusError(err)
	}

	return nil
}

func (h *GroupAgentContextHandler) authenticate(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "缺少 authorization metadata")
	}
	values := md.Get("authorization")
	if len(values) != 1 {
		return "", status.Error(codes.Unauthenticated, "authorization metadata 无效")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", status.Error(codes.Unauthenticated, "authorization 必须使用 Bearer token")
	}
	userID, sessionID, err := h.tokenValidator.ValidateAccessToken(parts[1])
	if err != nil || userID == "" || sessionID == "" {
		return "", status.Error(codes.Unauthenticated, "access token 无效或已过期")
	}
	active, err := h.authCache.IsSessionActive(ctx, userID, sessionID)
	if err != nil {
		if ctx.Err() != nil {
			return "", status.FromContextError(ctx.Err()).Err()
		}
		return "", status.Error(codes.Unavailable, "认证服务暂时不可用")
	}
	if !active {
		return "", status.Error(codes.Unauthenticated, "登录会话已失效")
	}
	return userID, nil
}

func groupAgentStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, agentapp.ErrGroupAgentAccessDenied):
		return status.Error(codes.PermissionDenied, "用户无权访问该群聊")
	case errors.Is(err, domainconversation.ErrConversationNotCreated):
		return status.Error(codes.NotFound, "群聊会话不存在")
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, "Group Agent 请求处理失败")
	}
}
