package agent

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	agentapp "IM_backend/internal/application/agent"
	authcache "IM_backend/internal/application/ports/cache/auth"
	"IM_backend/internal/application/ports/security"
	domainconversation "IM_backend/internal/domain/conversation"
	roomagentpb "IM_backend/internal/interfaces/grpc/pb"
)

type RoomAgentContextHandler struct {
	roomagentpb.UnimplementedRoomAgentContextServiceServer

	app            *agentapp.RoomAgentApplication
	tokenValidator security.TokenValidator
	authCache      authcache.AuthCache
}

func NewRoomAgentContextHandler(
	app *agentapp.RoomAgentApplication,
	tokenValidator security.TokenValidator,
	authCache authcache.AuthCache,
) *RoomAgentContextHandler {
	return &RoomAgentContextHandler{
		app:            app,
		tokenValidator: tokenValidator,
		authCache:      authCache,
	}
}

func toProtoMessages(messages []agentapp.ChatMessageDTO) []*roomagentpb.ChatMessage {
	result := make([]*roomagentpb.ChatMessage, len(messages))
	for i, message := range messages {
		result[i] = &roomagentpb.ChatMessage{
			MessageId:      message.MessageID,
			ConversationId: message.ConversationID,
			SenderId:       message.SenderID,
			Seq:            message.Seq,
			Type:           roomagentpb.MessageType(message.Type),
			Content:        message.Content,
			Status:         roomagentpb.MessageStatus(message.Status),
			SendTime:       message.SendTime,
			SenderName:     message.SenderName,
		}
	}
	return result
}

func (h *RoomAgentContextHandler) GetRoomUnreadMessages(req *roomagentpb.GetRoomUnreadMessagesRequest, stream roomagentpb.RoomAgentContextService_GetRoomUnreadMessagesServer) error {
	ctx := stream.Context()

	roomID := req.GetRoomId()

	userID, err := h.authenticate(ctx)
	if err != nil {
		return err
	}

	err = h.app.GetRoomUnreadMessages(
		ctx,
		roomID,
		userID,
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&roomagentpb.GetRoomUnreadMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)

	return roomAgentStatusError(err)
}

func (h *RoomAgentContextHandler) SearchRoomMessages(
	req *roomagentpb.SearchRoomMessagesRequest,
	stream roomagentpb.RoomAgentContextService_SearchRoomMessagesServer,
) error {
	ctx := stream.Context()

	roomID := strings.TrimSpace(req.GetRoomId())
	if roomID == "" {
		return status.Error(codes.InvalidArgument, "room_id 不能为空")
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

	err = h.app.SearchRoomMessages(
		ctx,
		roomID,
		userID,
		ranges,
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&roomagentpb.SearchRoomMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)
	return roomAgentStatusError(err)
}

func (h *RoomAgentContextHandler) VerifyRoomAccess(
	ctx context.Context,
	req *roomagentpb.VerifyRoomAccessRequest,
) (*roomagentpb.VerifyRoomAccessResponse, error) {
	roomID := strings.TrimSpace(req.GetRoomId())
	if roomID == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id 不能为空")
	}

	userID, err := h.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	threadID, err := h.app.VerifyRoomAccess(ctx, roomID, userID)
	if err != nil {
		return nil, roomAgentStatusError(err)
	}

	return &roomagentpb.VerifyRoomAccessResponse{
		ThreadId: threadID,
	}, nil
}

func (h *RoomAgentContextHandler) SyncRoomMessages(
	req *roomagentpb.SyncRoomMessagesRequest,
	stream roomagentpb.RoomAgentContextService_SyncRoomMessagesServer,
) error {
	ctx := stream.Context()
	roomID := strings.TrimSpace(req.GetRoomId())

	if roomID == "" {
		return status.Error(codes.InvalidArgument, "room_id 不能为空")
	}

	if req.GetAfterSeq() < 0 {
		return status.Error(codes.InvalidArgument, "after_seq 不能小于 0")
	}

	userID, err := h.authenticate(ctx)
	if err != nil {
		return err
	}

	err = h.app.SyncRoomMessages(
		ctx,
		roomID,
		userID,
		req.GetAfterSeq(),
		req.GetLimit(),
		func(messages []agentapp.ChatMessageDTO) error {
			return stream.Send(&roomagentpb.SyncRoomMessagesResponse{
				Messages: toProtoMessages(messages),
			})
		},
	)

	if err != nil {
		return roomAgentStatusError(err)
	}

	return nil
}

func (h *RoomAgentContextHandler) authenticate(ctx context.Context) (string, error) {
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

func roomAgentStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, agentapp.ErrRoomAgentAccessDenied):
		return status.Error(codes.PermissionDenied, "用户无权访问该房间")
	case errors.Is(err, domainconversation.ErrConversationNotCreated):
		return status.Error(codes.NotFound, "房间会话不存在")
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, "Room Agent 请求处理失败")
	}
}
