package agent

import (
	"IM_backend/configs"
	agentapp "IM_backend/internal/application/agent"
	authport "IM_backend/internal/application/ports/persistence/cache/auth"
	messagevo "IM_backend/internal/domain/message/value_object"
	jwtutil "IM_backend/internal/infrastructure/security/jwt"
	roomagentpb "IM_backend/internal/transport/grpc/propto"
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type RoomAgentContextHandler struct {
	roomagentpb.UnimplementedRoomAgentContextServiceServer
	config    configs.Config
	app       *agentapp.RoomAgentApplication
	authCache authport.AuthCache
}

func NewRoomAgentContextHandler(
	app *agentapp.RoomAgentApplication,
	config configs.Config,
	authCache authport.AuthCache,
) *RoomAgentContextHandler {
	return &RoomAgentContextHandler{
		app:       app,
		config:    config,
		authCache: authCache,
	}
}

func (h *RoomAgentContextHandler) GetRoomAgentContext(
	ctx context.Context,
	req *roomagentpb.GetRoomAgentContextRequest,
) (*roomagentpb.GetRoomAgentContextResponse, error) {
	if req == nil || strings.TrimSpace(req.GetRoomId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id 不能为空")
	}

	userID, sessionID, err := h.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	active, err := h.authCache.IsSessionActive(ctx, userID, sessionID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "认证服务暂时不可用")
	}
	if !active {
		return nil, status.Error(codes.Unauthenticated, "登录会话已失效")
	}

	result, err := h.app.GetRoomAgentContext(ctx, strings.TrimSpace(req.GetRoomId()), userID)
	if err != nil {
		if errors.Is(err, agentapp.ErrRoomAgentAccessDenied) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, status.Error(codes.Internal, "获取 Room Agent 上下文失败")
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "Room Agent 上下文为空")
	}

	response := &roomagentpb.GetRoomAgentContextResponse{
		UserId:         userID,
		ThreadId:       result.ThreadID,
		UnreadSnapshot: &roomagentpb.UnreadSnapshot{Messages: make([]*roomagentpb.ChatMessage, 0, len(result.Messages))},
		UnreadBeginSeq: result.UnreadBeginSeq,
		UnreadEndSeq:   result.UnreadEndSeq,
	}
	for _, message := range result.Messages {
		messageType, err := toProtoMessageType(message.Type)
		if err != nil {
			return nil, status.Error(codes.Internal, "摘要上下文包含不支持的消息类型")
		}
		messageStatus, err := toProtoMessageStatus(message.Status)
		if err != nil {
			return nil, status.Error(codes.Internal, "摘要上下文包含未知的消息状态")
		}
		response.UnreadSnapshot.Messages = append(response.UnreadSnapshot.Messages, &roomagentpb.ChatMessage{
			MessageId:      message.MessageID,
			ConversationId: message.ConversationID,
			SenderId:       message.SenderID,
			ClientMsgId:    message.ClientMsgID,
			Seq:            message.Seq,
			Type:           messageType,
			Content:        message.Content,
			Status:         messageStatus,
			SendTime:       message.SendTime,
		})
	}
	response.UnreadMessageCount = uint32(len(response.UnreadSnapshot.Messages))
	return response, nil
}

func (h *RoomAgentContextHandler) authenticate(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "缺少 authorization metadata")
	}
	values := md.Get("authorization")
	if len(values) != 1 {
		return "", "", status.Error(codes.Unauthenticated, "authorization metadata 无效")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", "", status.Error(codes.Unauthenticated, "authorization 必须使用 Bearer token")
	}

	claims, err := jwtutil.ValidateToken(parts[1], h.config.JWT.Secret)
	if err != nil || claims.UserID == "" || claims.SessionID == "" {
		return "", "", status.Error(codes.Unauthenticated, "access token 无效或已过期")
	}
	return claims.UserID, claims.SessionID, nil
}

func toProtoMessageType(value int32) (roomagentpb.MessageType, error) {
	switch messagevo.CType(value) {
	case messagevo.Text:
		return roomagentpb.MessageType_MESSAGE_TYPE_TEXT, nil
	default:
		return roomagentpb.MessageType_MESSAGE_TYPE_TEXT, status.Error(codes.InvalidArgument, "不支持的消息类型")
	}
}

func toProtoMessageStatus(value int32) (roomagentpb.MessageStatus, error) {
	switch messagevo.Status(value) {
	case messagevo.Normal:
		return roomagentpb.MessageStatus_MESSAGE_STATUS_NORMAL, nil
	case messagevo.WithDraw:
		return roomagentpb.MessageStatus_MESSAGE_STATUS_RECALLED, nil
	default:
		return roomagentpb.MessageStatus_MESSAGE_STATUS_NORMAL, status.Error(codes.InvalidArgument, "未知的消息状态")
	}
}
