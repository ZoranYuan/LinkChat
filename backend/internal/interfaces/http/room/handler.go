package room

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	conversationapp "IM_backend/internal/application/conversation"
	roomapp "IM_backend/internal/application/room"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	roomApp         *roomapp.RoomApplication
	conversationApp *conversationapp.ConversationApplication
}

func NewHandler(roomApp *roomapp.RoomApplication, conversationApp *conversationapp.ConversationApplication) *Handler {
	return &Handler{
		roomApp:         roomApp,
		conversationApp: conversationApp,
	}
}

func (rh *Handler) Create(c *gin.Context) {

	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req CreateRoomRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	ctx := c.Request.Context()
	roomApp, err := rh.roomApp.Create(ctx, userID, req.RoomName, req.AvatarURL, req.Description)

	if err != nil {
		if errors.Is(err, roomapp.ErrInviteCodeUnavailable) {
			c.JSON(http.StatusServiceUnavailable, response.Error(http.StatusServiceUnavailable, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.Success(CreateRoomResponse{
		RoomID:      roomApp.RoomID,
		Description: roomApp.Description,
		RoomName:    roomApp.RoomName,
		AvatarURL:   roomApp.AvatarURL,
		MemberCount: roomApp.MemberCount,
		InviteCode:  roomApp.InviteCode,
	}))
}

func (rh *Handler) Invite(c *gin.Context) {
	userID := c.GetString("userId")

	roomID := c.Param("roomId")

	if roomID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	ctx := c.Request.Context()

	inviteCode, err := rh.roomApp.Invite(ctx, userID, roomID)

	if err != nil {
		if errors.Is(err, roomapp.ErrInviteCodeUnavailable) {
			c.JSON(http.StatusServiceUnavailable, response.Error(http.StatusServiceUnavailable, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.Success(inviteCode))
}

func (rh *Handler) Join(c *gin.Context) {
	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req JoinRoomRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	ctx := c.Request.Context()
	roomMemberApp, roomApp, err := rh.roomApp.Join(ctx, userID, req.InviteCode)

	if err != nil {
		if errors.Is(err, roomapp.ErrRoomFull) {
			c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	conversation, err := rh.conversationApp.FindConversationByRoomID(ctx, roomApp.RoomID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(
			http.StatusInternalServerError,
			"加入房间成功，但查询会话失败，请刷新会话列表",
		))
		return
	}

	c.JSON(http.StatusOK, response.Success(JoinRoomResponse{
		RoomID:         roomApp.RoomID,
		Description:    roomApp.Description,
		RoomName:       roomApp.RoomName,
		AvatarURL:      roomApp.AvatarURL,
		MemberCount:    roomApp.MemberCount,
		Role:           roomMemberApp.Role,
		JoinTime:       roomMemberApp.JoinTime,
		ConversationID: conversation.ConversationID,
		LatestSeq:      conversation.LatestSeq,
	}))
}

func (rh *Handler) Leave(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}
	roomID := c.Param("roomId")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if err := rh.roomApp.Leave(c.Request.Context(), userID, roomID); err != nil {
		if errors.Is(err, roomapp.ErrNotRoomMember) {
			c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}
	c.JSON(http.StatusOK, response.Success(nil))
}
