package group

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	conversationapp "IM_backend/core/application/conversation"
	groupapp "IM_backend/core/application/group"
	"IM_backend/core/interfaces/http/response"
)

type Handler struct {
	groupApp         *groupapp.GroupApplication
	conversationApp *conversationapp.ConversationApplication
}

func NewHandler(groupApp *groupapp.GroupApplication, conversationApp *conversationapp.ConversationApplication) *Handler {
	return &Handler{
		groupApp:         groupApp,
		conversationApp: conversationApp,
	}
}

func (rh *Handler) Create(c *gin.Context) {

	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	ctx := c.Request.Context()
	groupApp, err := rh.groupApp.Create(ctx, userID, req.GroupName, req.AvatarURL, req.Description)

	if err != nil {
		if errors.Is(err, groupapp.ErrInviteCodeUnavailable) {
			c.JSON(http.StatusServiceUnavailable, response.Error(http.StatusServiceUnavailable, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.Success(CreateGroupResponse{
		GroupID:      groupApp.GroupID,
		Description: groupApp.Description,
		GroupName:    groupApp.GroupName,
		AvatarURL:   groupApp.AvatarURL,
		MemberCount: groupApp.MemberCount,
		InviteCode:  groupApp.InviteCode,
	}))
}

func (rh *Handler) Invite(c *gin.Context) {
	userID := c.GetString("userId")

	groupID := c.Param("groupId")

	if groupID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	ctx := c.Request.Context()

	inviteCode, err := rh.groupApp.Invite(ctx, userID, groupID)

	if err != nil {
		if errors.Is(err, groupapp.ErrInviteCodeUnavailable) {
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

	var req JoinGroupRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	ctx := c.Request.Context()
	groupMemberApp, groupApp, err := rh.groupApp.Join(ctx, userID, req.InviteCode)

	if err != nil {
		if errors.Is(err, groupapp.ErrGroupFull) {
			c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	conversation, err := rh.conversationApp.FindConversationByGroupID(ctx, groupApp.GroupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(
			http.StatusInternalServerError,
			"加入群聊成功，但查询会话失败，请刷新会话列表",
		))
		return
	}

	c.JSON(http.StatusOK, response.Success(JoinGroupResponse{
		GroupID:         groupApp.GroupID,
		Description:    groupApp.Description,
		GroupName:       groupApp.GroupName,
		AvatarURL:      groupApp.AvatarURL,
		MemberCount:    groupApp.MemberCount,
		Role:           groupMemberApp.Role,
		JoinTime:       groupMemberApp.JoinTime,
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
	groupID := c.Param("groupId")
	if groupID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if err := rh.groupApp.Leave(c.Request.Context(), userID, groupID); err != nil {
		if errors.Is(err, groupapp.ErrNotGroupMember) {
			c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
			return
		}
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}
	c.JSON(http.StatusOK, response.Success(nil))
}
