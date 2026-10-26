package conversation

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	conversationapp "IM_backend/internal/application/conversation"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	app *conversationapp.UserConversationApplication
}

func NewHandler(app *conversationapp.UserConversationApplication) *Handler {
	return &Handler{
		app: app,
	}
}

func (h *Handler) GetUserConversations(c *gin.Context) {
	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	convs, err := h.app.GetUserConversationsByUserID(c, userID)

	if err != nil {
		log.Println("用户会话获取失败", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "用户会话获取失败"))
		return
	}

	c.JSON(http.StatusOK, response.Success(convs))
}
