package conversation

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r gin.IRoutes, h *Handler) {
	r.GET("", h.GetUserConversations)
}
