package conversation

import (
	"github.com/gin-gonic/gin"

	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, h *Handler, auth *middleware.AuthMiddleware) {
	group := r.Group("/conversations", auth.JWTAuthMiddleware())
	group.GET("", h.GetUserConversations)
}
