package ws

import (
	"github.com/gin-gonic/gin"

	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterWSRouter(r *gin.RouterGroup, wh *Handler, auth *middleware.AuthMiddleware) {
	r.Use(auth.JWTAuthMiddleware()).GET("/ws", wh.Upgrade)
}
