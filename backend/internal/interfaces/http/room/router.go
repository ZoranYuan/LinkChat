package room

import (
	"github.com/gin-gonic/gin"

	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, rh *Handler, auth *middleware.AuthMiddleware) {
	rr := r.Group("/rooms", auth.JWTAuthMiddleware())
	rr.POST("", rh.Create)
	rr.GET("/:roomId/invite-code", rh.Invite)
	rr.POST("/join", rh.Join)
	rr.POST("/:roomId/leave", rh.Leave)
}
