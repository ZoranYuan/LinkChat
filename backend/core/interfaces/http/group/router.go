package group

import (
	"github.com/gin-gonic/gin"

	"IM_backend/core/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, rh *Handler, auth *middleware.AuthMiddleware) {
	rr := r.Group("/groups", auth.JWTAuthMiddleware())
	rr.POST("", rh.Create)
	rr.GET("/:groupId/invite-code", rh.Invite)
	rr.POST("/join", rh.Join)
	rr.POST("/:groupId/leave", rh.Leave)
}
