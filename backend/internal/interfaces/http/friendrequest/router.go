package friendrequest

import (
	"github.com/gin-gonic/gin"

	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, fh *Handler, auth *middleware.AuthMiddleware) {
	fg := r.Group("/friend-requests", auth.JWTAuthMiddleware())
	fg.POST("", fh.Create)
	fg.GET("", fh.List)
	fg.POST("/actions", fh.OperateRequest)
}
