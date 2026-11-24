package friend

import (
	"github.com/gin-gonic/gin"

	"IM_backend/core/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, fh *Handler, auth *middleware.AuthMiddleware) {
	fr := r.Group("/friends", auth.JWTAuthMiddleware())
	fr.GET("", fh.GetFriendList)
}
