package message

import (
	"github.com/gin-gonic/gin"

	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, mh *Handler, auth *middleware.AuthMiddleware) {
	mr := r.Group("/messages", auth.JWTAuthMiddleware())
	mr.GET("/history", mh.GetHistoryMessages)
	mr.GET("/offline", mh.GetOfflineMessages)
	mr.GET("/sync", mh.SyncMessages)
	mr.GET("/seqs", mh.GetMessagesBySeqs)
}
