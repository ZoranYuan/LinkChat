package message

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(mr gin.IRoutes, mh *Handler) {
	mr.GET("/history", mh.GetHistoryMessages)
	mr.GET("/offline", mh.GetOfflineMessages)
	mr.GET("/sync", mh.SyncMessages)
	mr.GET("/seqs", mh.GetMessagesBySeqs)
}
