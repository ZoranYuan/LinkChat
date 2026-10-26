package friend

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(fr gin.IRoutes, fh *Handler) {
	fr.GET("", fh.GetFriendList)
}
