package friendrequest

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(fg gin.IRoutes, fh *Handler) {
	fg.POST("", fh.Create)
	fg.GET("", fh.List)
	fg.POST("/actions", fh.OperateRequest)
}
