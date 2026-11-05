package monitoringhttp

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterRoutes(r *gin.Engine) {
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
}
