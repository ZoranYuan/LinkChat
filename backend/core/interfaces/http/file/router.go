package file

import (
	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/core/application/ports/ratelimit"
	"IM_backend/core/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, h *Handler, auth *middleware.AuthMiddleware, limiter *middleware.RateLimitMiddleware, policy ratelimitport.Policy) {
	group := r.Group("/files", auth.JWTAuthMiddleware())
	// 分片规格协商（OpenIM PartLimit / PartSize）：轻量读接口，不单独限流
	group.GET("/uploads/part-limit", h.PartLimit)
	group.POST("/uploads/part-size", h.PartSize)
	group.POST("/uploads/initiate-multipart", limiter.ByIP("file_upload_init", policy), h.InitiateMultipartUpload)
	group.POST("/uploads/:uploadId/complete-multipart", limiter.ByIP("file_upload_complete", policy), h.CompleteMultipartUpload)
	group.POST("/uploads/:uploadId/parts/auth-sign", limiter.ByIP("file_upload_presign", policy), h.AuthSign)
	group.POST("/attachments/access-url", limiter.ByIP("file_attachment_access_urls", policy), h.AccessURLs)
	group.GET("/attachments/:attachmentId/access-url", h.AccessURL)
}
