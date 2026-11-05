package file

import (
	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r *gin.RouterGroup, h *Handler, auth *middleware.AuthMiddleware, limiter *middleware.RateLimitMiddleware, policy ratelimitport.Policy) {
	group := r.Group("/files", auth.JWTAuthMiddleware())
	group.POST("/uploads/init", limiter.ByIP("file_upload_init", policy), h.InitUpload)
	group.POST("/uploads/:uploadId/complete", limiter.ByIP("file_upload_complete", policy), h.CompleteUpload)
	group.POST("/uploads/:uploadId/parts/presign", limiter.ByIP("file_upload_presign", policy), h.PresignMultipartParts)
	group.POST("/attachments/access-urls", limiter.ByIP("file_attachment_access_urls", policy), h.GetAttachmentAccessURLs)
	group.GET("/attachments/:attachmentId/access-url", h.GetAttachmentAccessURL)
}
