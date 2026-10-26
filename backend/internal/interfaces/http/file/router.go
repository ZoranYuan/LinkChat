package file

import (
	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(r gin.IRoutes, h *Handler, limiter *middleware.RateLimitMiddleware, policy ratelimitport.Policy) {
	r.POST("/uploads/init", limiter.ByIP("file_upload_init", policy), h.InitUpload)
	r.POST("/uploads/:uploadId/complete", limiter.ByIP("file_upload_complete", policy), h.CompleteUpload)
	r.POST("/uploads/:uploadId/parts/presign", limiter.ByIP("file_upload_presign", policy), h.PresignMultipartParts)
	r.POST("/attachments/access-urls", limiter.ByIP("file_attachment_access_urls", policy), h.GetAttachmentAccessURLs)
	r.GET("/attachments/:attachmentId/access-url", h.GetAttachmentAccessURL)
}
