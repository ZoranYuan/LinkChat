package file

import (
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	fileapp "IM_backend/internal/application/file"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	app *fileapp.FileApplication
}

func NewHandler(app *fileapp.FileApplication) *Handler {
	return &Handler{app: app}
}

func (h *Handler) InitUpload(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req UploadInitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	dto, err := h.app.InitUpload(c.Request.Context(), fileapp.UploadInitDTO{
		UploaderID:  userID,
		FileName:    req.FileName,
		ContentType: req.ContentType,
		Size:        req.Size,
		FileHash:    req.FileHash,
		ChunkSize:   req.ChunkSize,
		TotalChunks: req.TotalChunks,
	})
	if err != nil {
		h.writeUploadInitError(c, err)
		return
	}
	c.JSON(http.StatusOK, response.Success(dto))
}

func (h *Handler) CompleteUpload(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}
	uploadID := c.Param("uploadId")
	if uploadID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	dto, err := h.app.CompleteUpload(c.Request.Context(), uploadID, userID)
	if err != nil {
		h.writeUploadCompleteError(c, err)
		return
	}
	c.JSON(http.StatusOK, response.Success(toFileResponse(dto)))
}

func (h *Handler) writeUploadInitError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, fileapp.ErrInvalidUpload), errors.Is(err, fileapp.ErrInvalidPart), errors.Is(err, fileapp.ErrInvalidMIME):
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, err.Error()))
	case errors.Is(err, fileapp.ErrFileTooLarge), errors.Is(err, fileapp.ErrTooManyParts):
		c.JSON(http.StatusRequestEntityTooLarge, response.Error(http.StatusRequestEntityTooLarge, err.Error()))
	case errors.Is(err, fileapp.ErrUploadBusy):
		c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
	default:
		log.Println("初始化上传失败：", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "初始化上传失败"))
	}
}

func (h *Handler) writeUploadCompleteError(c *gin.Context, err error) {
	var incompleteErr *fileapp.UploadIncompleteError
	if errors.As(err, &incompleteErr) {
		c.JSON(http.StatusConflict, response.ErrorWithData(
			http.StatusConflict,
			incompleteErr.Error(),
			toUploadIncompleteResponse(c.Param("uploadId"), incompleteErr),
		))
		return
	}
	switch {
	case errors.Is(err, fileapp.ErrUploadUnauthorized):
		c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, err.Error()))
	case errors.Is(err, fileapp.ErrUploadNotCompleted), errors.Is(err, fileapp.ErrInvalidUpload), errors.Is(err, fileapp.ErrUploadBusy):
		c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
	case errors.Is(err, fileapp.ErrFileSizeMismatch), errors.Is(err, fileapp.ErrFileHashMismatch), errors.Is(err, fileapp.ErrUnsupportedFileType):
		c.JSON(http.StatusUnprocessableEntity, response.Error(http.StatusUnprocessableEntity, err.Error()))
	default:
		log.Println("完成上传失败：", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "完成上传失败"))
	}
}

func (h *Handler) PresignMultipartParts(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}
	uploadID := c.Param("uploadId")
	if uploadID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	var req MultipartPartsPresignRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "分片参数错误"))
		return
	}
	dtos, err := h.app.PresignMultipartParts(c.Request.Context(), uploadID, userID, req.PartNumbers)
	if err != nil {
		if errors.Is(err, fileapp.ErrUploadUnauthorized) {
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权操作该上传任务"))
			return
		}
		log.Println("批量生成分片上传地址失败：", err)
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "分片上传任务无效"))
		return
	}
	c.JSON(http.StatusOK, response.Success(dtos))
}

func (h *Handler) GetAttachmentAccessURL(c *gin.Context) {
	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	attachmentID := c.Param("attachmentId")
	if attachmentID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	dto, err := h.app.GetAttachmentAccessURL(c.Request.Context(), userID, attachmentID)
	if err != nil {
		if errors.Is(err, fileapp.ErrFileNotFound) {
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "文件不存在"))
			return
		}
		if errors.Is(err, fileapp.ErrUploadUnauthorized) {
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权访问该附件"))
			return
		}
		log.Println("获取文件失败：", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "获取文件失败"))
		return
	}

	c.JSON(http.StatusOK, response.Success(dto))
}

func (h *Handler) GetAttachmentAccessURLs(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req AttachmentAccessURLsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	dtos, err := h.app.GetAttachmentAccessURLs(c.Request.Context(), userID, req.AttachmentIDs)
	if err != nil {
		if errors.Is(err, fileapp.ErrUploadUnauthorized) {
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权访问该附件"))
			return
		}
		log.Println("批量获取附件访问地址失败：", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "获取附件失败"))
		return
	}

	c.JSON(http.StatusOK, response.Success(AttachmentAccessURLsResponse{Attachments: dtos}))
}
