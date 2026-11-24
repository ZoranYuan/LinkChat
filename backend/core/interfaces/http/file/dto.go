package file

import (
	fileapp "IM_backend/core/application/file"
)

type FileResponse struct {
	Status       string `json:"status"`
	UploadID     string `json:"uploadId,omitempty"`
	FileID       string `json:"fileId,omitempty"`
	FileName     string `json:"fileName,omitempty"`
	ContentType  string `json:"contentType,omitempty"`
	Size         int64  `json:"size,omitempty"`
	CreatedAt    int64  `json:"createdAt,omitempty"`
	MissingParts []int  `json:"missingParts,omitempty"`
	InvalidParts []int  `json:"invalidParts,omitempty"`
}

type AccessURLsRequest struct {
	AttachmentIDs []string `json:"attachmentIds" binding:"required,min=1,max=100"`
}

type AccessURLsResponse struct {
	Attachments []*fileapp.AttachmentAccessURLDTO `json:"attachments"`
}

// UploadInitRequest 对齐 OpenIM InitiateMultipartUploadReq：
// hash/size/partSize/contentType/name 是标准字段。
type UploadInitRequest struct {
	Name        string `json:"name" binding:"required"` // 对象名，须以 userID/ 开头
	FileName    string `json:"fileName,omitempty"`      // 展示文件名，缺省用 name
	ContentType string `json:"contentType" binding:"required"`
	Size        int64  `json:"size" binding:"required,gt=0"`
	FileHash    string `json:"fileHash" binding:"required"`
	PartSize    int64  `json:"partSize,omitempty"` // 按 PartSize 接口返回填入
}

// CompleteMultipartRequest 对齐 OpenIM CompleteMultipartUploadReq：客户端提交 parts[ETag]。
type CompleteMultipartRequest struct {
	Parts []fileapp.MultipartPartDTO `json:"parts"`
}

type AuthSignRequest struct {
	PartNumbers []int `json:"partNumbers" binding:"required"`
}

func toFileResponse(dto *fileapp.FileDTO) FileResponse {
	return FileResponse{
		Status:      "completed",
		FileID:      dto.FileID,
		FileName:    dto.FileName,
		ContentType: dto.ContentType,
		Size:        dto.Size,
		CreatedAt:   dto.CreatedAt,
	}
}

func toUploadIncompleteResponse(uploadID string, incompleteErr *fileapp.UploadIncompleteError) FileResponse {
	return FileResponse{
		Status:       "uploading",
		UploadID:     uploadID,
		MissingParts: incompleteErr.MissingParts,
		InvalidParts: incompleteErr.InvalidParts,
	}
}
