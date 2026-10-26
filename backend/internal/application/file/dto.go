package file

import (
	domainfile "IM_backend/internal/domain/file"
)

func toFileDTO(file *domainfile.File) *FileDTO {
	return &FileDTO{
		FileID:      file.FileID,
		FileName:    file.FileName,
		ContentType: file.ContentType,
		Size:        file.Size,
		CreatedAt:   file.CreatedAt,
	}
}

// UploadInitDTO 是统一上传入口的请求。后端根据文件大小选择直传或分片上传。
type UploadInitDTO struct {
	UploaderID  string
	FileName    string
	ContentType string
	Size        int64
	FileHash    string
	ChunkSize   int64
	TotalChunks int
}

// UploadInitResDTO 统一描述直传和分片上传的初始化结果。
// 直传使用 URL，分片上传使用 ChunkSize、TotalChunks 和 UploadedParts。
type UploadInitResDTO struct {
	Status        string `json:"status"`
	UploadMode    string `json:"uploadMode,omitempty"`
	UploadID      string `json:"uploadId,omitempty"`
	FileID        string `json:"fileId,omitempty"`
	URL           string `json:"url,omitempty"`
	ExpiresAt     int64  `json:"expiresAt,omitempty"`
	ChunkSize     int64  `json:"chunkSize,omitempty"`
	TotalChunks   int    `json:"totalChunks,omitempty"`
	UploadedParts []int  `json:"uploadedParts,omitempty"`
}

type MultipartPartURLDTO struct {
	UploadID   string `json:"uploadId"`
	PartNumber int    `json:"partNumber"`
	URL        string `json:"url"`
}

type AttachmentAccessURLDTO struct {
	AttachmentID string `json:"attachmentId"`
	FileID       string `json:"fileId"`
	FileName     string `json:"fileName"`
	ContentType  string `json:"contentType"`
	Size         int64  `json:"size"`
	MediaURL     string `json:"mediaUrl"`
	ThumbURL     string `json:"thumbUrl,omitempty"`
	ExpiresAt    int64  `json:"expiresAt"`
	MessageType  int    `json:"cType"`
	Width        int    `json:"width,omitempty"`
	Height       int    `json:"height,omitempty"`
	DurationMs   *int64 `json:"durationMs,omitempty"`
}

type FileDTO struct {
	FileID      string
	FileName    string
	ContentType string
	Size        int64
	CreatedAt   int64
}
