package file

import domainfile "IM_backend/core/domain/file"

func toFileDTO(file *domainfile.File) *FileDTO {
	return &FileDTO{
		FileID:      file.FileID,
		FileName:    file.FileName,
		ContentType: file.ContentType,
		Size:        file.Size,
		CreatedAt:   file.CreatedAt,
	}
}

type PartLimitDTO struct {
	MinPartSize     int64 `json:"minPartSize"`
	MaxPartSize     int64 `json:"maxPartSize"`
	DefaultPartSize int64 `json:"defaultPartSize"`
	MaxPartCount    int   `json:"maxPartCount"`
}

type PartSizeReqDTO struct {
	Size int64 `json:"size" binding:"required"`
}

type PartSizeDTO struct {
	PartSize    int64 `json:"partSize"`
	TotalChunks int64 `json:"totalChunks"`
}

type InitiateMultipartUploadReqDTO struct {
	UploaderID string

	// TODO: 这个 Name 的作用是什么？
	Name        string // 对象名，普通用户必须以 "userID/" 开头
	FileName    string
	ContentType string
	Size        int64
	FileHash    string
	PartSize    int64 // 客户端按 PartSize 接口返回值填入
}

type InitiateMultipartUploadResDTO struct {
	Status        string `json:"status"`
	UploadID      string `json:"uploadId,omitempty"`
	FileID        string `json:"fileId,omitempty"`
	Name          string `json:"name,omitempty"`
	PartSize      int64  `json:"partSize,omitempty"`
	TotalChunks   int    `json:"totalChunks,omitempty"`
	UploadedParts []int  `json:"uploadedParts,omitempty"`
}

type MultipartPartDTO struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
}

type AuthSignPartDTO struct {
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
}

type FileDTO struct {
	FileID      string
	FileName    string
	ContentType string
	Size        int64
	CreatedAt   int64
}
