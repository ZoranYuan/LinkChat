package file

import (
	fileapp "IM_backend/internal/application/file"
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

type AttachmentAccessURLsRequest struct {
	AttachmentIDs []string `json:"attachmentIds" binding:"required,min=1,max=100"`
}

type AttachmentAccessURLsResponse struct {
	Attachments []*fileapp.AttachmentAccessURLDTO `json:"attachments"`
}

type UploadInitRequest struct {
	FileName    string `json:"fileName" binding:"required"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size" binding:"required"`
	FileHash    string `json:"fileHash" binding:"required"`
	ChunkSize   int64  `json:"chunkSize,omitempty"`
	TotalChunks int    `json:"totalChunks,omitempty"`
}

type MultipartPartsPresignRequest struct {
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
