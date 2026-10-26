package domainfile

import (
	"time"
)

const (
	FileStatusUploaded = "uploaded"
	FileStatusDeleting = "deleting"
)

type File struct {
	FileID      string `json:"FileId"`
	UploaderID  string `json:"UploaderId"`
	Bucket      string
	ObjectKey   string
	FileName    string
	ContentType string
	FileHash    string
	Size        int64
	CreatedAt   int64
	Status      string
}

func NewFile(fileID, uploaderID, bucket, objectKey, fileName, contentType string, size int64) *File {
	return &File{
		FileID:      fileID,
		UploaderID:  uploaderID,
		Bucket:      bucket,
		ObjectKey:   objectKey,
		FileName:    fileName,
		ContentType: contentType,
		Size:        size,
		CreatedAt:   time.Now().UnixMilli(),
		Status:      FileStatusUploaded,
	}
}
