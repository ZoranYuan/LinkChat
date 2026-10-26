package objectstorage

import (
	"context"
	"io"
	"time"
)

type ObjectStorage interface {
	Health(ctx context.Context) error
	PresignedPutURL(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
	StatObject(ctx context.Context, objectKey string) (int64, error)
	OpenObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, objectKey string) error
	CreateMultipartUpload(ctx context.Context, objectKey string, contentType string) (string, error)
	PresignMultipartPart(ctx context.Context, objectKey string, uploadID string, partNumber int, ttl time.Duration) (string, error)
	ListMultipartParts(ctx context.Context, objectKey string, uploadID string) ([]MultipartPart, error)
	CompleteMultipartUpload(ctx context.Context, objectKey string, uploadID string, parts []MultipartPart) error
	AbortMultipartUpload(ctx context.Context, objectKey string, uploadID string) error
	PresignedGetURL(ctx context.Context,
		objectKey string,
		ttl time.Duration,
		contentType string,
		fileName string,
	) (string, error)
	Bucket() string
}

type MultipartPart struct {
	PartNumber int
	ETag       string
	Size       int64
}
