package domainfile

import (
	"context"
)

type FileRepository interface {
	CreateFile(ctx context.Context, file *File) error
	FindFileByID(ctx context.Context, fileID string) (*File, error)
	// FindFileByHash 按内容 hash 全局查已上传文件，用于标准方案的全局秒传，命中即复用物理对象。
	FindFileByHash(ctx context.Context, fileHash string) (*File, error)
	ListFilesEligibleForCleanup(ctx context.Context, before int64, now int64, limit int) ([]*File, error)
	MarkFileAsDeletingIfEligible(ctx context.Context, fileID string, before int64, now int64) (bool, error)
	DeleteFileIfMarkedDeleting(ctx context.Context, fileID string) (bool, error)
	WithTx(tx any) FileRepository
}

type FileUploadRepository interface {
	CreateFileUpload(ctx context.Context, upload Upload) error
	FindFileUploadByID(ctx context.Context, uploadID string) (*Upload, error)
	FindActiveFileUploadByUploaderAndHash(ctx context.Context, uploaderID, fileHash string, now int64) (*Upload, error)
	MarkCompleted(ctx context.Context, uploadID, fileID string, completedAt int64) (bool, error)
	WithTx(tx any) FileUploadRepository
}
