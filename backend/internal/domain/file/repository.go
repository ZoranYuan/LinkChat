package domainfile

import (
	"context"
)

type FileRepository interface {
	CreateFile(ctx context.Context, file *File) error
	FindFileByID(ctx context.Context, fileID string) (*File, error)
	FindUploadedFileByIDAndUploader(ctx context.Context, fileID string, uploaderID string) (*File, error)
	FindUploadedFileByIDAndUploaderForUpdate(ctx context.Context, fileID string, uploaderID string) (*File, error)
	FindUploadedFileByUploaderAndHash(ctx context.Context, uploaderID string, fileHash string) (*File, error)
	ListFilesByFileIDs(ctx context.Context, fileIDs []string) (map[string]*File, error)
	ListFilesEligibleForCleanup(ctx context.Context, before int64, now int64, limit int) ([]*File, error)
	MarkFileAsDeletingIfEligible(ctx context.Context, fileID string, before int64, now int64) (bool, error)
	DeleteFileIfMarkedDeleting(ctx context.Context, fileID string) (bool, error)
	WithTx(tx any) FileRepository
}

type FileUploadRepository interface {
	CreateFileUpload(ctx context.Context, upload Upload) error
	FindFileUploadByID(ctx context.Context, uploadID string) (*Upload, error)
	FindActiveFileUploadByUploaderAndHash(ctx context.Context, uploaderID, fileHash string, now int64) (*Upload, error)
	ClaimExpiredFileUploads(ctx context.Context, now int64, staleBefore int64, limit int) ([]Upload, error)
	MarkExpired(ctx context.Context, uploadID, lockToken string, updatedAt int64) (bool, error)
	MarkCleanupRetry(ctx context.Context, uploadID, lockToken string, nextRetryAt int64, lastError string, updatedAt int64) (bool, error)
	MarkCleanupFailed(ctx context.Context, uploadID, lockToken string, lastError string, updatedAt int64) (bool, error)
	MarkCompleted(ctx context.Context, uploadID, fileID string, completedAt int64) (bool, error)
	DeleteFileUploadByUploadID(ctx context.Context, uploadID string) error
	WithTx(tx any) FileUploadRepository
}
