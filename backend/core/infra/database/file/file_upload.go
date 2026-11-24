package infrafile

import (
	"context"
	"errors"

	"gorm.io/gorm"

	domainfile "IM_backend/core/domain/file"
)

type FileUploadRepository struct {
	db *gorm.DB
}

func NewFileUploadRepository(db *gorm.DB) domainfile.FileUploadRepository {
	return &FileUploadRepository{db: db}
}

func (r *FileUploadRepository) WithTx(tx any) domainfile.FileUploadRepository {
	return &FileUploadRepository{db: tx.(*gorm.DB)}
}

func (r *FileUploadRepository) CreateFileUpload(ctx context.Context, upload domainfile.Upload) error {
	var storageUploadID *string
	if upload.StorageUploadID != "" {
		storageUploadID = &upload.StorageUploadID
	}
	var chunkSize *int64
	if upload.ChunkSize > 0 {
		chunkSize = &upload.ChunkSize
	}
	var totalChunks *int
	if upload.TotalChunks > 0 {
		totalChunks = &upload.TotalChunks
	}
	return r.db.WithContext(ctx).Create(&FileUploadModel{
		UploadID:        upload.UploadID,
		FileID:          upload.FileID,
		UploaderID:      upload.UploaderID,
		UploadMode:      upload.UploadMode,
		StorageUploadID: storageUploadID,
		FileHash:        upload.FileHash,
		ObjectKey:       upload.ObjectKey,
		FileName:        upload.FileName,
		ContentType:     upload.ContentType,
		ExpectedSize:    upload.ExpectedSize,
		ChunkSize:       chunkSize,
		TotalChunks:     totalChunks,
		Status:          upload.Status,
		ExpiresAt:       upload.ExpiresAt,
		CreatedAt:       upload.CreatedAt,
		UpdatedAt:       upload.UpdatedAt,
	}).Error
}

func (r *FileUploadRepository) FindFileUploadByID(ctx context.Context, uploadID string) (*domainfile.Upload, error) {
	var row FileUploadModel
	if err := r.db.WithContext(ctx).Where("upload_id = ?", uploadID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	record := toUpload(row)
	return &record, nil
}

func (r *FileUploadRepository) FindActiveFileUploadByUploaderAndHash(ctx context.Context, uploaderID, fileHash string, now int64) (*domainfile.Upload, error) {
	var row FileUploadModel
	err := r.db.WithContext(ctx).
		Where("uploader_id = ? AND file_hash = ? AND status = ? AND expires_at > ?", uploaderID, fileHash, "uploading", now).
		Order("created_at DESC").
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	record := toUpload(row)
	return &record, nil
}

func (r *FileUploadRepository) MarkCompleted(ctx context.Context, uploadID, fileID string, completedAt int64) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&FileUploadModel{}).
		Where("upload_id = ? AND status = ?", uploadID, "uploading").
		Updates(map[string]any{
			"status":       "completed",
			"file_id":      fileID,
			"completed_at": completedAt,
			"updated_at":   completedAt,
		})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return true, nil
	}
	var row struct {
		Status string
		FileID string `gorm:"column:file_id"`
	}

	// TODO: 这里的二阶段查询是什么意思呢？
	if err := r.db.WithContext(ctx).
		Model(&FileUploadModel{}).
		Select("status, file_id").
		Where("upload_id = ?", uploadID).
		Scan(&row).Error; err != nil {
		return false, err
	}
	return row.Status == "completed" && row.FileID == fileID, nil
}

func toUpload(row FileUploadModel) domainfile.Upload {
	chunkSize := int64(0)
	if row.ChunkSize != nil {
		chunkSize = *row.ChunkSize
	}
	totalChunks := 0
	if row.TotalChunks != nil {
		totalChunks = *row.TotalChunks
	}
	storageUploadID := ""
	if row.StorageUploadID != nil {
		storageUploadID = *row.StorageUploadID
	}

	return domainfile.Upload{
		UploadID:        row.UploadID,
		FileID:          row.FileID,
		UploaderID:      row.UploaderID,
		UploadMode:      row.UploadMode,
		StorageUploadID: storageUploadID,
		FileHash:        row.FileHash,
		ObjectKey:       row.ObjectKey,
		FileName:        row.FileName,
		ContentType:     row.ContentType,
		ExpectedSize:    row.ExpectedSize,
		ChunkSize:       chunkSize,
		TotalChunks:     totalChunks,
		Status:          row.Status,
		ExpiresAt:       row.ExpiresAt,
		CreatedAt:       row.CreatedAt,
		UpdatedAt:       row.UpdatedAt,
	}
}
