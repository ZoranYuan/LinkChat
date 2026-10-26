package infrafile

import (
	"context"
	"errors"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainfile "IM_backend/internal/domain/file"
)

type FileRepository struct {
	db *gorm.DB
}

func NewFileRepository(db *gorm.DB) domainfile.FileRepository {
	return &FileRepository{db: db}
}

func (r *FileRepository) WithTx(tx any) domainfile.FileRepository {
	return &FileRepository{db: tx.(*gorm.DB)}
}

func (r *FileRepository) CreateFile(ctx context.Context, file *domainfile.File) error {
	err := r.db.WithContext(ctx).Create(toFileModel(file)).Error

	var mysqlErr *mysqlDriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		if strings.Contains(mysqlErr.Message, "uk_file_uploader_hash") {
			return domainfile.ErrFileHashConflict
		}
		return domainfile.ErrFileIdentityConflict
	}

	return err
}

func (r *FileRepository) FindFileByID(ctx context.Context, fileID string) (*domainfile.File, error) {
	var m FileModel
	if err := r.db.WithContext(ctx).Where("file_id = ?", fileID).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return toFileDomain(&m), nil
}

func (r *FileRepository) FindUploadedFileByIDAndUploader(ctx context.Context, fileID string, uploaderID string) (*domainfile.File, error) {
	if fileID == "" || uploaderID == "" {
		return nil, nil
	}

	var m FileModel
	if err := r.db.WithContext(ctx).
		Where("file_id = ? AND uploader_id = ? AND status = ?", fileID, uploaderID, domainfile.FileStatusUploaded).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return toFileDomain(&m), nil
}

// FindUploadedFileByIDAndUploaderForUpdate 查询指定上传者拥有的已上传文件，并锁定该行。
func (r *FileRepository) FindUploadedFileByIDAndUploaderForUpdate(ctx context.Context, fileID string, uploaderID string) (*domainfile.File, error) {
	if fileID == "" || uploaderID == "" {
		return nil, nil
	}

	var m FileModel
	if err := r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("file_id = ? AND uploader_id = ? AND status = ?", fileID, uploaderID, domainfile.FileStatusUploaded).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return toFileDomain(&m), nil
}

func (r *FileRepository) FindUploadedFileByUploaderAndHash(ctx context.Context, uploaderID string, fileHash string) (*domainfile.File, error) {
	if uploaderID == "" || fileHash == "" {
		return nil, nil
	}
	var m FileModel
	if err := r.db.WithContext(ctx).
		Where("uploader_id = ? AND file_hash = ? AND status = ?", uploaderID, fileHash, domainfile.FileStatusUploaded).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return toFileDomain(&m), nil
}

func (r *FileRepository) ListFilesByFileIDs(ctx context.Context, fileIDs []string) (map[string]*domainfile.File, error) {
	if len(fileIDs) == 0 {
		return map[string]*domainfile.File{}, nil
	}
	var models []FileModel
	if err := r.db.WithContext(ctx).Where("file_id IN (?)", fileIDs).Find(&models).Error; err != nil {
		return nil, err
	}
	result := make(map[string]*domainfile.File, len(models))
	for i := range models {
		result[models[i].FileID] = toFileDomain(&models[i])
	}
	return result, nil
}

func (r *FileRepository) ListFilesEligibleForCleanup(ctx context.Context, before int64, now int64, limit int) ([]*domainfile.File, error) {
	if limit <= 0 {
		return []*domainfile.File{}, nil
	}
	var models []FileModel
	err := r.db.WithContext(ctx).Table("files AS f").
		Where("f.created_at < ? AND f.status IN ?", before, []string{
			domainfile.FileStatusUploaded,
			domainfile.FileStatusDeleting,
		}).
		Where("NOT EXISTS (SELECT 1 FROM message_attachments ma WHERE ma.file_id = f.file_id AND ma.expire_at > ?)", now).
		Order("f.created_at ASC").Limit(limit).Find(&models).Error
	if err != nil {
		return nil, err
	}
	result := make([]*domainfile.File, 0, len(models))
	for i := range models {
		result = append(result, toFileDomain(&models[i]))
	}
	return result, nil
}

func (r *FileRepository) MarkFileAsDeletingIfEligible(ctx context.Context, fileID string, before int64, now int64) (bool, error) {
	result := r.db.WithContext(ctx).Model(&FileModel{}).
		Where("file_id = ? AND status = ? AND created_at < ?", fileID, domainfile.FileStatusUploaded, before).
		Where("NOT EXISTS (SELECT 1 FROM message_attachments ma WHERE ma.file_id = ? AND ma.expire_at > ?)", fileID, now).
		Update("status", domainfile.FileStatusDeleting)
	return result.RowsAffected == 1, result.Error
}

func (r *FileRepository) DeleteFileIfMarkedDeleting(ctx context.Context, fileID string) (bool, error) {
	result := r.db.WithContext(ctx).Where("file_id = ? AND status = ?", fileID, domainfile.FileStatusDeleting).Delete(&FileModel{})
	return result.RowsAffected == 1, result.Error
}
