package infrafile

import (
	"context"
	"errors"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	domainfile "IM_backend/core/domain/file"
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
		if strings.Contains(mysqlErr.Message, "uk_file_hash") || strings.Contains(mysqlErr.Message, "uk_file_uploader_hash") {
			// TODO: 这里返回 ErrDuplicatedKey 是不是更好呢？
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

// FindFileByHash 全局按 hash 查已上传文件（不限定 uploader），用于标准方案的全局秒传。
func (r *FileRepository) FindFileByHash(ctx context.Context, fileHash string) (*domainfile.File, error) {
	if fileHash == "" {
		return nil, nil
	}
	var m FileModel
	if err := r.db.WithContext(ctx).
		Where("file_hash = ? AND status = ?", fileHash, domainfile.FileStatusUploaded).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return toFileDomain(&m), nil
}

func (r *FileRepository) ListFilesEligibleForCleanup(ctx context.Context, before int64, now int64, limit int) ([]*domainfile.File, error) {
	if limit <= 0 {
		return []*domainfile.File{}, nil
	}
	var models []FileModel
	err := r.db.WithContext(ctx).Table("files AS f").
		// 找出创建时间在 before 前的文件，默认是 1 天
		Where("f.created_at < ? AND f.status IN ?", before, []string{
			domainfile.FileStatusUploaded,
			domainfile.FileStatusDeleting,
		}).
		// 查找没有引用的 file以及引用过期的文件
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
