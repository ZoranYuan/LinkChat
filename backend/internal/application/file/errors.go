package file

import (
	"errors"
	"fmt"

	domainfile "IM_backend/internal/domain/file"
)

type UploadIncompleteError struct {
	MissingParts []int `json:"missingParts"`
	InvalidParts []int `json:"invalidParts"`
}

func (e *UploadIncompleteError) Error() string {
	return fmt.Sprintf("分片未上传完成，缺失 %d 个，异常 %d 个", len(e.MissingParts), len(e.InvalidParts))
}

func (e *UploadIncompleteError) Unwrap() error {
	return ErrUploadNotCompleted
}

var (
	ErrFileNotFound         = errors.New("文件不存在")
	ErrFileSizeMismatch     = errors.New("文件大小校验失败")
	ErrFileHashMismatch     = errors.New("文件哈希校验失败")
	ErrFileTooLarge         = errors.New("文件超过大小限制")
	ErrFileHashConflict     = domainfile.ErrFileHashConflict
	ErrFileIdentityConflict = domainfile.ErrFileIdentityConflict
	ErrInvalidUpload        = errors.New("上传任务不存在")
	ErrInvalidMIME          = errors.New("不允许的媒体资源类型")
	ErrInvalidPart          = errors.New("分片参数错误")
	ErrUploadNotCompleted   = errors.New("分片未上传完成")
	ErrUnsupportedFileType  = errors.New("不支持该文件类型")
	ErrUploadUnauthorized   = errors.New("无权操作该上传任务")
	ErrUploadBusy           = errors.New("文件正在上传")
	ErrFileCompleteLockLost = errors.New("文件完成锁已失效")
	ErrTooManyParts         = errors.New("分片数量超过限制")
)
