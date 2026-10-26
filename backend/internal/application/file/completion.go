package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	filecache "IM_backend/internal/application/ports/cache/file"
	domainfile "IM_backend/internal/domain/file"
)

func (a *FileApplication) verifyAndDetectCompletedObject(
	ctx context.Context,
	objectKey string,
	expectedSize int64,
	expectedHash string,
) (result string, err error) {
	size, err := a.storage.StatObject(ctx, objectKey)
	if err != nil {
		return "", ErrUploadNotCompleted
	}
	if size != expectedSize {
		return "", ErrFileSizeMismatch
	}

	object, err := a.storage.OpenObject(ctx, objectKey)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := object.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	hasher := sha256.New()
	header := make([]byte, 512)
	n, readErr := io.ReadFull(object, header)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return "", readErr
	}

	_, _ = hasher.Write(header[:n])
	written := int64(n)
	rest, err := io.Copy(hasher, object)
	if err != nil {
		return "", err
	}
	written += rest

	if written != expectedSize {
		return "", ErrFileSizeMismatch
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(actualHash, expectedHash) {
		return "", ErrFileHashMismatch
	}

	return strings.ToLower(http.DetectContentType(header[:n])), nil
}

func (a *FileApplication) buildVerifiedFile(ctx context.Context, meta *filecache.UploadMeta) (*domainfile.File, error) {
	detectedType, err := a.verifyAndDetectCompletedObject(ctx, meta.ObjectKey, meta.Size, meta.FileHash)
	if err != nil {
		return nil, err
	}
	if err := a.validateContentType(detectedType); err != nil {
		return nil, err
	}
	file := domainfile.NewFile(meta.FileID, meta.UploaderID, meta.Bucket, meta.ObjectKey, meta.FileName, detectedType, meta.Size)
	file.FileHash = meta.FileHash
	return file, nil
}

// CompleteUpload 统一完成锁、状态复查、对象校验与落库，分片上传仅额外执行合并。
func (a *FileApplication) CompleteUpload(ctx context.Context, uploadID, uploaderID string) (*FileDTO, error) {
	meta, err := a.loadUploadMeta(ctx, uploadID, uploaderID)
	if err != nil {
		return nil, err
	}
	if meta.Status == uploadStatusCompleted {
		return a.GetFileForUser(ctx, meta.FileID, uploaderID)
	}
	if meta.Status != uploadStatusUploading || meta.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}

	unlock, lockLost, err := a.acquireFileCompleteLock(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// 加锁后重新检查状态和有效期，避免等待期间上传任务发生变化。
	meta, err = a.loadUploadMeta(ctx, uploadID, uploaderID)
	if err != nil {
		return nil, err
	}
	if meta.Status == uploadStatusCompleted {
		return a.GetFileForUser(ctx, meta.FileID, uploaderID)
	}
	if meta.Status != uploadStatusUploading || meta.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}

	file, verifyErr := a.buildVerifiedFile(ctx, meta)
	if meta.UploadMode == multipartUploadMode && errors.Is(verifyErr, ErrUploadNotCompleted) {
		parts, err := a.storage.ListMultipartParts(ctx, meta.ObjectKey, meta.StorageUploadID)
		if err != nil {
			return nil, err
		}
		completeParts, err := a.resolveCompleteMultiParts(meta, parts)
		if err != nil {
			return nil, err
		}
		if err := checkFileCompleteLock(lockLost); err != nil {
			return nil, err
		}
		if err := a.storage.CompleteMultipartUpload(ctx, meta.ObjectKey, meta.StorageUploadID, completeParts); err != nil {
			return nil, err
		}
		file, verifyErr = a.buildVerifiedFile(ctx, meta)
	}
	if verifyErr != nil {
		if meta.UploadMode == multipartUploadMode || errors.Is(verifyErr, ErrUnsupportedFileType) {
			if err := checkFileCompleteLock(lockLost); err != nil {
				return nil, err
			}
			a.cleanupObject(meta.ObjectKey)
		}
		return nil, verifyErr
	}
	if err := checkFileCompleteLock(lockLost); err != nil {
		return nil, err
	}
	file, err = a.persistCompletedFile(ctx, uploadID, file)
	if err != nil {
		if lockErr := checkFileCompleteLock(lockLost); lockErr != nil {
			return nil, lockErr
		}
		a.cleanupObject(meta.ObjectKey)
		return nil, err
	}
	if file.ObjectKey != meta.ObjectKey {
		a.cleanupObject(meta.ObjectKey)
	}
	ttl := time.Duration(a.config.Storage.MinIO.CacheTTLSeconds) * time.Second
	if meta.UploadMode == multipartUploadMode {
		ttl = time.Duration(a.config.Storage.MinIO.MultipartTTL) * time.Second
	}
	a.afterUploadCompleted(ctx, meta, file, ttl)
	return toFileDTO(file), nil
}

func checkFileCompleteLock(lockLost <-chan struct{}) error {
	select {
	case <-lockLost:
		return ErrFileCompleteLockLost
	default:
		return nil
	}
}

func (a *FileApplication) persistCompletedFile(ctx context.Context, uploadID string, entity *domainfile.File) (*domainfile.File, error) {
	if entity == nil {
		return nil, ErrInvalidUpload
	}

	var file *domainfile.File
	err := a.txManager.WithinTransaction(ctx, func(tx any) error {
		fileRepo := a.fileRepository.WithTx(tx)
		fileUploadRepo := a.fileUploadRepository.WithTx(tx)

		saveErr := fileRepo.CreateFile(ctx, entity)
		switch {
		case saveErr == nil:
			file = entity
		case errors.Is(saveErr, ErrFileHashConflict):
			existing, findErr := fileRepo.FindUploadedFileByUploaderAndHash(ctx, entity.UploaderID, entity.FileHash)
			if findErr != nil {
				return findErr
			}
			if existing == nil {
				return saveErr
			}
			file = existing
		default:
			return saveErr
		}

		marked, markErr := fileUploadRepo.MarkCompleted(ctx, uploadID, file.FileID, time.Now().UnixMilli())
		if markErr != nil {
			return markErr
		}
		if !marked {
			return ErrInvalidUpload
		}
		return nil
	})
	return file, err
}

func (a *FileApplication) cleanupObject(objectKey string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Duration(a.config.Storage.MinIO.ObjectCleanupTimeoutSeconds)*time.Second)
	defer cancel()
	if err := a.storage.DeleteObject(cleanupCtx, objectKey); err != nil {
		log.Printf("清理孤儿文件对象失败：对象=%s 错误=%v", objectKey, err)
	}
}

func (a *FileApplication) acquireFileCompleteLock(ctx context.Context, uploadID string) (func(), <-chan struct{}, error) {
	lockTTL := time.Duration(a.config.Storage.MinIO.MultipartCompleteLockTTLSeconds) * time.Second
	lockToken, locked, err := a.fileCache.AcquireFileCompleteLock(ctx, uploadID, lockTTL)
	if err != nil {
		return nil, nil, err
	}

	if !locked {
		return nil, nil, ErrUploadBusy
	}

	reneCtx, stopRene := context.WithCancel(context.Background())
	reneInterval := lockTTL / 3

	// 交给业务层判断锁是否还在
	lockLost := make(chan struct{})
	renewDone := make(chan struct{})

	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(reneInterval)
		defer ticker.Stop()

		for {
			select {
			case <-reneCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), time.Duration(a.config.Storage.MinIO.CompleteLockRenewTimeoutSeconds)*time.Second)
				renewed, err := a.fileCache.RenewFileCompleteLock(renewCtx, uploadID, lockToken, lockTTL)
				cancel()
				if err != nil || !renewed {
					if err == nil {
						err = ErrFileCompleteLockLost
					}
					log.Printf("续期文件完成锁失败：上传=%s 错误=%v", uploadID, err)
					close(lockLost)
					return
				}
			}
		}
	}()

	return func() {
		stopRene()
		// 当业务层结束合并时，需要等待协程成功释放资源
		<-renewDone
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Duration(a.config.Storage.MinIO.LockReleaseTimeoutSeconds)*time.Second)
		defer cancel()
		_ = a.fileCache.ReleaseFileCompleteLock(unlockCtx, uploadID, lockToken)
	}, lockLost, nil
}

func (a *FileApplication) afterUploadCompleted(ctx context.Context, meta *filecache.UploadMeta, file *domainfile.File, metaTTL time.Duration) {
	if err := a.fileCache.SetFileMetadata(ctx, file, time.Duration(a.config.Storage.MinIO.CacheTTLSeconds)*time.Second); err != nil {
		log.Printf("完成上传后写入文件缓存失败：文件=%s 错误=%v", file.FileID, err)
	}
	if err := a.fileCache.SetFileByUploaderAndHash(ctx, meta.UploaderID, meta.FileHash, file, time.Duration(a.config.Storage.MinIO.CacheTTLSeconds)*time.Second); err != nil {
		log.Printf("完成上传后写入文件哈希缓存失败：文件=%s 错误=%v", file.FileID, err)
	}
	meta.FileID = file.FileID
	meta.ContentType = file.ContentType
	meta.Status = uploadStatusCompleted
	if err := a.fileCache.SetUploadMeta(ctx, *meta, metaTTL); err != nil {
		log.Printf("完成上传后更新上传任务缓存失败：上传=%s 错误=%v", meta.UploadID, err)
	}
	if err := a.fileCache.DeleteActiveFileUploadIfMatches(ctx, meta.UploaderID, meta.FileHash, meta.UploadID); err != nil {
		log.Printf("完成上传后删除活跃上传缓存失败：上传=%s 错误=%v", meta.UploadID, err)
	}
}
