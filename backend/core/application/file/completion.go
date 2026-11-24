package file

import (
	"IM_backend/core/application/ports/objectstorage"
	domainfile "IM_backend/core/domain/file"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// 大文件成为瓶颈后可演进为 MinIO checksum 下沉（后端只读校验结果）。
func (a *FileApplication) verifyAndDetectCompletedObject(
	ctx context.Context, objectKey string, expectedSize int64, expectedHash string,
) (detectedType string, err error) {
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

	// TODO: 好好研究下这种网络传输，传递文件得时候到底是怎么传得
	hasher := sha256.New()
	header := make([]byte, 512)
	n, readErr := io.ReadFull(object, header)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return "", readErr
	}
	_, _ = hasher.Write(header[:n])
	written := int64(n)
	rest, err := io.Copy(hasher, object)
	written += rest
	if written != expectedSize {
		return "", ErrFileSizeMismatch
	}
	if !strings.EqualFold(hex.EncodeToString(hasher.Sum(nil)), expectedHash) {
		return "", ErrFileHashMismatch
	}

	return strings.ToLower(http.DetectContentType(header[:n])), nil
}

func (a *FileApplication) buildVerifiedFile(ctx context.Context, u *domainfile.Upload) (*domainfile.File, error) {
	detected, err := a.verifyAndDetectCompletedObject(ctx, u.ObjectKey, u.ExpectedSize, u.FileHash)
	if err != nil {
		return nil, err
	}

	if err := a.validateContentType(detected); err != nil {
		return nil, err
	}

	// TODO: 这两个是可以直接融合在一起得吧？
	file := domainfile.NewFile(u.FileID, u.UploaderID, a.storage.Bucket(), u.ObjectKey, u.FileName, detected, u.ExpectedSize)
	file.FileHash = u.FileHash
	return file, nil
}

// 客户端提交 parts[ETag]，后端合并 + 校验 + 落库，并发靠状态机 CAS，幂等靠 files 表全局 hash 唯一键
func (a *FileApplication) CompleteMultipartUpload(
	ctx context.Context, uploadID, uploaderID string, parts []MultipartPartDTO,
) (*FileDTO, error) {
	record, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if record == nil || record.UploadMode != multipartUploadMode {
		return nil, ErrInvalidUpload
	}
	if record.UploaderID != uploaderID {
		return nil, ErrUploadUnauthorized
	}

	if record.Status == uploadStatusCompleted {
		return a.completeResultByFileID(ctx, record.FileID)
	}

	// 上传任务已经过期
	if record.Status != uploadStatusUploading || record.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}

	file, verifyErr := a.buildVerifiedFile(ctx, record)
	if errors.Is(verifyErr, ErrUploadNotCompleted) {
		// 分片文件尚未合并
		storageParts, listErr := a.storage.ListMultipartParts(ctx, record.ObjectKey, record.StorageUploadID)
		if listErr != nil {
			return nil, listErr
		}
		completeParts, e := a.resolveCompleteMultiParts(record, storageParts, parts)
		if e != nil {
			return nil, e
		}

		if mergeErr := a.storage.CompleteMultipartUpload(ctx, record.ObjectKey, record.StorageUploadID, completeParts); mergeErr != nil {
			// 并发下另一请求可能已合并（第二次合并报 NoSuchUpload）：重读会话走幂等
			if cur, e2 := a.fileUploadRepository.FindFileUploadByID(ctx, uploadID); e2 == nil &&
				cur != nil && cur.Status == uploadStatusCompleted {
				return a.completeResultByFileID(ctx, cur.FileID)
			}
			return nil, mergeErr
		}

		file, verifyErr = a.buildVerifiedFile(ctx, record)
	}

	if verifyErr != nil {
		// 校验失败：清理对象，兜底交给 lifecycle（未合并分片）+ DeleteOutdatedData（已合并对象）
		a.cleanupObject(ctx, record.ObjectKey)
		return nil, verifyErr
	}

	file, err = a.persistCompletedFile(ctx, uploadID, file)
	if err != nil {
		a.cleanupObject(ctx, record.ObjectKey)
		return nil, err
	}

	if file.ObjectKey != record.ObjectKey {
		// 全局 hash 命中已有 file：本次对象冗余，清掉
		a.cleanupObject(ctx, record.ObjectKey)
	}
	return toFileDTO(file), nil
}

// 校验分片完整性
//   - storageParts 是 MinIO 的实际分片（权威）；
//   - clientParts 是客户端提交的 (partNumber, ETag)，用于交叉校验防伪造；
//
// 要求 partNumber 连续覆盖 1..TotalChunks、ETag 一致、大小符合规格
func (a *FileApplication) resolveCompleteMultiParts(
	u *domainfile.Upload, storageParts []objectstorage.MultipartPart, clientParts []MultipartPartDTO,
) ([]objectstorage.MultipartPart, error) {
	clientETag := make(map[int]string, len(clientParts))
	for _, p := range clientParts {
		if p.PartNumber <= 0 || p.PartNumber > u.TotalChunks || p.ETag == "" {
			return nil, ErrInvalidPart
		}
		clientETag[p.PartNumber] = strings.Trim(p.ETag, "\"")
	}

	byNumber := make(map[int]objectstorage.MultipartPart, len(storageParts))
	invalid := make([]int, 0)

	// 过滤非法的 part
	for _, part := range storageParts {
		// 去除 ETag 两端可能得双引号
		storageETag := strings.Trim(part.ETag, "\"")

		if want, ok := clientETag[part.PartNumber]; ok && !strings.EqualFold(want, storageETag) {
			invalid = append(invalid, part.PartNumber)
			continue
		}

		if part.PartNumber <= 0 || part.PartNumber > u.TotalChunks || part.ETag == "" ||
			part.Size != a.expectedMultipartPartSize(u, part.PartNumber) {
			invalid = append(invalid, part.PartNumber)
			continue
		}
		byNumber[part.PartNumber] = part
	}

	// 收集缺失片段
	missing := make([]int, 0)
	for i := 1; i <= u.TotalChunks; i++ {
		if _, ok := byNumber[i]; !ok {
			missing = append(missing, i)
		}
	}

	if len(missing) > 0 || len(invalid) > 0 {
		return nil, &UploadIncompleteError{MissingParts: missing, InvalidParts: invalid}
	}

	complete := make([]objectstorage.MultipartPart, 0, u.TotalChunks)
	for i := 1; i <= u.TotalChunks; i++ {
		complete = append(complete, byNumber[i])
	}

	return complete, nil
}

func (a *FileApplication) expectedMultipartPartSize(u *domainfile.Upload, partNumber int) int64 {
	if partNumber == u.TotalChunks {
		// 单独处理最后一个分片的大小
		return u.ExpectedSize - int64(u.TotalChunks-1)*u.ChunkSize
	}

	return u.ChunkSize
}

func (a *FileApplication) persistCompletedFile(ctx context.Context, uploadID string, entity *domainfile.File) (*domainfile.File, error) {
	if entity == nil {
		return nil, ErrInvalidUpload
	}

	var file *domainfile.File
	err := a.txManager.WithinTransaction(ctx, func(tx any) error {
		fileRepo := a.fileRepository.WithTx(tx)
		uploadRepo := a.fileUploadRepository.WithTx(tx)

		saveErr := fileRepo.CreateFile(ctx, entity)
		switch {
		case saveErr == nil:
			file = entity
		case errors.Is(saveErr, ErrFileHashConflict):
			// 全局 hash 已存在：复用已有 file，本会话对象稍后清理
			existing, findErr := fileRepo.FindFileByHash(ctx, entity.FileHash)
			if findErr != nil {
				return findErr
			}

			// TODO: 这里有一个空窗，就是文件的状态刚好就是 deleting
			if existing == nil {
				return saveErr
			}

			if existing.Status == domainfile.FileStatusDeleting {
				break
			}

			// TODO: 这里应该是可以直接返回了
			file = existing
			return nil
		default:
			return saveErr
		}

		marked, err := uploadRepo.MarkCompleted(ctx, uploadID, file.FileID, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		if !marked {
			return ErrInvalidUpload
		}
		return nil
	})
	return file, err
}

func (a *FileApplication) completeResultByFileID(ctx context.Context, fileID string) (*FileDTO, error) {
	file, err := a.fileRepository.FindFileByID(ctx, fileID)
	if err != nil {
		return nil, err
	}
	if file == nil || file.Status != domainfile.FileStatusUploaded {
		return nil, ErrFileNotFound
	}
	return toFileDTO(file), nil
}

func (a *FileApplication) cleanupObject(ctx context.Context, objectKey string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Duration(a.config.Storage.MinIO.ObjectCleanupTimeoutSeconds)*time.Second)
	defer cancel()
	if err := a.storage.DeleteObject(cleanupCtx, objectKey); err != nil {
		log.Printf("清理文件对象失败：对象=%s 错误=%v（由清理任务兜底）", objectKey, err)
	}
}
