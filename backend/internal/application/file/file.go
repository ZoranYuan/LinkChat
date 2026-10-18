package file

import (
	idport "IM_backend/internal/application/ports/id"
	filecache "IM_backend/internal/application/ports/persistence/cache/file"
	filerepo "IM_backend/internal/application/ports/persistence/repository/file"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	"errors"
	"net/http"

	objectstorage "IM_backend/internal/application/ports/storage/object"
	fileentity "IM_backend/internal/domain/file/entity"
	messageentity "IM_backend/internal/domain/message/entity"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

type FileApplication struct {
	options                      Options
	fileRepository               filerepo.FileRepository
	fileUploadRepository         filerepo.FileUploadRepository
	messageAttachmentsRepository messagerepo.MessageAttachmentRepository
	fileCache                    filecache.FileCache
	storage                      objectstorage.ObjectStorage
	idGenerator                  idport.Generator
	sf                           singleflight.Group
	txManager                    txmanager.TxManager
}

type Options struct {
	MultipartTTL             time.Duration
	PartURLTTL               time.Duration
	DirectUploadURLTTL       time.Duration
	DirectUploadMaxSize      int64
	MultipartInitLockTTL     time.Duration
	MultipartCompleteLockTTL time.Duration
	CacheTTL                 time.Duration
	AttachmentFileCardTTL    time.Duration
	URLTTL                   time.Duration
	AttachmentAccessCacheTTL time.Duration
	MaxFileSize              int64
	MaxMultipartParts        int
}

const (
	uploadStatusUploading = "uploading"
	uploadStatusCompleted = "completed"
	multipartUploadMode   = "multipart"
	directUploadMode      = "direct"
)

func NewFileApplication(
	options Options,
	fileRepository filerepo.FileRepository,
	fileUploadRepository filerepo.FileUploadRepository,
	messageAttachmentsRepository messagerepo.MessageAttachmentRepository,
	fileCache filecache.FileCache,
	storage objectstorage.ObjectStorage,
	idGenerator idport.Generator,
	txManager txmanager.TxManager,
) *FileApplication {
	return &FileApplication{
		options:                      options,
		fileRepository:               fileRepository,
		fileUploadRepository:         fileUploadRepository,
		messageAttachmentsRepository: messageAttachmentsRepository,
		fileCache:                    fileCache,
		storage:                      storage,
		idGenerator:                  idGenerator,
		txManager:                    txManager,
	}
}

func normalizeFileHash(fileHash string) string {
	return strings.ToLower(strings.TrimSpace(fileHash))
}

var allowedContentTypes = map[string]struct{}{
	"image/jpeg":      {},
	"image/png":       {},
	"image/webp":      {},
	"video/mp4":       {},
	"application/pdf": {},
	"text/plain":      {},
}

func validateContentType(contentType string) error {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if mediaType, _, err := mime.ParseMediaType(contentType); err == nil {
		contentType = mediaType
	}
	if _, ok := allowedContentTypes[contentType]; !ok {
		return ErrUnsupportedFileType
	}
	return nil
}

func (a *FileApplication) verifyAndDetectCompletedObject(
	ctx context.Context,
	objectKey string,
	expectedSize int64,
	expectedHash string,
) (result string, err error) {
	info, err := a.storage.StatObject(ctx, objectKey)
	if err != nil {
		return "", ErrUploadNotCompleted
	}
	if info == nil {
		return "", ErrUploadNotCompleted
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

	if _, err = hasher.Write(header[:n]); err != nil {
		return "", err
	}
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

func (a *FileApplication) buildVerifiedFile(
	ctx context.Context,
	fileID string,
	uploaderID string,
	bucket string,
	objectKey string,
	fileName string,
	expectedSize int64,
	expectedHash string,
) (*fileentity.File, error) {
	detectedType, err := a.verifyAndDetectCompletedObject(ctx, objectKey, expectedSize, expectedHash)
	if err != nil {
		return nil, err
	}
	if err := validateContentType(detectedType); err != nil {
		return nil, err
	}

	file := fileentity.NewFile(
		fileID,
		uploaderID,
		bucket,
		objectKey,
		fileName,
		detectedType,
		expectedSize,
	)
	file.FileHash = expectedHash
	return file, nil
}

func (a *FileApplication) InitMultipartUpload(ctx context.Context, dto MultipartInitDTO) (*MultipartInitResDTO, error) {
	fileHash := normalizeFileHash(dto.FileHash)

	if err := validateContentType(strings.ToLower(strings.TrimSpace(dto.ContentType))); err != nil {
		return nil, ErrInvalidMIME
	}

	if dto.UploaderID == "" || dto.FileName == "" || dto.Size <= 0 ||
		fileHash == "" || dto.ChunkSize <= 0 || dto.TotalChunks <= 0 {
		return nil, ErrInvalidPart
	}
	if a.options.MaxFileSize > 0 && dto.Size > a.options.MaxFileSize {
		return nil, ErrFileTooLarge
	}
	expectedChunks := dto.Size / dto.ChunkSize
	if dto.Size%dto.ChunkSize != 0 {
		expectedChunks++
	}
	if int64(dto.TotalChunks) != expectedChunks {
		return nil, ErrInvalidPart
	}
	if a.options.MaxMultipartParts > 0 && dto.TotalChunks > a.options.MaxMultipartParts {
		return nil, ErrTooManyParts
	}

	if result, found, err := a.findMultipartInitResult(ctx, dto.UploaderID, fileHash); err != nil || found {
		return result, err
	}

	// 新上传文件
	lockToken, locked, err := a.fileCache.AcquireFileInitLock(ctx, dto.UploaderID, fileHash, a.uploadInitLockTTL())
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, ErrUploadBusy
	}
	defer func() {
		_ = a.fileCache.ReleaseFileInitLock(context.Background(), dto.UploaderID, fileHash, lockToken)
	}()

	// 二次检查，避免出现窗口竞态问题
	if result, found, err := a.findMultipartInitResult(ctx, dto.UploaderID, fileHash); err != nil || found {
		return result, err
	}

	// uploadId 依旧为空，确定是新文件上传
	fileId, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	uploadId, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	objectKey := a.buildObjectKey(dto.UploaderID, fileId)
	storageUploadId, err := a.storage.CreateMultipartUpload(
		ctx,
		objectKey,
		dto.ContentType,
	)
	if err != nil {
		return nil, err
	}

	ttl := a.multipartTTL()
	now := time.Now()
	createdAt := now.UnixMilli()
	expiresAt := now.Add(ttl).UnixMilli()
	meta := filecache.UploadMeta{
		UploadMode:      multipartUploadMode,
		UploadId:        uploadId,
		StorageUploadId: storageUploadId,
		FileId:          fileId,
		UploaderId:      dto.UploaderID,
		Bucket:          a.storage.Bucket(),
		ObjectKey:       objectKey,
		FileName:        dto.FileName,
		ContentType:     dto.ContentType,
		Size:            dto.Size,
		FileHash:        fileHash,
		ChunkSize:       dto.ChunkSize,
		TotalChunks:     dto.TotalChunks,
		CreatedAt:       createdAt,
		ExpiresAt:       expiresAt,
		Status:          uploadStatusUploading,
	}
	if err := a.fileUploadRepository.CreateFileUpload(ctx, filerepo.FileUploadRecord{
		UploadId:        meta.UploadId,
		FileId:          meta.FileId,
		UploaderId:      meta.UploaderId,
		UploadMode:      multipartUploadMode,
		StorageUploadId: meta.StorageUploadId,
		FileHash:        meta.FileHash,
		ObjectKey:       meta.ObjectKey,
		FileName:        meta.FileName,
		ContentType:     meta.ContentType,
		ExpectedSize:    meta.Size,
		ChunkSize:       meta.ChunkSize,
		TotalChunks:     meta.TotalChunks,
		Status:          uploadStatusUploading,
		ExpiresAt:       expiresAt,
		CreatedAt:       createdAt,
		UpdatedAt:       createdAt,
	}); err != nil {
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadId)
		return nil, err
	}

	if err := a.fileCache.SetUploadMeta(ctx, meta, ttl); err != nil {
		// 取消 minio 上传
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadId)
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(ctx, uploadId)
		return nil, err
	}

	if err := a.fileCache.SetActiveFileUpload(ctx, dto.UploaderID, fileHash, uploadId, ttl); err != nil {
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadId)
		_ = a.fileCache.DeleteUploadMeta(ctx, uploadId)
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(ctx, uploadId)
		return nil, err
	}

	return &MultipartInitResDTO{
		UploadID:      uploadId,
		Status:        uploadStatusUploading,
		ChunkSize:     dto.ChunkSize,
		TotalChunks:   dto.TotalChunks,
		UploadedParts: []int{},
	}, nil
}

// InitUpload 是统一上传初始化入口。媒体类型不参与文件上传决策，后端只根据大小选择上传模式。
func (a *FileApplication) InitUpload(ctx context.Context, dto UploadInitDTO) (*UploadInitResDTO, error) {
	if dto.Size <= 0 {
		return nil, ErrInvalidUpload
	}

	if a.options.DirectUploadMaxSize > 0 && dto.Size <= a.options.DirectUploadMaxSize {
		result, err := a.InitDirectUpload(ctx, DirectUploadInitDTO{
			UploaderID:  dto.UploaderID,
			FileName:    dto.FileName,
			ContentType: dto.ContentType,
			Size:        dto.Size,
			FileHash:    dto.FileHash,
		})
		if err != nil {
			return nil, err
		}
		return &UploadInitResDTO{
			UploadID:   result.UploadID,
			FileID:     result.FileID,
			UploadMode: directUploadMode,
			Status:     result.Status,
			URL:        result.URL,
			ExpiresAt:  result.ExpiresAt,
		}, nil
	}

	result, err := a.InitMultipartUpload(ctx, MultipartInitDTO{
		UploaderID:  dto.UploaderID,
		FileName:    dto.FileName,
		ContentType: dto.ContentType,
		Size:        dto.Size,
		FileHash:    dto.FileHash,
		ChunkSize:   dto.ChunkSize,
		TotalChunks: dto.TotalChunks,
	})
	if err != nil {
		return nil, err
	}
	return &UploadInitResDTO{
		UploadID:      result.UploadID,
		FileID:        result.FileID,
		UploadMode:    multipartUploadMode,
		Status:        result.Status,
		ChunkSize:     result.ChunkSize,
		TotalChunks:   result.TotalChunks,
		UploadedParts: result.UploadedParts,
	}, nil
}

func (a *FileApplication) InitDirectUpload(ctx context.Context, dto DirectUploadInitDTO) (*DirectUploadInitResDTO, error) {
	fileHash := normalizeFileHash(dto.FileHash)

	if err := validateContentType(strings.ToLower(strings.TrimSpace(dto.ContentType))); err != nil {
		return nil, ErrInvalidMIME
	}

	if dto.UploaderID == "" || dto.FileName == "" || dto.Size <= 0 || fileHash == "" {
		return nil, ErrInvalidUpload
	}
	if a.options.DirectUploadMaxSize > 0 && dto.Size > a.options.DirectUploadMaxSize {
		return nil, ErrFileTooLarge
	}
	if a.options.MaxFileSize > 0 && dto.Size > a.options.MaxFileSize {
		return nil, ErrFileTooLarge
	}

	lookupKey := fmt.Sprintf("uploaderID:%s:fileHash:%s", dto.UploaderID, fileHash)
	value, err, _ := a.sf.Do(lookupKey, func() (any, error) {
		return a.lookupDirectUploadCache(ctx, dto.UploaderID, fileHash)
	})
	if err != nil {
		return nil, err
	}

	lookup, ok := value.(directUploadCacheLookup)
	if !ok {
		return nil, errors.New("直传缓存查询结果类型错误")
	}
	if lookup.file != nil {
		// 找到了之前上传过的文件，实现秒传
		return &DirectUploadInitResDTO{FileID: lookup.file.FileId, Status: uploadStatusCompleted}, nil
	}
	if lookup.activeUploadID != "" {
		// 直传任务可以复用原 uploadId，只需要重新签发一个 PUT URL。
		existing, findErr := a.fileUploadRepository.FindFileUploadByID(ctx, lookup.activeUploadID)
		if findErr != nil {
			return nil, findErr
		}
		if existing != nil && existing.UploadMode == directUploadMode &&
			existing.UploaderId == dto.UploaderID &&
			existing.Status == uploadStatusUploading &&
			existing.ExpiresAt > time.Now().UnixMilli() {
			return a.presignDirectUpload(ctx, existing)
		}
		if existing != nil && existing.UploadMode != directUploadMode {
			return nil, ErrUploadBusy
		}
	}

	// 获取文件上传初始化锁，避免文件重复初始化
	lockToken, locked, err := a.fileCache.AcquireFileInitLock(ctx, dto.UploaderID, fileHash, a.uploadInitLockTTL())
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, ErrUploadBusy
	}
	defer func() {
		_ = a.fileCache.ReleaseFileInitLock(context.Background(), dto.UploaderID, fileHash, lockToken)
	}()

	// 双重检查，防止时间空白出现文件已经被上传的可能
	if file, err := a.fileCache.GetFileByUploaderAndHash(ctx, dto.UploaderID, fileHash); err != nil {
		return nil, err
	} else if file != nil {
		return &DirectUploadInitResDTO{FileID: file.FileId, Status: uploadStatusCompleted}, nil
	}

	now := time.Now()
	existing, err := a.fileUploadRepository.FindActiveFileUploadByUploaderAndHash(ctx, dto.UploaderID, fileHash, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.UploadMode == directUploadMode {
			return a.presignDirectUpload(ctx, existing)
		}
		return nil, ErrUploadBusy
	}

	fileId, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	uploadId, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	ttl := a.directUploadURLTTL()
	record := filerepo.FileUploadRecord{
		UploadId:     uploadId,
		FileId:       fileId,
		UploaderId:   dto.UploaderID,
		UploadMode:   directUploadMode,
		FileHash:     fileHash,
		ObjectKey:    a.buildObjectKey(dto.UploaderID, fileId),
		FileName:     dto.FileName,
		ContentType:  dto.ContentType,
		ExpectedSize: dto.Size,
		Status:       uploadStatusUploading,
		ExpiresAt:    now.Add(ttl).UnixMilli(),
		CreatedAt:    now.UnixMilli(),
		UpdatedAt:    now.UnixMilli(),
	}
	if err := a.fileUploadRepository.CreateFileUpload(ctx, record); err != nil {
		return nil, err
	}

	result, err := a.presignDirectUpload(ctx, &record)
	if err != nil {
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(context.Background(), uploadId)
		return nil, err
	}
	meta := uploadMetaFromRecord(&record, a.storage.Bucket())
	if err := a.fileCache.SetUploadMeta(ctx, *meta, ttl); err != nil {
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(context.Background(), uploadId)
		return nil, err
	}
	if err := a.fileCache.SetActiveFileUpload(ctx, dto.UploaderID, fileHash, uploadId, ttl); err != nil {
		log.Printf("写入直传活跃上传缓存失败：上传=%s 错误=%v", uploadId, err)
	}
	return result, nil
}

type directUploadCacheLookup struct {
	file           *fileentity.File
	activeUploadID string
}

func (a *FileApplication) lookupDirectUploadCache(ctx context.Context, uploaderID, fileHash string) (directUploadCacheLookup, error) {
	file, err := a.fileCache.GetFileByUploaderAndHash(ctx, uploaderID, fileHash)
	if err != nil {
		return directUploadCacheLookup{}, err
	}
	if file != nil && file.Status == fileentity.FileStatusUploaded {
		return directUploadCacheLookup{file: file}, nil
	}

	// 是否有活跃任务
	activeUploadID, err := a.fileCache.GetActiveFileUploadID(ctx, uploaderID, fileHash)
	if err != nil {
		return directUploadCacheLookup{}, err
	}
	return directUploadCacheLookup{activeUploadID: activeUploadID}, nil
}

func (a *FileApplication) presignDirectUpload(ctx context.Context, record *filerepo.FileUploadRecord) (*DirectUploadInitResDTO, error) {
	ttl := time.Until(time.UnixMilli(record.ExpiresAt))
	if ttl <= 0 {
		return nil, ErrInvalidUpload
	}
	url, err := a.storage.PresignedPutURL(ctx, record.ObjectKey, ttl)
	if err != nil {
		return nil, err
	}
	return &DirectUploadInitResDTO{
		UploadID:  record.UploadId,
		Status:    uploadStatusUploading,
		URL:       url,
		ExpiresAt: time.UnixMilli(record.ExpiresAt).Unix(),
	}, nil
}

func (a *FileApplication) CompleteDirectUpload(ctx context.Context, uploadId, uploaderId string) (*FileDTO, error) {
	if uploadId == "" || uploaderId == "" {
		return nil, ErrInvalidUpload
	}

	meta, err := a.loadDirectUploadMeta(ctx, uploadId, uploaderId)
	if err != nil {
		return nil, err
	}
	if meta.Status == uploadStatusCompleted {
		return a.GetFileForUser(ctx, meta.FileId, uploaderId)
	}
	if err := validateDirectUploadPending(meta, time.Now().UnixMilli()); err != nil {
		return nil, err
	}

	// 获得可续期锁
	unlock, lockLost, err := a.acquireFileCompleteLock(ctx, uploadId)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// 加锁后重新读取，避免等待锁期间上传状态发生变化。
	meta, err = a.loadDirectUploadMeta(ctx, uploadId, uploaderId)
	if err != nil {
		return nil, err
	}
	if meta.Status == uploadStatusCompleted {
		return a.GetFileForUser(ctx, meta.FileId, uploaderId)
	}
	if err := validateDirectUploadPending(meta, time.Now().UnixMilli()); err != nil {
		return nil, err
	}

	file, err := a.buildVerifiedFile(
		ctx,
		meta.FileId,
		meta.UploaderId,
		meta.Bucket,
		meta.ObjectKey,
		meta.FileName,
		meta.Size,
		meta.FileHash,
	)
	if err != nil {
		if errors.Is(err, ErrUnsupportedFileType) {
			if lockErr := checkFileCompleteLock(lockLost); lockErr != nil {
				return nil, lockErr
			}

			// 校验失败，清除 minio 中的文件
			a.cleanupObject(meta.ObjectKey)
		}
		return nil, err
	}

	if err := checkFileCompleteLock(lockLost); err != nil {
		return nil, err
	}

	file, err = a.persistCompletedFile(ctx, meta.UploadId, file)
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
	a.afterUploadCompleted(ctx, meta, file, a.cacheTTL())
	return toFileDTO(file), nil
}

// CompleteUpload 是统一上传完成入口，具体的对象完成动作由上传会话模式决定。
func (a *FileApplication) CompleteUpload(ctx context.Context, uploadId, uploaderId string) (*FileDTO, error) {
	if uploadId == "" || uploaderId == "" {
		return nil, ErrInvalidUpload
	}

	meta, cacheErr := a.fileCache.GetUploadMeta(ctx, uploadId)
	if cacheErr != nil {
		log.Printf("读取统一上传元数据缓存失败，回源数据库：上传=%s 错误=%v", uploadId, cacheErr)
	}
	if meta == nil || meta.UploadId != uploadId || meta.UploaderId != uploaderId {
		record, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadId)
		if err != nil {
			return nil, err
		}
		if record == nil || record.UploaderId != uploaderId {
			return nil, ErrInvalidUpload
		}
		meta = uploadMetaFromRecord(record, a.storage.Bucket())
	}

	switch meta.UploadMode {
	case directUploadMode:
		return a.CompleteDirectUpload(ctx, uploadId, uploaderId)
	case multipartUploadMode:
		return a.CompleteMultipartUpload(ctx, uploadId, uploaderId)
	default:
		return nil, ErrInvalidUpload
	}
}

func (a *FileApplication) loadDirectUploadMeta(ctx context.Context, uploadId, uploaderId string) (*filecache.UploadMeta, error) {
	meta, cacheErr := a.fileCache.GetUploadMeta(ctx, uploadId)
	now := time.Now().UnixMilli()
	if cacheErr == nil && meta != nil &&
		meta.UploadId == uploadId &&
		meta.UploadMode == directUploadMode &&
		meta.UploaderId == uploaderId &&
		(meta.Status == uploadStatusCompleted ||
			(meta.Status == uploadStatusUploading && meta.ExpiresAt > now)) {
		return meta, nil
	}

	if cacheErr != nil {
		log.Printf("读取直传上传元数据缓存失败，回源数据库：上传=%s 错误=%v", uploadId, cacheErr)
	}

	record, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadId)
	if err != nil {
		return nil, err
	}
	if record == nil || record.UploadMode != directUploadMode {
		return nil, ErrInvalidUpload
	}
	if record.UploaderId != uploaderId {
		return nil, ErrUploadUnauthorized
	}

	meta = uploadMetaFromRecord(record, a.storage.Bucket())
	ttl := a.cacheTTL()
	if record.Status != uploadStatusCompleted {
		ttl = time.Until(time.UnixMilli(record.ExpiresAt))
	}
	if ttl > 0 {
		if cacheErr := a.fileCache.SetUploadMeta(ctx, *meta, ttl); cacheErr != nil {
			log.Printf("回填直传上传元数据缓存失败：上传=%s 错误=%v", uploadId, cacheErr)
		}
	}
	return meta, nil
}

func uploadMetaFromRecord(record *filerepo.FileUploadRecord, bucket string) *filecache.UploadMeta {
	if record == nil {
		return nil
	}
	return &filecache.UploadMeta{
		UploadId:        record.UploadId,
		UploadMode:      record.UploadMode,
		StorageUploadId: record.StorageUploadId,
		FileId:          record.FileId,
		UploaderId:      record.UploaderId,
		Bucket:          bucket,
		ObjectKey:       record.ObjectKey,
		FileName:        record.FileName,
		ContentType:     record.ContentType,
		Size:            record.ExpectedSize,
		FileHash:        record.FileHash,
		ChunkSize:       record.ChunkSize,
		TotalChunks:     record.TotalChunks,
		CreatedAt:       record.CreatedAt,
		ExpiresAt:       record.ExpiresAt,
		Status:          record.Status,
	}
}

func validateDirectUploadPending(meta *filecache.UploadMeta, now int64) error {
	if meta == nil || meta.Status != uploadStatusUploading || meta.ExpiresAt <= now {
		return ErrInvalidUpload
	}
	return nil
}

func checkFileCompleteLock(lockLost <-chan struct{}) error {
	select {
	case <-lockLost:
		return ErrFileCompleteLockLost
	default:
		return nil
	}
}

func (a *FileApplication) persistCompletedFile(ctx context.Context, uploadId string, entity *fileentity.File) (*fileentity.File, error) {
	if entity == nil {
		return nil, ErrInvalidUpload
	}

	var file *fileentity.File
	err := a.txManager.WithinTransaction(ctx, func(tx any) error {
		fileRepo := a.fileRepository.WithTx(tx)
		fileUploadRepo := a.fileUploadRepository.WithTx(tx)

		saveErr := fileRepo.CreateFile(ctx, entity)
		switch {
		case saveErr == nil:
			file = entity
		case errors.Is(saveErr, ErrFileHashConflict):
			existing, findErr := fileRepo.FindUploadedFileByUploaderAndHash(ctx, entity.UploaderId, entity.FileHash)
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

		marked, markErr := fileUploadRepo.MarkCompleted(ctx, uploadId, file.FileId, time.Now().UnixMilli())
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

func (a *FileApplication) finishMultipartCompletion(
	ctx context.Context,
	meta *filecache.UploadMeta,
	entity *fileentity.File,
	lockLost <-chan struct{},
) (*FileDTO, error) {
	if err := checkFileCompleteLock(lockLost); err != nil {
		return nil, err
	}

	file, err := a.persistCompletedFile(ctx, meta.UploadId, entity)
	if err != nil {
		log.Printf("文件记录创建和上传状态更新失败：上传=%s 错误=%v", meta.UploadId, err)
		if lockErr := checkFileCompleteLock(lockLost); lockErr != nil {
			return nil, lockErr
		}
		a.cleanupObject(entity.ObjectKey)
		return nil, err
	}
	if file == nil {
		return nil, ErrInvalidUpload
	}

	if file.ObjectKey != entity.ObjectKey {
		a.cleanupObject(entity.ObjectKey)
	}
	a.afterUploadCompleted(ctx, meta, file, a.multipartTTL())
	return toFileDTO(file), nil
}

func (a *FileApplication) CompleteMultipartUpload(ctx context.Context, uploadId string, uploaderId string) (*FileDTO, error) {
	meta, err := a.loadMultipartMeta(ctx, uploadId, uploaderId)
	if err != nil {
		return nil, err
	}

	if meta.Status == uploadStatusCompleted {
		file, err := a.GetFileForUser(ctx, meta.FileId, uploaderId)
		if err != nil {
			return nil, err
		}

		return file, nil
	}

	if meta.Status != uploadStatusUploading || meta.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}

	// 加锁开始上传
	unlock, lockLost, err := a.acquireFileCompleteLock(ctx, uploadId)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// 二次检查
	meta, err = a.loadMultipartMeta(ctx, uploadId, uploaderId)
	if err != nil {
		return nil, err
	}
	if meta.Status == uploadStatusCompleted {
		return a.GetFileForUser(ctx, meta.FileId, uploaderId)
	}

	entity, verifyErr := a.buildVerifiedFile(
		ctx,
		meta.FileId,
		meta.UploaderId,
		meta.Bucket,
		meta.ObjectKey,
		meta.FileName,
		meta.Size,
		meta.FileHash,
	)
	if verifyErr == nil {
		return a.finishMultipartCompletion(ctx, meta, entity, lockLost)
	}
	if !errors.Is(verifyErr, ErrUploadNotCompleted) {
		if lockErr := checkFileCompleteLock(lockLost); lockErr != nil {
			return nil, lockErr
		}
		a.cleanupObject(meta.ObjectKey)
		return nil, verifyErr
	}

	parts, err := a.storage.ListMultipartParts(ctx, meta.ObjectKey, meta.StorageUploadId)
	if err != nil {
		return nil, err
	}

	completePart, err := a.resolveCompleteMultiParts(meta, parts)
	if err != nil {
		return nil, err
	}

	if err := checkFileCompleteLock(lockLost); err != nil {
		return nil, err
	}

	if err := a.completeMultipartObject(ctx, meta, completePart); err != nil {
		return nil, err
	}

	entity, err = a.buildVerifiedFile(
		ctx,
		meta.FileId,
		meta.UploaderId,
		meta.Bucket,
		meta.ObjectKey,
		meta.FileName,
		meta.Size,
		meta.FileHash,
	)

	if err != nil {
		if lockErr := checkFileCompleteLock(lockLost); lockErr != nil {
			return nil, lockErr
		}
		a.cleanupObject(meta.ObjectKey)
		return nil, err
	}

	return a.finishMultipartCompletion(ctx, meta, entity, lockLost)
}

func (a *FileApplication) cleanupObject(objectKey string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.storage.DeleteObject(cleanupCtx, objectKey); err != nil {
		log.Printf("清理孤儿文件对象失败：对象=%s 错误=%v", objectKey, err)
	}
}

func (a *FileApplication) findFileByUploaderAndHash(ctx context.Context, uploaderId, fileHash string) (*fileentity.File, error) {
	if uploaderId == "" || fileHash == "" {
		return nil, nil
	}
	file, err := a.fileCache.GetFileByUploaderAndHash(ctx, uploaderId, fileHash)
	if err == nil && file != nil && file.UploaderId == uploaderId && file.Status == fileentity.FileStatusUploaded {
		return file, nil
	}
	if err != nil {
		log.Printf("读取直传文件缓存失败，回源数据库：用户=%s 哈希=%s 错误=%v", uploaderId, fileHash, err)
	}

	file, err = a.fileRepository.FindUploadedFileByUploaderAndHash(ctx, uploaderId, fileHash)
	if err != nil || file == nil {
		return nil, err
	}

	if cacheErr := a.fileCache.SetFileMetadata(ctx, file, a.cacheTTL()); cacheErr != nil {
		log.Printf("回填直传文件缓存失败：文件=%s 错误=%v", file.FileId, cacheErr)
	}
	if cacheErr := a.fileCache.SetFileByUploaderAndHash(ctx, uploaderId, fileHash, file, a.cacheTTL()); cacheErr != nil {
		log.Printf("回填直传文件哈希缓存失败：文件=%s 错误=%v", file.FileId, cacheErr)
	}
	return file, nil
}

func (a *FileApplication) findMultipartInitResult(ctx context.Context, uploaderId, fileHash string) (*MultipartInitResDTO, bool, error) {
	file, err := a.findFileByUploaderAndHash(ctx, uploaderId, fileHash)
	if err != nil {
		return nil, false, err
	}
	if file != nil {
		return &MultipartInitResDTO{
			FileID: file.FileId,
			Status: uploadStatusCompleted,
		}, true, nil
	}

	return a.findActiveMultipartUpload(ctx, uploaderId, fileHash)
}

func (a *FileApplication) findActiveMultipartUpload(ctx context.Context, uploaderId, fileHash string) (*MultipartInitResDTO, bool, error) {
	now := time.Now().UnixMilli()
	if uploadId, err := a.fileCache.GetActiveFileUploadID(ctx, uploaderId, fileHash); err == nil && uploadId != "" {
		meta, metaErr := a.fileCache.GetUploadMeta(ctx, uploadId)
		if metaErr == nil && meta != nil &&
			meta.UploadId == uploadId &&
			meta.UploaderId == uploaderId &&
			meta.FileHash == fileHash &&
			meta.Status == uploadStatusUploading &&
			meta.ExpiresAt > now &&
			meta.StorageUploadId != "" {
			uploadedParts, partsErr := a.uploadedPartNumbers(ctx, meta.ObjectKey, meta.StorageUploadId)
			if partsErr != nil {
				return nil, false, partsErr
			}
			return &MultipartInitResDTO{
				UploadID: uploadId, FileID: meta.FileId,
				Status:    uploadStatusUploading,
				ChunkSize: meta.ChunkSize, TotalChunks: meta.TotalChunks,
				UploadedParts: uploadedParts,
			}, true, nil
		}
	}

	record, err := a.fileUploadRepository.FindActiveFileUploadByUploaderAndHash(ctx, uploaderId, fileHash, now)
	if err != nil {
		return nil, false, err
	}
	if record == nil {
		return nil, false, nil
	}
	if record.UploadMode != multipartUploadMode || record.StorageUploadId == "" {
		return nil, false, ErrUploadBusy
	}
	meta := filecache.UploadMeta{
		UploadId:        record.UploadId,
		UploadMode:      record.UploadMode,
		StorageUploadId: record.StorageUploadId,
		FileId:          record.FileId,
		UploaderId:      record.UploaderId,
		Bucket:          a.storage.Bucket(),
		ObjectKey:       record.ObjectKey,
		FileName:        record.FileName,
		ContentType:     record.ContentType,
		Size:            record.ExpectedSize,
		FileHash:        record.FileHash,
		ChunkSize:       record.ChunkSize,
		TotalChunks:     record.TotalChunks,
		CreatedAt:       record.CreatedAt,
		ExpiresAt:       record.ExpiresAt,
		Status:          record.Status,
	}
	ttl := time.Until(time.UnixMilli(record.ExpiresAt))
	if ttl <= 0 {
		return nil, false, nil
	}
	if err := a.fileCache.SetUploadMeta(ctx, meta, ttl); err != nil {
		log.Printf("回填分片上传元数据缓存失败：上传=%s 错误=%v", record.UploadId, err)
	}
	if err := a.fileCache.SetActiveFileUpload(ctx, uploaderId, fileHash, record.UploadId, ttl); err != nil {
		log.Printf("回填活跃上传缓存失败：上传=%s 错误=%v", record.UploadId, err)
	}
	uploadedParts, err := a.uploadedPartNumbers(ctx, record.ObjectKey, record.StorageUploadId)
	if err != nil {
		return nil, false, err
	}
	return &MultipartInitResDTO{
		UploadID: record.UploadId, FileID: record.FileId,
		Status:    uploadStatusUploading,
		ChunkSize: record.ChunkSize, TotalChunks: record.TotalChunks,
		UploadedParts: uploadedParts,
	}, true, nil
}

func (a *FileApplication) PresignMultipartParts(ctx context.Context, uploadId string, uploaderId string, partNumbers []int) ([]MultipartPartURLDTO, error) {
	meta, err := a.loadMultipartMeta(ctx, uploadId, uploaderId)
	if err != nil {
		return nil, err
	}

	if meta.Status != uploadStatusUploading ||
		meta.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}
	if len(partNumbers) == 0 {
		return nil, ErrInvalidPart
	}
	if a.options.MaxMultipartParts > 0 && len(partNumbers) > a.options.MaxMultipartParts {
		return nil, ErrTooManyParts
	}
	seen := make(map[int]struct{}, len(partNumbers))

	// 后端返回切片上传的 url，由前端通过 Promise.all 实现并发上传给 minio
	result := make([]MultipartPartURLDTO, 0, len(partNumbers))
	for _, partNumber := range partNumbers {
		if partNumber <= 0 || partNumber > meta.TotalChunks {
			return nil, ErrInvalidPart
		}
		if _, ok := seen[partNumber]; ok {
			continue
		}
		seen[partNumber] = struct{}{}
		url, err := a.storage.PresignMultipartPart(ctx, meta.ObjectKey, meta.StorageUploadId, partNumber, a.partURLTTL())
		if err != nil {
			return nil, err
		}
		result = append(result, MultipartPartURLDTO{UploadID: uploadId, PartNumber: partNumber, URL: url})
	}
	return result, nil
}

func (a *FileApplication) acquireFileCompleteLock(ctx context.Context, uploadId string) (func(), <-chan struct{}, error) {
	lockTTL := a.fileCompleteLockTTL()
	lockToken, locked, err := a.fileCache.AcquireFileCompleteLock(ctx, uploadId, lockTTL)
	if err != nil {
		return nil, nil, err
	}

	if !locked {
		return nil, nil, ErrUploadBusy
	}

	reneCtx, stopRene := context.WithCancel(context.Background())
	reneInterval := lockTTL / 3
	if reneInterval <= 0 {
		reneInterval = time.Second
	}

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
				renewCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				renewed, err := a.fileCache.RenewFileCompleteLock(renewCtx, uploadId, lockToken, lockTTL)
				cancel()
				if err != nil || !renewed {
					if err == nil {
						err = ErrFileCompleteLockLost
					}
					log.Printf("续期文件完成锁失败：上传=%s 错误=%v", uploadId, err)
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
		unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.fileCache.ReleaseFileCompleteLock(unlockCtx, uploadId, lockToken)
	}, lockLost, nil
}

func (a *FileApplication) loadMultipartMeta(ctx context.Context, uploadId string, uploaderId string) (*filecache.UploadMeta, error) {
	meta, cacheErr := a.fileCache.GetUploadMeta(ctx, uploadId)
	now := time.Now().UnixMilli()
	if cacheErr == nil && meta != nil &&
		meta.UploadId == uploadId &&
		meta.UploadMode == multipartUploadMode &&
		meta.UploaderId == uploaderId &&
		(meta.Status == uploadStatusCompleted ||
			(meta.Status == uploadStatusUploading && meta.ExpiresAt > now)) {
		return meta, nil
	}

	if cacheErr != nil {
		log.Printf("读取分片上传元数据缓存失败，回源数据库：上传=%s 错误=%v", uploadId, cacheErr)
	}

	// 缓存缺失、格式不正确或状态可能过期时，以数据库为准。
	record, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadId)
	if err != nil {
		return nil, err
	}
	if record == nil || record.UploadMode != multipartUploadMode {
		return nil, ErrInvalidUpload
	}
	if record.UploaderId != uploaderId {
		return nil, ErrUploadUnauthorized
	}

	meta = uploadMetaFromRecord(record, a.storage.Bucket())

	ttl := a.multipartTTL()
	if record.Status != uploadStatusCompleted {
		ttl = time.Until(time.UnixMilli(record.ExpiresAt))
	}
	if ttl > 0 {
		if cacheErr := a.fileCache.SetUploadMeta(ctx, *meta, ttl); cacheErr != nil {
			log.Printf("回填分片上传元数据缓存失败：上传=%s 错误=%v", record.UploadId, cacheErr)
		}
	}

	return meta, nil
}

func (a *FileApplication) expectedMultipartPartSize(meta *filecache.UploadMeta, partNumber int) int64 {
	if partNumber == meta.TotalChunks {
		return meta.Size - int64(meta.TotalChunks-1)*meta.ChunkSize
	}

	return meta.ChunkSize
}

func (a *FileApplication) resolveCompleteMultiParts(meta *filecache.UploadMeta, parts []objectstorage.MultipartPart) ([]objectstorage.MultipartPart, error) {
	partByNumber := make(map[int]objectstorage.MultipartPart)
	invalidParts := make([]int, 0)

	for _, part := range parts {
		if part.PartNumber <= 0 || part.PartNumber > meta.TotalChunks {
			invalidParts = append(invalidParts, part.PartNumber)
			continue
		}

		if part.ETag == "" {
			invalidParts = append(invalidParts, part.PartNumber)
			continue
		}

		if part.Size != a.expectedMultipartPartSize(meta, part.PartNumber) {
			invalidParts = append(invalidParts, part.PartNumber)
			continue
		}

		partByNumber[part.PartNumber] = part
	}

	missingParts := make([]int, 0)
	for i := 1; i <= meta.TotalChunks; i++ {
		if _, ok := partByNumber[i]; !ok {
			missingParts = append(missingParts, i)
		}
	}

	if len(missingParts) > 0 || len(invalidParts) > 0 {
		return nil, &UploadIncompleteError{
			MissingParts: missingParts,
			InvalidParts: invalidParts,
		}
	}

	// 对 multiparts 进行排序
	completeParts := make([]objectstorage.MultipartPart, 0, meta.TotalChunks)
	for partNumber := 1; partNumber <= meta.TotalChunks; partNumber++ {
		part := partByNumber[partNumber]
		completeParts = append(completeParts, objectstorage.MultipartPart{
			PartNumber: partNumber,
			ETag:       part.ETag,
		})
	}

	return completeParts, nil
}

func (a *FileApplication) completeMultipartObject(ctx context.Context, meta *filecache.UploadMeta, parts []objectstorage.MultipartPart) error {
	return a.storage.CompleteMultipartUpload(ctx, meta.ObjectKey, meta.StorageUploadId, parts)
}

func (a *FileApplication) afterUploadCompleted(ctx context.Context, meta *filecache.UploadMeta, file *fileentity.File, metaTTL time.Duration) {
	if err := a.fileCache.SetFileMetadata(ctx, file, a.cacheTTL()); err != nil {
		log.Printf("完成上传后写入文件缓存失败：文件=%s 错误=%v", file.FileId, err)
	}
	if err := a.fileCache.SetFileByUploaderAndHash(ctx, meta.UploaderId, meta.FileHash, file, a.cacheTTL()); err != nil {
		log.Printf("完成上传后写入文件哈希缓存失败：文件=%s 错误=%v", file.FileId, err)
	}
	meta.FileId = file.FileId
	meta.ContentType = file.ContentType
	meta.Status = uploadStatusCompleted
	if err := a.fileCache.SetUploadMeta(ctx, *meta, metaTTL); err != nil {
		log.Printf("完成上传后更新上传任务缓存失败：上传=%s 错误=%v", meta.UploadId, err)
	}
	if err := a.fileCache.DeleteActiveFileUploadIfMatches(ctx, meta.UploaderId, meta.FileHash, meta.UploadId); err != nil {
		log.Printf("完成上传后删除活跃上传缓存失败：上传=%s 错误=%v", meta.UploadId, err)
	}
}

func (a *FileApplication) uploadedPartNumbers(ctx context.Context, objectKey string, uploadId string) ([]int, error) {
	parts, err := a.storage.ListMultipartParts(ctx, objectKey, uploadId)
	if err != nil {
		return nil, err
	}
	res := make([]int, 0, len(parts))
	for _, part := range parts {
		res = append(res, part.PartNumber)
	}
	sort.Ints(res)
	return res, nil
}

func (a *FileApplication) GetAttachmentAccessURLs(ctx context.Context, userId string, attachmentIds []string) ([]*AttachmentAccessURLDTO, error) {
	if userId == "" {
		return nil, ErrUploadUnauthorized
	}
	if a.messageAttachmentsRepository == nil {
		return nil, ErrUploadUnauthorized
	}

	ids := uniqueAttachmentIDs(attachmentIds)
	if len(ids) == 0 {
		return []*AttachmentAccessURLDTO{}, nil
	}

	// 必须先根据当前用户校验附件权限，再读取公共附件访问缓存。
	attachments, err := a.messageAttachmentsRepository.FindUserAccessAttachments(ctx, userId, ids)
	if err != nil {
		return nil, err
	}

	cards := make(map[string]*filecache.AttachmentFileCard)
	accessURLs := make(map[string]*filecache.AttachmentURL)
	if a.fileCache != nil {
		cards, err = a.fileCache.GetAttachmentFileCardBatch(ctx, ids)
		if err != nil {
			log.Printf("读取附件文件卡片缓存失败，继续回源：错误=%v", err)
			cards = make(map[string]*filecache.AttachmentFileCard)
		}
		accessURLs, err = a.fileCache.GetAttachmentURLBatch(ctx, ids)
		if err != nil {
			log.Printf("读取附件访问地址缓存失败，继续回源：错误=%v", err)
			accessURLs = make(map[string]*filecache.AttachmentURL)
		}
	}

	resolved := make(map[string]*AttachmentAccessURLDTO, len(ids))
	missedCards := make([]*messageentity.MessageAttachment, 0, len(ids))
	for _, attachmentId := range ids {
		attachment := attachments[attachmentId]
		if attachment == nil {
			// 未通过权限校验的附件不返回任何信息。
			continue
		}
		if card := cards[attachmentId]; card != nil && card.ObjectKey != "" {
			if accessURL := accessURLs[attachmentId]; accessURL != nil {
				resolved[attachmentId] = attachmentAccessDTOFromCache(card, accessURL)
			}
			continue
		}
		missedCards = append(missedCards, attachment)
	}

	// 文件卡片缺失时批量回源 files 表并回填缓存。
	if len(missedCards) > 0 {
		fileIDs := make([]string, 0, len(missedCards))
		seenFileIDs := make(map[string]struct{}, len(missedCards))
		for _, attachment := range missedCards {
			if attachment.FileId == "" {
				continue
			}
			if _, ok := seenFileIDs[attachment.FileId]; ok {
				continue
			}
			seenFileIDs[attachment.FileId] = struct{}{}
			fileIDs = append(fileIDs, attachment.FileId)
		}

		if len(fileIDs) > 0 {
			if a.fileRepository == nil {
				return nil, ErrFileNotFound
			}
			files, err := a.fileRepository.FindFilesByFileIDs(ctx, fileIDs)
			if err != nil {
				return nil, err
			}

			cardValues := make([]*filecache.AttachmentFileCard, 0, len(missedCards))
			for _, attachment := range missedCards {
				file := files[attachment.FileId]
				if file == nil || (file.Status != "" && file.Status != fileentity.FileStatusUploaded) {
					continue
				}
				card := &filecache.AttachmentFileCard{
					AttachmentID:       attachment.AttachmentId,
					FileID:             file.FileId,
					ObjectKey:          file.ObjectKey,
					FileName:           file.FileName,
					ContentType:        file.ContentType,
					Size:               file.Size,
					CType:              int(attachment.Kind),
					AttachmentExpireAt: attachment.ExpireAt,
				}
				cards[attachment.AttachmentId] = card
				cardValues = append(cardValues, card)
			}

			if a.fileCache != nil {
				if err := a.fileCache.SetAttachmentFileCardBatch(ctx, cardValues, a.attachmentFileCardCacheTTL()); err != nil {
					log.Printf("回填附件文件卡片缓存失败：错误=%v", err)
				}
			}
		}
	}

	// 为缓存命中或刚刚回源的卡片补齐缺失的短期 URL。
	urlValues := make([]*filecache.AttachmentURL, 0)
	for _, attachmentId := range ids {
		attachment := attachments[attachmentId]
		card := cards[attachmentId]
		if attachment == nil || card == nil || card.ObjectKey == "" {
			continue
		}
		if accessURL := accessURLs[attachmentId]; accessURL != nil {
			resolved[attachmentId] = attachmentAccessDTOFromCache(card, accessURL)
			continue
		}

		ttl := a.urlTTL()
		mediaURL, err := a.storage.PresignedGetURL(ctx, card.ObjectKey, ttl, card.ContentType, card.FileName)
		if err != nil {
			return nil, err
		}
		accessURL := &filecache.AttachmentURL{
			AttachmentID: attachmentId,
			MediaURL:     mediaURL,
			ExpiresAt:    time.Now().Add(ttl).Unix(),
		}
		resolved[attachmentId] = attachmentAccessDTOFromCache(card, accessURL)
		urlValues = append(urlValues, accessURL)
	}
	if a.fileCache != nil && len(urlValues) > 0 {
		if err := a.fileCache.SetAttachmentURLBatch(ctx, urlValues, a.attachmentAccessCacheTTL()); err != nil {
			log.Printf("回填附件访问地址缓存失败：错误=%v", err)
		}
	}

	result := make([]*AttachmentAccessURLDTO, 0, len(resolved))
	for _, attachmentId := range ids {
		if value := resolved[attachmentId]; value != nil {
			result = append(result, value)
		}
	}
	return result, nil
}

func (a *FileApplication) GetAttachmentAccessURL(ctx context.Context, userId string, attachmentId string) (*AttachmentAccessURLDTO, error) {
	if attachmentId == "" {
		return nil, ErrUploadUnauthorized
	}
	values, err := a.GetAttachmentAccessURLs(ctx, userId, []string{attachmentId})
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrFileNotFound
	}
	return values[0], nil
}

func attachmentAccessDTOFromCache(card *filecache.AttachmentFileCard, accessURL *filecache.AttachmentURL) *AttachmentAccessURLDTO {
	return &AttachmentAccessURLDTO{
		AttachmentID: card.AttachmentID,
		FileID:       card.FileID,
		FileName:     card.FileName,
		ContentType:  card.ContentType,
		Size:         card.Size,
		MediaURL:     accessURL.MediaURL,
		ThumbURL:     accessURL.ThumbURL,
		ExpiresAt:    accessURL.ExpiresAt,
		CType:        card.CType,
		Width:        card.Width,
		Height:       card.Height,
		DurationMs:   card.DurationMs,
	}
}

func uniqueAttachmentIDs(ids []string) []string {
	result := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func (a *FileApplication) GetFileForUser(ctx context.Context, fileId, userId string) (*FileDTO, error) {
	var (
		file *fileentity.File
		err  error
	)

	file, err = a.fileCache.GetFileMetadata(ctx, fileId)
	if err != nil {
		return nil, err
	}

	if file == nil {
		file, err = a.fileRepository.FindFileByID(ctx, fileId)
		if err != nil {
			return nil, err
		}
		if file == nil {
			return nil, ErrFileNotFound
		}
	}
	if file.Status != "" && file.Status != fileentity.FileStatusUploaded {
		return nil, fmt.Errorf("文件当前不可引用")
	}

	if file.UploaderId != userId {
		return nil, ErrUploadUnauthorized
	}

	_ = a.fileCache.SetFileMetadata(ctx, file, a.cacheTTL())

	return toFileDTO(file), nil
}

func (a *FileApplication) buildObjectKey(uploaderId, fileId string) string {
	day := time.Now().Format("20060102")
	uploaderId = strings.TrimSpace(uploaderId)
	if uploaderId == "" {
		uploaderId = "unknown"
	}
	return "uploads/" + day + "/" + uploaderId + "/" + fileId
}

func (a *FileApplication) uploadInitLockTTL() time.Duration {
	if a.options.MultipartInitLockTTL <= 0 {
		return 15 * time.Second
	}
	return a.options.MultipartInitLockTTL
}

// 文件完成操作的锁租期需要自动续期，否则对象校验或分片合并时间过长会导致并发完成。
func (a *FileApplication) fileCompleteLockTTL() time.Duration {
	if a.options.MultipartCompleteLockTTL <= 0 {
		return 30 * time.Second
	}
	return a.options.MultipartCompleteLockTTL
}

func (a *FileApplication) directUploadURLTTL() time.Duration {
	if a.options.DirectUploadURLTTL <= 0 {
		return 30 * time.Minute
	}
	return a.options.DirectUploadURLTTL
}

func (a *FileApplication) partURLTTL() time.Duration {
	if a.options.PartURLTTL <= 0 {
		return 15 * time.Minute
	}
	return a.options.PartURLTTL
}

func (a *FileApplication) multipartTTL() time.Duration {
	ttl := a.options.MultipartTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return ttl
}

func (a *FileApplication) cacheTTL() time.Duration {
	ttl := a.options.CacheTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return ttl
}

func (a *FileApplication) urlTTL() time.Duration {
	ttl := a.options.URLTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	return ttl
}

func (a *FileApplication) attachmentAccessCacheTTL() time.Duration {
	ttl := a.options.AttachmentAccessCacheTTL
	urlTTL := a.urlTTL()
	if ttl <= 0 || ttl >= urlTTL {
		ttl = urlTTL - 30*time.Second
	}
	if ttl <= 0 {
		ttl = urlTTL
	}
	return ttl
}

func (a *FileApplication) attachmentFileCardCacheTTL() time.Duration {
	ttl := a.options.AttachmentFileCardTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return ttl
}
