package file

import (
	"context"
	"fmt"
	"log"
	"mime"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"IM_backend/configs"
	filecache "IM_backend/internal/application/ports/cache/file"
	idport "IM_backend/internal/application/ports/id"
	"IM_backend/internal/application/ports/objectstorage"
	"IM_backend/internal/application/ports/transaction"
	domainfile "IM_backend/internal/domain/file"
	domainmessage "IM_backend/internal/domain/message"
)

type FileApplication struct {
	config                       configs.Config
	fileRepository               domainfile.FileRepository
	fileUploadRepository         domainfile.FileUploadRepository
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository
	fileCache                    filecache.FileCache
	storage                      objectstorage.ObjectStorage
	idGenerator                  idport.Generator
	sf                           singleflight.Group
	txManager                    transaction.Manager
}

const (
	uploadStatusUploading = "uploading"
	uploadStatusCompleted = "completed"
	multipartUploadMode   = "multipart"
	directUploadMode      = "direct"
)

func NewFileApplication(
	config configs.Config,
	fileRepository domainfile.FileRepository,
	fileUploadRepository domainfile.FileUploadRepository,
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository,
	fileCache filecache.FileCache,
	storage objectstorage.ObjectStorage,
	idGenerator idport.Generator,
	txManager transaction.Manager,
) *FileApplication {
	return &FileApplication{
		config:                       config,
		fileRepository:               fileRepository,
		fileUploadRepository:         fileUploadRepository,
		messageAttachmentsRepository: messageAttachmentsRepository,
		fileCache:                    fileCache,
		storage:                      storage,
		idGenerator:                  idGenerator,
		txManager:                    txManager,
	}
}

func (a *FileApplication) validateContentType(contentType string) error {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if mediaType, _, err := mime.ParseMediaType(contentType); err == nil {
		contentType = mediaType
	}
	for _, allowed := range a.config.Storage.MinIO.AllowedContentTypes {
		if contentType == strings.ToLower(strings.TrimSpace(allowed)) {
			return nil
		}
	}
	return ErrUnsupportedFileType
}

func (a *FileApplication) initMultipartUpload(ctx context.Context, dto UploadInitDTO) (*UploadInitResDTO, error) {
	fileHash := dto.FileHash

	if dto.UploaderID == "" || dto.FileName == "" ||
		fileHash == "" || dto.ChunkSize <= 0 || dto.TotalChunks <= 0 {
		return nil, ErrInvalidPart
	}
	if dto.Size > a.config.Storage.MinIO.MaxFileSizeBytes {
		return nil, ErrFileTooLarge
	}
	expectedChunks := dto.Size / dto.ChunkSize
	if dto.Size%dto.ChunkSize != 0 {
		expectedChunks++
	}
	if int64(dto.TotalChunks) != expectedChunks {
		return nil, ErrInvalidPart
	}
	if dto.TotalChunks > a.config.Storage.MinIO.MaxMultipartParts {
		return nil, ErrTooManyParts
	}

	if result, found, err := a.findMultipartInitResult(ctx, dto.UploaderID, fileHash); err != nil || found {
		return result, err
	}

	// 新上传文件
	lockToken, locked, err := a.fileCache.AcquireFileInitLock(ctx, dto.UploaderID, fileHash, time.Duration(a.config.Storage.MinIO.MultipartInitLockTTLSeconds)*time.Second)
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
	fileID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	uploadID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	objectKey := a.buildObjectKey(dto.UploaderID, fileID)
	storageUploadID, err := a.storage.CreateMultipartUpload(
		ctx,
		objectKey,
		dto.ContentType,
	)
	if err != nil {
		return nil, err
	}

	ttl := time.Duration(a.config.Storage.MinIO.MultipartTTL) * time.Second
	now := time.Now()
	createdAt := now.UnixMilli()
	expiresAt := now.Add(ttl).UnixMilli()
	meta := filecache.UploadMeta{
		UploadMode:      multipartUploadMode,
		UploadID:        uploadID,
		StorageUploadID: storageUploadID,
		FileID:          fileID,
		UploaderID:      dto.UploaderID,
		Bucket:          a.storage.Bucket(),
		ObjectKey:       objectKey,
		FileName:        dto.FileName,
		ContentType:     dto.ContentType,
		Size:            dto.Size,
		FileHash:        fileHash,
		ChunkSize:       dto.ChunkSize,
		TotalChunks:     dto.TotalChunks,
		ExpiresAt:       expiresAt,
		Status:          uploadStatusUploading,
	}
	if err := a.fileUploadRepository.CreateFileUpload(ctx, domainfile.Upload{
		UploadID:        meta.UploadID,
		FileID:          meta.FileID,
		UploaderID:      meta.UploaderID,
		UploadMode:      multipartUploadMode,
		StorageUploadID: meta.StorageUploadID,
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
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadID)
		return nil, err
	}

	if err := a.fileCache.SetUploadMeta(ctx, meta, ttl); err != nil {
		// 取消 minio 上传
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadID)
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(ctx, uploadID)
		return nil, err
	}

	if err := a.fileCache.SetActiveFileUpload(ctx, dto.UploaderID, fileHash, uploadID, ttl); err != nil {
		_ = a.storage.AbortMultipartUpload(ctx, meta.ObjectKey, storageUploadID)
		_ = a.fileCache.DeleteUploadMeta(ctx, uploadID)
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(ctx, uploadID)
		return nil, err
	}

	return &UploadInitResDTO{
		UploadMode:    multipartUploadMode,
		UploadID:      uploadID,
		Status:        uploadStatusUploading,
		ChunkSize:     dto.ChunkSize,
		TotalChunks:   dto.TotalChunks,
		UploadedParts: []int{},
	}, nil
}

// InitUpload 根据文件大小选择上传模式，两个分支共用同一套请求和结果。
func (a *FileApplication) InitUpload(ctx context.Context, dto UploadInitDTO) (*UploadInitResDTO, error) {
	if dto.Size <= 0 {
		return nil, ErrInvalidUpload
	}
	dto.FileHash = strings.ToLower(strings.TrimSpace(dto.FileHash))
	if err := a.validateContentType(dto.ContentType); err != nil {
		return nil, ErrInvalidMIME
	}
	if dto.Size <= a.config.Storage.MinIO.DirectUploadMaxSizeBytes {
		return a.initDirectUpload(ctx, dto)
	}
	return a.initMultipartUpload(ctx, dto)
}

func (a *FileApplication) initDirectUpload(ctx context.Context, dto UploadInitDTO) (*UploadInitResDTO, error) {
	fileHash := dto.FileHash

	if dto.UploaderID == "" || dto.FileName == "" || fileHash == "" {
		return nil, ErrInvalidUpload
	}
	if dto.Size > a.config.Storage.MinIO.MaxFileSizeBytes {
		return nil, ErrFileTooLarge
	}

	lookupKey := fmt.Sprintf("uploaderID:%s:fileHash:%s", dto.UploaderID, fileHash)
	value, err, _ := a.sf.Do(lookupKey, func() (any, error) {
		file, err := a.fileCache.GetFileByUploaderAndHash(ctx, dto.UploaderID, fileHash)
		if err != nil {
			return nil, err
		}
		if file != nil && file.Status == domainfile.FileStatusUploaded {
			return &UploadInitResDTO{UploadMode: directUploadMode, FileID: file.FileID, Status: uploadStatusCompleted}, nil
		}
		uploadID, err := a.fileCache.GetActiveFileUploadID(ctx, dto.UploaderID, fileHash)
		if err != nil || uploadID == "" {
			return nil, err
		}
		existing, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadID)
		if err != nil {
			return nil, err
		}
		if existing != nil && existing.UploadMode == directUploadMode && existing.UploaderID == dto.UploaderID &&
			existing.Status == uploadStatusUploading && existing.ExpiresAt > time.Now().UnixMilli() {
			return a.presignDirectUpload(ctx, existing)
		}
		if existing != nil && existing.UploadMode != directUploadMode {
			return nil, ErrUploadBusy
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if value != nil {
		return value.(*UploadInitResDTO), nil
	}

	// 获取文件上传初始化锁，避免文件重复初始化
	lockToken, locked, err := a.fileCache.AcquireFileInitLock(ctx, dto.UploaderID, fileHash, time.Duration(a.config.Storage.MinIO.MultipartInitLockTTLSeconds)*time.Second)
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
		return &UploadInitResDTO{UploadMode: directUploadMode, FileID: file.FileID, Status: uploadStatusCompleted}, nil
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

	fileID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	uploadID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	ttl := time.Duration(a.config.Storage.MinIO.DirectUploadURLTTLSeconds) * time.Second
	record := domainfile.Upload{
		UploadID:     uploadID,
		FileID:       fileID,
		UploaderID:   dto.UploaderID,
		UploadMode:   directUploadMode,
		FileHash:     fileHash,
		ObjectKey:    a.buildObjectKey(dto.UploaderID, fileID),
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
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(context.Background(), uploadID)
		return nil, err
	}
	meta := uploadMetaFromRecord(&record, a.storage.Bucket())
	if err := a.fileCache.SetUploadMeta(ctx, *meta, ttl); err != nil {
		_ = a.fileUploadRepository.DeleteFileUploadByUploadID(context.Background(), uploadID)
		return nil, err
	}
	if err := a.fileCache.SetActiveFileUpload(ctx, dto.UploaderID, fileHash, uploadID, ttl); err != nil {
		log.Printf("写入直传活跃上传缓存失败：上传=%s 错误=%v", uploadID, err)
	}
	return result, nil
}

func (a *FileApplication) presignDirectUpload(ctx context.Context, record *domainfile.Upload) (*UploadInitResDTO, error) {
	ttl := time.Until(time.UnixMilli(record.ExpiresAt))
	if ttl <= 0 {
		return nil, ErrInvalidUpload
	}
	url, err := a.storage.PresignedPutURL(ctx, record.ObjectKey, ttl)
	if err != nil {
		return nil, err
	}
	return &UploadInitResDTO{
		UploadMode: directUploadMode,
		UploadID:   record.UploadID,
		Status:     uploadStatusUploading,
		URL:        url,
		ExpiresAt:  time.UnixMilli(record.ExpiresAt).Unix(),
	}, nil
}

func (a *FileApplication) loadUploadMeta(ctx context.Context, uploadID, uploaderID string) (*filecache.UploadMeta, error) {
	if uploadID == "" || uploaderID == "" {
		return nil, ErrInvalidUpload
	}
	meta, cacheErr := a.fileCache.GetUploadMeta(ctx, uploadID)
	now := time.Now().UnixMilli()
	if cacheErr == nil && meta != nil && meta.UploadID == uploadID && meta.UploaderID == uploaderID &&
		(meta.UploadMode == directUploadMode || meta.UploadMode == multipartUploadMode) &&
		(meta.Status == uploadStatusCompleted || (meta.Status == uploadStatusUploading && meta.ExpiresAt > now)) {
		return meta, nil
	}
	if cacheErr != nil {
		log.Printf("读取上传元数据缓存失败，回源数据库：上传=%s 错误=%v", uploadID, cacheErr)
	}
	record, err := a.fileUploadRepository.FindFileUploadByID(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if record == nil || (record.UploadMode != directUploadMode && record.UploadMode != multipartUploadMode) {
		return nil, ErrInvalidUpload
	}
	if record.UploaderID != uploaderID {
		return nil, ErrUploadUnauthorized
	}
	meta = uploadMetaFromRecord(record, a.storage.Bucket())
	ttl := time.Duration(a.config.Storage.MinIO.CacheTTLSeconds) * time.Second
	if record.UploadMode == multipartUploadMode {
		ttl = time.Duration(a.config.Storage.MinIO.MultipartTTL) * time.Second
	}
	if record.Status != uploadStatusCompleted {
		ttl = time.Until(time.UnixMilli(record.ExpiresAt))
	}
	if ttl > 0 {
		if err := a.fileCache.SetUploadMeta(ctx, *meta, ttl); err != nil {
			log.Printf("回填上传元数据缓存失败：上传=%s 错误=%v", uploadID, err)
		}
	}
	return meta, nil
}

func uploadMetaFromRecord(record *domainfile.Upload, bucket string) *filecache.UploadMeta {
	if record == nil {
		return nil
	}
	return &filecache.UploadMeta{
		UploadID:        record.UploadID,
		UploadMode:      record.UploadMode,
		StorageUploadID: record.StorageUploadID,
		FileID:          record.FileID,
		UploaderID:      record.UploaderID,
		Bucket:          bucket,
		ObjectKey:       record.ObjectKey,
		FileName:        record.FileName,
		ContentType:     record.ContentType,
		Size:            record.ExpectedSize,
		FileHash:        record.FileHash,
		ChunkSize:       record.ChunkSize,
		TotalChunks:     record.TotalChunks,
		ExpiresAt:       record.ExpiresAt,
		Status:          record.Status,
	}
}

func (a *FileApplication) findFileByUploaderAndHash(ctx context.Context, uploaderID, fileHash string) (*domainfile.File, error) {
	if uploaderID == "" || fileHash == "" {
		return nil, nil
	}
	file, err := a.fileCache.GetFileByUploaderAndHash(ctx, uploaderID, fileHash)
	if err == nil && file != nil && file.UploaderID == uploaderID && file.Status == domainfile.FileStatusUploaded {
		return file, nil
	}
	if err != nil {
		log.Printf("读取直传文件缓存失败，回源数据库：用户=%s 哈希=%s 错误=%v", uploaderID, fileHash, err)
	}

	file, err = a.fileRepository.FindUploadedFileByUploaderAndHash(ctx, uploaderID, fileHash)
	if err != nil || file == nil {
		return nil, err
	}

	if cacheErr := a.fileCache.SetFileMetadata(ctx, file, time.Duration(a.config.Storage.MinIO.CacheTTLSeconds)*time.Second); cacheErr != nil {
		log.Printf("回填直传文件缓存失败：文件=%s 错误=%v", file.FileID, cacheErr)
	}
	if cacheErr := a.fileCache.SetFileByUploaderAndHash(ctx, uploaderID, fileHash, file, time.Duration(a.config.Storage.MinIO.CacheTTLSeconds)*time.Second); cacheErr != nil {
		log.Printf("回填直传文件哈希缓存失败：文件=%s 错误=%v", file.FileID, cacheErr)
	}
	return file, nil
}

func (a *FileApplication) findMultipartInitResult(ctx context.Context, uploaderID, fileHash string) (*UploadInitResDTO, bool, error) {
	file, err := a.findFileByUploaderAndHash(ctx, uploaderID, fileHash)
	if err != nil {
		return nil, false, err
	}
	if file != nil {
		return &UploadInitResDTO{
			UploadMode: multipartUploadMode,
			FileID:     file.FileID,
			Status:     uploadStatusCompleted,
		}, true, nil
	}

	return a.findActiveMultipartUpload(ctx, uploaderID, fileHash)
}

func (a *FileApplication) findActiveMultipartUpload(ctx context.Context, uploaderID, fileHash string) (*UploadInitResDTO, bool, error) {
	now := time.Now().UnixMilli()
	if uploadID, err := a.fileCache.GetActiveFileUploadID(ctx, uploaderID, fileHash); err == nil && uploadID != "" {
		meta, metaErr := a.fileCache.GetUploadMeta(ctx, uploadID)
		if metaErr == nil && meta != nil &&
			meta.UploadID == uploadID &&
			meta.UploaderID == uploaderID &&
			meta.FileHash == fileHash &&
			meta.Status == uploadStatusUploading &&
			meta.ExpiresAt > now &&
			meta.StorageUploadID != "" {
			uploadedParts, partsErr := a.uploadedPartNumbers(ctx, meta.ObjectKey, meta.StorageUploadID)
			if partsErr != nil {
				return nil, false, partsErr
			}
			return &UploadInitResDTO{
				UploadMode: multipartUploadMode,
				UploadID:   uploadID, FileID: meta.FileID,
				Status:    uploadStatusUploading,
				ChunkSize: meta.ChunkSize, TotalChunks: meta.TotalChunks,
				UploadedParts: uploadedParts,
			}, true, nil
		}
	}

	record, err := a.fileUploadRepository.FindActiveFileUploadByUploaderAndHash(ctx, uploaderID, fileHash, now)
	if err != nil {
		return nil, false, err
	}
	if record == nil {
		return nil, false, nil
	}
	if record.UploadMode != multipartUploadMode || record.StorageUploadID == "" {
		return nil, false, ErrUploadBusy
	}
	meta := filecache.UploadMeta{
		UploadID:        record.UploadID,
		UploadMode:      record.UploadMode,
		StorageUploadID: record.StorageUploadID,
		FileID:          record.FileID,
		UploaderID:      record.UploaderID,
		Bucket:          a.storage.Bucket(),
		ObjectKey:       record.ObjectKey,
		FileName:        record.FileName,
		ContentType:     record.ContentType,
		Size:            record.ExpectedSize,
		FileHash:        record.FileHash,
		ChunkSize:       record.ChunkSize,
		TotalChunks:     record.TotalChunks,
		ExpiresAt:       record.ExpiresAt,
		Status:          record.Status,
	}
	ttl := time.Until(time.UnixMilli(record.ExpiresAt))
	if ttl <= 0 {
		return nil, false, nil
	}
	if err := a.fileCache.SetUploadMeta(ctx, meta, ttl); err != nil {
		log.Printf("回填分片上传元数据缓存失败：上传=%s 错误=%v", record.UploadID, err)
	}
	if err := a.fileCache.SetActiveFileUpload(ctx, uploaderID, fileHash, record.UploadID, ttl); err != nil {
		log.Printf("回填活跃上传缓存失败：上传=%s 错误=%v", record.UploadID, err)
	}
	uploadedParts, err := a.uploadedPartNumbers(ctx, record.ObjectKey, record.StorageUploadID)
	if err != nil {
		return nil, false, err
	}
	return &UploadInitResDTO{
		UploadMode: multipartUploadMode,
		UploadID:   record.UploadID, FileID: record.FileID,
		Status:    uploadStatusUploading,
		ChunkSize: record.ChunkSize, TotalChunks: record.TotalChunks,
		UploadedParts: uploadedParts,
	}, true, nil
}

func (a *FileApplication) buildObjectKey(uploaderID, fileID string) string {
	day := time.Now().Format("20060102")
	uploaderID = strings.TrimSpace(uploaderID)
	if uploaderID == "" {
		uploaderID = "unknown"
	}
	return "uploads/" + day + "/" + uploaderID + "/" + fileID
}

func (a *FileApplication) PresignMultipartParts(ctx context.Context, uploadID string, uploaderID string, partNumbers []int) ([]MultipartPartURLDTO, error) {
	meta, err := a.loadUploadMeta(ctx, uploadID, uploaderID)
	if err != nil {
		return nil, err
	}

	if meta.UploadMode != multipartUploadMode || meta.Status != uploadStatusUploading ||
		meta.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}
	if len(partNumbers) == 0 {
		return nil, ErrInvalidPart
	}
	if len(partNumbers) > a.config.Storage.MinIO.MaxMultipartParts {
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
		url, err := a.storage.PresignMultipartPart(ctx, meta.ObjectKey, meta.StorageUploadID, partNumber, time.Duration(a.config.Storage.MinIO.PartURLTTLSeconds)*time.Second)
		if err != nil {
			return nil, err
		}
		result = append(result, MultipartPartURLDTO{UploadID: uploadID, PartNumber: partNumber, URL: url})
	}
	return result, nil
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

func (a *FileApplication) uploadedPartNumbers(ctx context.Context, objectKey string, uploadID string) ([]int, error) {
	parts, err := a.storage.ListMultipartParts(ctx, objectKey, uploadID)
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
