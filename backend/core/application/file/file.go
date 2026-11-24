package file

import (
	"IM_backend/configs"
	filecache "IM_backend/core/application/ports/cache/file"
	idport "IM_backend/core/application/ports/id"
	"IM_backend/core/application/ports/objectstorage"
	"IM_backend/core/application/ports/transaction"
	domainfile "IM_backend/core/domain/file"
	domainmessage "IM_backend/core/domain/message"
	"context"
	"errors"
	"mime"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// 分片规格服务端下发、按纯 hash 全局秒传、name 前缀隔离权限
// 文件字节由客户端凭预签名 URL 直传 MinIO，后端不搬运；上传态只在 file_uploads 表
type FileApplication struct {
	config                       configs.Config
	fileRepository               domainfile.FileRepository
	fileUploadRepository         domainfile.FileUploadRepository
	messageAttachmentsRepository domainmessage.MessageAttachmentRepository
	fileCache                    filecache.FileCache // 仅供 access.go 的附件访问缓存使用
	storage                      objectstorage.ObjectStorage
	idGenerator                  idport.Generator
	txManager                    transaction.Manager
}

const (
	uploadStatusUploading = "uploading"
	uploadStatusCompleted = "completed"
	multipartUploadMode   = "multipart"

	minPartSize     int64 = 5 << 20 // 5 MiB
	defaultPartSize int64 = 8 << 20 // 8 MiB
	maxPartSize     int64 = 5 << 30 // 5 GiB，S3 单片上限
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

func (a *FileApplication) maxParts() int {
	maxParts := a.config.Storage.MinIO.MaxMultipartParts
	if maxParts <= 0 || maxParts > 10000 {
		return 10000
	}
	return maxParts
}

func (a *FileApplication) PartLimit(_ context.Context) *PartLimitDTO {
	return &PartLimitDTO{
		MinPartSize:     minPartSize,
		MaxPartSize:     maxPartSize,
		DefaultPartSize: defaultPartSize,
		MaxPartCount:    a.maxParts(),
	}
}

func (a *FileApplication) PartSize(_ context.Context, size int64) (*PartSizeDTO, error) {
	if size <= 0 {
		return nil, ErrInvalidPart
	}

	if size > a.config.Storage.MinIO.MaxFileSizeBytes {
		return nil, ErrFileTooLarge
	}

	maxParts := a.maxParts()
	partSize := defaultPartSize

	if need := (size + int64(maxParts) - 1) / int64(maxParts); need > partSize {
		partSize = need
	}

	// 向上取整到 MiB，避免非整数分片
	if partSize%(1<<20) != 0 {
		partSize = (partSize/(1<<20) + 1) << 20
	}

	if partSize < minPartSize {
		partSize = minPartSize
	}

	if partSize > maxPartSize {
		partSize = maxPartSize
	}

	total := (size + partSize - 1) / partSize

	if total < 1 {
		total = 1
	}

	if total > int64(maxParts) {
		return nil, ErrTooManyParts
	}

	return &PartSizeDTO{PartSize: partSize, TotalChunks: total}, nil
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

// 普通用户的对象名必须以自己的 userID 为前缀（user123/xxx.jpg），防止越权往他人命名空间写对象
func checkObjectName(uploaderID, name string) error {
	uploaderID = strings.TrimSpace(uploaderID)
	name = strings.TrimSpace(name)
	if uploaderID == "" || name == "" {
		return ErrInvalidObjectName
	}

	if strings.HasPrefix(name, "..") || strings.Contains(name, "\\") || strings.HasPrefix(name, "/") {
		return ErrInvalidObjectName
	}

	if !strings.HasPrefix(name, uploaderID+"/") {
		return ErrInvalidObjectName
	}

	return nil
}

func (a *FileApplication) InitiateMultipartUpload(ctx context.Context, dto InitiateMultipartUploadReqDTO) (*InitiateMultipartUploadResDTO, error) {
	if dto.UploaderID == "" || dto.FileHash == "" || dto.Size <= 0 {
		return nil, ErrInvalidPart
	}

	if err := checkObjectName(dto.UploaderID, dto.Name); err != nil {
		return nil, err
	}

	if dto.Size > a.config.Storage.MinIO.MaxFileSizeBytes {
		return nil, ErrFileTooLarge
	}

	if err := a.validateContentType(dto.ContentType); err != nil {
		return nil, ErrInvalidMIME
	}

	fileHash := strings.ToLower(strings.TrimSpace(dto.FileHash))

	// 直接查询数据库，使用数据库唯一性索引去重
	file, err := a.fileRepository.FindFileByHash(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	if file != nil {
		return &InitiateMultipartUploadResDTO{
			Status: uploadStatusCompleted,
			FileID: file.FileID,
			Name:   dto.Name,
		}, nil
	}

	spec, err := a.PartSize(ctx, dto.Size)
	if err != nil {
		return nil, err
	}

	partSize := spec.PartSize
	if dto.PartSize > 0 && dto.PartSize != partSize {
		return nil, ErrInvalidPart
	}

	fileID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}
	uploadID, err := a.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	objectKey, err := a.buildObjectKey(dto.UploaderID, fileID)
	if err != nil {
		return nil, err
	}

	storageUploadID, err := a.storage.CreateMultipartUpload(ctx, objectKey, dto.ContentType)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	expiresAt := now.Add(time.Duration(a.config.Storage.MinIO.MultipartTTL) * time.Second).UnixMilli()

	record := domainfile.Upload{
		UploadID: uploadID,
		// TODO: 可以暂时不分配 fileID
		FileID:          fileID,
		UploaderID:      dto.UploaderID,
		UploadMode:      multipartUploadMode,
		StorageUploadID: storageUploadID,
		FileHash:        fileHash,
		ObjectKey:       objectKey,
		FileName:        firstNonEmpty(dto.FileName, dto.Name),
		ContentType:     dto.ContentType,
		ExpectedSize:    dto.Size,
		ChunkSize:       partSize,
		TotalChunks:     int(spec.TotalChunks),
		Status:          uploadStatusUploading,
		ExpiresAt:       expiresAt,
		CreatedAt:       now.UnixMilli(),
		UpdatedAt:       now.UnixMilli(),
	}

	if err := a.fileUploadRepository.CreateFileUpload(ctx, record); err != nil {
		_ = a.storage.AbortMultipartUpload(ctx, objectKey, storageUploadID)
		if isDuplicateKey(err) {
			// 唯一键冲突 = 并发已有会话，查回复用
			return a.resumeExistingUpload(ctx, dto.UploaderID, fileHash)
		}
		return nil, err
	}

	// 首次上传，UploadedParts 为空
	return &InitiateMultipartUploadResDTO{
		Status:        uploadStatusUploading,
		UploadID:      uploadID,
		Name:          dto.Name,
		PartSize:      partSize,
		TotalChunks:   int(spec.TotalChunks),
		UploadedParts: []int{},
	}, nil
}

// 并发冲突或续传时，从 uploads 表查回会话
func (a *FileApplication) resumeExistingUpload(ctx context.Context, uploaderID, fileHash string) (*InitiateMultipartUploadResDTO, error) {
	record, err := a.fileUploadRepository.FindActiveFileUploadByUploaderAndHash(ctx, uploaderID, fileHash, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	if record == nil || record.UploadMode != multipartUploadMode || record.StorageUploadID == "" {
		return nil, ErrUploadBusy
	}

	parts, err := a.uploadedPartNumbers(ctx, record.ObjectKey, record.StorageUploadID)
	if err != nil {
		return nil, err
	}

	return &InitiateMultipartUploadResDTO{
		Status:        uploadStatusUploading,
		UploadID:      record.UploadID,
		FileID:        record.FileID,
		PartSize:      record.ChunkSize,
		TotalChunks:   record.TotalChunks,
		UploadedParts: parts,
	}, nil
}

// 为指定分片签发 PUT URL，URL 过期可按需重签
func (a *FileApplication) AuthSign(ctx context.Context, uploadID, uploaderID string, partNumbers []int) ([]AuthSignPartDTO, error) {
	record, err := a.loadActiveUpload(ctx, uploadID, uploaderID)
	if err != nil {
		return nil, err
	}

	if len(partNumbers) == 0 || len(partNumbers) > a.maxParts() {
		return nil, ErrInvalidPart
	}

	// 获取已经上传过的分片
	uploaded, err := a.uploadedPartNumbers(ctx, record.ObjectKey, record.StorageUploadID)
	if err != nil {
		return nil, err
	}

	uploadedSet := make(map[int]struct{}, len(uploaded))
	for _, p := range uploaded {
		uploadedSet[p] = struct{}{}
	}

	ttl := time.Duration(a.config.Storage.MinIO.PartURLTTLSeconds) * time.Second
	seen := make(map[int]struct{}, len(partNumbers))
	result := make([]AuthSignPartDTO, 0, len(partNumbers))

	for _, partNumber := range partNumbers {
		if partNumber <= 0 || partNumber > record.TotalChunks {
			return nil, ErrInvalidPart
		}

		if _, dup := seen[partNumber]; dup {
			continue
		}

		seen[partNumber] = struct{}{}
		if _, done := uploadedSet[partNumber]; done {
			continue
		}

		// TODO: 这里最好改为批量预签名咯
		url, err := a.storage.PresignMultipartPart(ctx, record.ObjectKey, record.StorageUploadID, partNumber, ttl)
		if err != nil {
			return nil, err
		}
		result = append(result,
			AuthSignPartDTO{
				UploadID:   uploadID,
				PartNumber: partNumber,
				URL:        url,
			})
	}
	return result, nil
}

func (a *FileApplication) loadActiveUpload(ctx context.Context, uploadID, uploaderID string) (*domainfile.Upload, error) {
	if uploadID == "" || uploaderID == "" {
		return nil, ErrInvalidUpload
	}

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

	if record.Status != uploadStatusUploading || record.ExpiresAt <= time.Now().UnixMilli() {
		return nil, ErrInvalidUpload
	}

	return record, nil
}

func (a *FileApplication) buildObjectKey(uploaderID, fileID string) (string, error) {
	day := time.Now().Format("20060102")
	uploaderID = strings.TrimSpace(uploaderID)
	if uploaderID == "" {
		return "", ErrUploadUnauthorized
	}
	// {provider}/direct/{date}/{userID}/{uuid}
	return "minio/direct/" + day + "/" + uploaderID + "/" + fileID, nil
}

func (a *FileApplication) uploadedPartNumbers(ctx context.Context, objectKey, storageUploadID string) ([]int, error) {
	parts, err := a.storage.ListMultipartParts(ctx, objectKey, storageUploadID)
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

func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "1062")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}
