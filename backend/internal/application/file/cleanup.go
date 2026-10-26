package file

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"IM_backend/configs"
	filecache "IM_backend/internal/application/ports/cache/file"
	"IM_backend/internal/application/ports/objectstorage"
	"IM_backend/internal/application/ports/transaction"
	domainfile "IM_backend/internal/domain/file"
)

type CleanupWorker struct {
	txManager          transaction.Manager
	fileUploadRepo     domainfile.FileUploadRepository
	fileRepo           domainfile.FileRepository
	fileCache          filecache.FileCache
	storage            objectstorage.ObjectStorage
	interval           time.Duration
	batchSize          int
	staleAfter         time.Duration
	baseRetryWait      time.Duration
	operationTimeout   time.Duration
	completeLockTTL    time.Duration
	maxRetries         int
	orphanAfter        time.Duration
	maxRetryWait       time.Duration
	lockReleaseTimeout time.Duration
}

func NewCleanupWorker(
	txManager transaction.Manager,
	fileUploadRepo domainfile.FileUploadRepository,
	fileRepo domainfile.FileRepository,
	fileCache filecache.FileCache,
	storage objectstorage.ObjectStorage,
	config configs.FileCleanupConfig,
	minioConfig configs.MinIOConfig,
) *CleanupWorker {
	operationTimeout := time.Duration(config.OperationTimeoutSeconds) * time.Second

	return &CleanupWorker{
		txManager:          txManager,
		fileUploadRepo:     fileUploadRepo,
		fileRepo:           fileRepo,
		fileCache:          fileCache,
		storage:            storage,
		interval:           time.Duration(config.IntervalSeconds) * time.Second,
		batchSize:          config.BatchSize,
		staleAfter:         time.Duration(config.StaleAfterSeconds) * time.Second,
		baseRetryWait:      time.Duration(config.BaseRetryWaitSeconds) * time.Second,
		operationTimeout:   operationTimeout,
		completeLockTTL:    time.Duration(config.CompleteLockTTLSeconds) * time.Second,
		maxRetryWait:       time.Duration(config.MaxRetryWaitSeconds) * time.Second,
		lockReleaseTimeout: time.Duration(minioConfig.LockReleaseTimeoutSeconds) * time.Second,
		maxRetries:         config.MaxRetries,
		orphanAfter:        time.Duration(config.OrphanAfterSeconds) * time.Second,
	}
}

func (w *CleanupWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.dispatchPendingOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("文件上传清理任务执行失败：%v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *CleanupWorker) dispatchPendingOnce(ctx context.Context) error {
	if w.txManager == nil || w.fileUploadRepo == nil || w.fileCache == nil || w.storage == nil {
		return errors.New("文件上传清理任务依赖未配置")
	}

	now := time.Now().UnixMilli()
	staleBefore := now - w.staleAfter.Milliseconds()
	var uploads []domainfile.Upload
	if err := w.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		uploads, err = w.fileUploadRepo.WithTx(tx).ClaimExpiredFileUploads(ctx, now, staleBefore, w.batchSize)
		return err
	}); err != nil {
		return err
	}

	for _, upload := range uploads {
		if err := w.cleanupOne(ctx, upload); err != nil {
			w.recordFailure(ctx, upload, err)
		}
	}
	if err := w.cleanupOrphanFiles(ctx, now-w.orphanAfter.Milliseconds(), now); err != nil && ctx.Err() == nil {
		log.Printf("未引用文件清理任务执行失败：%v", err)
	}
	return nil
}

func (w *CleanupWorker) cleanupOrphanFiles(ctx context.Context, before int64, now int64) error {
	if w.fileRepo == nil {
		return nil
	}
	files, err := w.fileRepo.ListFilesEligibleForCleanup(ctx, before, now, w.batchSize)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file == nil {
			continue
		}
		if file.Status == domainfile.FileStatusUploaded {
			marked, err := w.fileRepo.MarkFileAsDeletingIfEligible(ctx, file.FileID, before, now)
			if err != nil || !marked {
				continue
			}
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, w.operationTimeout)
		err = w.storage.DeleteObject(cleanupCtx, file.ObjectKey)
		cancel()
		if err != nil {
			log.Printf("删除未引用文件对象失败：文件=%s 错误=%v", file.FileID, err)
			continue
		}
		deleted, err := w.fileRepo.DeleteFileIfMarkedDeleting(ctx, file.FileID)
		if err != nil || !deleted {
			log.Printf("删除未引用文件记录失败：文件=%s 错误=%v", file.FileID, err)
			continue
		}
		if err := w.fileCache.DeleteFileMetadata(ctx, file.FileID); err != nil {
			log.Printf("删除未引用文件缓存失败：文件=%s 错误=%v", file.FileID, err)
		}
		if err := w.fileCache.DeleteFileByUploaderAndHash(ctx, file.UploaderID, file.FileHash); err != nil {
			log.Printf("删除文件哈希缓存失败：文件=%s 错误=%v", file.FileID, err)
		}
	}
	return nil
}

func (w *CleanupWorker) cleanupOne(ctx context.Context, upload domainfile.Upload) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, w.operationTimeout)
	defer cancel()

	completeLockToken, locked, err := w.fileCache.AcquireFileCompleteLock(
		cleanupCtx,
		upload.UploadID,
		w.completeLockTTL,
	)
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("上传完成操作正在进行")
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), w.lockReleaseTimeout)
		defer releaseCancel()
		_ = w.fileCache.ReleaseFileCompleteLock(releaseCtx, upload.UploadID, completeLockToken)
	}()

	if upload.StorageUploadID != "" {
		if err := w.storage.AbortMultipartUpload(cleanupCtx, upload.ObjectKey, upload.StorageUploadID); err != nil {
			return err
		}
	} else {
		if err := w.storage.DeleteObject(cleanupCtx, upload.ObjectKey); err != nil {
			return err
		}
	}
	if err := w.fileCache.DeleteUploadMeta(cleanupCtx, upload.UploadID); err != nil {
		return err
	}
	if err := w.fileCache.DeleteActiveFileUploadIfMatches(cleanupCtx, upload.UploaderID, upload.FileHash, upload.UploadID); err != nil {
		return err
	}

	marked, err := w.fileUploadRepo.MarkExpired(ctx, upload.UploadID, upload.LockToken, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if !marked {
		return errors.New("文件上传清理任务锁已失效")
	}
	return nil
}

func (w *CleanupWorker) recordFailure(ctx context.Context, upload domainfile.Upload, cleanupErr error) {
	now := time.Now()
	lastError := cleanupErr.Error()

	if upload.RetryCount+1 >= w.maxRetries {
		marked, err := w.fileUploadRepo.MarkCleanupFailed(ctx, upload.UploadID, upload.LockToken, lastError, now.UnixMilli())
		if err != nil || !marked {
			log.Printf("标记文件上传清理失败状态失败：上传=%s 错误=%v 原因=%s", upload.UploadID, err, lastError)
			return
		}
		log.Printf("文件上传清理重试耗尽：上传=%s 原因=%s", upload.UploadID, lastError)
		return
	}

	delay := w.baseRetryWait * time.Duration(math.Pow(2, float64(upload.RetryCount)))
	if delay > w.maxRetryWait {
		delay = w.maxRetryWait
	}
	marked, err := w.fileUploadRepo.MarkCleanupRetry(
		ctx,
		upload.UploadID,
		upload.LockToken,
		now.Add(delay).UnixMilli(),
		lastError,
		now.UnixMilli(),
	)
	if err != nil || !marked {
		log.Printf("记录文件上传清理重试失败：上传=%s 错误=%v 原因=%s", upload.UploadID, err, lastError)
	}
}
