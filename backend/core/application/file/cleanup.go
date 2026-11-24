package file

import (
	"context"
	"log"
	"time"

	"IM_backend/configs"
	"IM_backend/core/application/ports/objectstorage"
	domainfile "IM_backend/core/domain/file"
)

type DeleteOutdatedData struct {
	fileRepo         domainfile.FileRepository
	storage          objectstorage.ObjectStorage
	interval         time.Duration
	batchSize        int
	orphanAfter      time.Duration
	operationTimeout time.Duration
}

func NewDeleteOutdatedData(
	fileRepo domainfile.FileRepository,
	storage objectstorage.ObjectStorage,
	config configs.FileCleanupConfig,
) *DeleteOutdatedData {
	return &DeleteOutdatedData{
		fileRepo:         fileRepo,
		storage:          storage,
		interval:         time.Duration(config.IntervalSeconds) * time.Second,
		batchSize:        config.BatchSize,
		orphanAfter:      time.Duration(config.OrphanAfterSeconds) * time.Second,
		operationTimeout: time.Duration(config.OperationTimeoutSeconds) * time.Second,
	}
}

func (w *DeleteOutdatedData) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.deleteOutdatedOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("文件清理任务执行失败：%v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *DeleteOutdatedData) deleteOutdatedOnce(ctx context.Context) error {
	now := time.Now().UnixMilli()
	before := now - w.orphanAfter.Milliseconds()

	files, err := w.fileRepo.ListFilesEligibleForCleanup(ctx, before, now, w.batchSize)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file == nil {
			continue
		}
		w.cleanupFile(ctx, file, before, now)
	}
	return nil
}

func (w *DeleteOutdatedData) cleanupFile(ctx context.Context, file *domainfile.File, before, now int64) {
	if file.Status == domainfile.FileStatusUploaded {
		// 乐观锁更新
		marked, err := w.fileRepo.MarkFileAsDeletingIfEligible(ctx, file.FileID, before, now)
		if err != nil || !marked {
			return
		}
	}

	// 这里需要先删除minio中文件、再删除缓存，最后删除记录，避免出现记录删除成功，缓存/minio 文件删除失败，导致记录无法再被执行的问题
	cleanupCtx, cancel := context.WithTimeout(ctx, w.operationTimeout)

	// S3 DELETE 语义是幂等的，这里无需加锁，重复的 deleting ，但是结果幂等
	err := w.storage.DeleteObject(cleanupCtx, file.ObjectKey)
	cancel()
	if err != nil {
		log.Printf("删除过期文件对象失败：文件=%s 对象=%s 错误=%v（保留状态待重试）", file.FileID, file.ObjectKey, err)
		return
	}

	deleted, err := w.fileRepo.DeleteFileIfMarkedDeleting(ctx, file.FileID)
	if err != nil || !deleted {
		log.Printf("删除过期文件记录失败：文件=%s 错误=%v（下次扫描会重试，删对象幂等）", file.FileID, err)
	}
}
