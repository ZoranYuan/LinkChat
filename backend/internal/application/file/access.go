package file

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	filecache "IM_backend/internal/application/ports/cache/file"
	domainfile "IM_backend/internal/domain/file"
	domainmessage "IM_backend/internal/domain/message"
)

func (a *FileApplication) GetAttachmentAccessURLs(ctx context.Context, userID string, attachmentIDs []string) ([]*AttachmentAccessURLDTO, error) {
	if userID == "" {
		return nil, ErrUploadUnauthorized
	}
	if a.messageAttachmentsRepository == nil {
		return nil, ErrUploadUnauthorized
	}

	ids := uniqueAttachmentIDs(attachmentIDs)
	if len(ids) == 0 {
		return []*AttachmentAccessURLDTO{}, nil
	}

	// 必须先根据当前用户校验附件权限，再读取公共附件访问缓存。
	attachments, err := a.messageAttachmentsRepository.ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx, userID, ids)
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
	missedCards := make([]*domainmessage.MessageAttachment, 0, len(ids))
	for _, attachmentID := range ids {
		attachment := attachments[attachmentID]
		if attachment == nil {
			// 未通过权限校验的附件不返回任何信息。
			continue
		}
		if card := cards[attachmentID]; card != nil && card.ObjectKey != "" {
			if accessURL := accessURLs[attachmentID]; accessURL != nil {
				resolved[attachmentID] = attachmentAccessDTOFromCache(card, accessURL)
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
			if attachment.FileID == "" {
				continue
			}
			if _, ok := seenFileIDs[attachment.FileID]; ok {
				continue
			}
			seenFileIDs[attachment.FileID] = struct{}{}
			fileIDs = append(fileIDs, attachment.FileID)
		}

		if len(fileIDs) > 0 {
			if a.fileRepository == nil {
				return nil, ErrFileNotFound
			}
			files, err := a.fileRepository.ListFilesByFileIDs(ctx, fileIDs)
			if err != nil {
				return nil, err
			}

			cardValues := make([]*filecache.AttachmentFileCard, 0, len(missedCards))
			for _, attachment := range missedCards {
				file := files[attachment.FileID]
				if file == nil || file.Status != domainfile.FileStatusUploaded {
					continue
				}
				card := &filecache.AttachmentFileCard{
					AttachmentID: attachment.AttachmentID,
					FileID:       file.FileID,
					ObjectKey:    file.ObjectKey,
					FileName:     file.FileName,
					ContentType:  file.ContentType,
					Size:         file.Size,
					MessageType:  int(attachment.Kind),
				}
				cards[attachment.AttachmentID] = card
				cardValues = append(cardValues, card)
			}

			if a.fileCache != nil {
				if err := a.fileCache.SetAttachmentFileCardBatch(ctx, cardValues, time.Duration(a.config.Cache.AttachmentFileCard.TTLSeconds)*time.Second); err != nil {
					log.Printf("回填附件文件卡片缓存失败：错误=%v", err)
				}
			}
		}
	}

	// 为缓存命中或刚刚回源的卡片补齐缺失的短期 URL。
	urlValues := make([]*filecache.AttachmentURL, 0)
	for _, attachmentID := range ids {
		attachment := attachments[attachmentID]
		card := cards[attachmentID]
		if attachment == nil || card == nil || card.ObjectKey == "" {
			continue
		}
		if accessURL := accessURLs[attachmentID]; accessURL != nil {
			resolved[attachmentID] = attachmentAccessDTOFromCache(card, accessURL)
			continue
		}

		ttl := time.Duration(a.config.Storage.MinIO.URLTTLSeconds) * time.Second
		mediaURL, err := a.storage.PresignedGetURL(ctx, card.ObjectKey, ttl, card.ContentType, card.FileName)
		if err != nil {
			return nil, err
		}
		accessURL := &filecache.AttachmentURL{
			AttachmentID: attachmentID,
			MediaURL:     mediaURL,
			ExpiresAt:    time.Now().Add(ttl).Unix(),
		}
		resolved[attachmentID] = attachmentAccessDTOFromCache(card, accessURL)
		urlValues = append(urlValues, accessURL)
	}
	if a.fileCache != nil && len(urlValues) > 0 {
		if err := a.fileCache.SetAttachmentURLBatch(ctx, urlValues, time.Duration(a.config.Cache.AttachmentAccess.TTLSeconds)*time.Second); err != nil {
			log.Printf("回填附件访问地址缓存失败：错误=%v", err)
		}
	}

	result := make([]*AttachmentAccessURLDTO, 0, len(resolved))
	for _, attachmentID := range ids {
		if value := resolved[attachmentID]; value != nil {
			result = append(result, value)
		}
	}
	return result, nil
}

func (a *FileApplication) GetAttachmentAccessURL(ctx context.Context, userID string, attachmentID string) (*AttachmentAccessURLDTO, error) {
	if attachmentID == "" {
		return nil, ErrUploadUnauthorized
	}
	values, err := a.GetAttachmentAccessURLs(ctx, userID, []string{attachmentID})
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
		MessageType:  card.MessageType,
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

func (a *FileApplication) GetFileForUser(ctx context.Context, fileID, userID string) (*FileDTO, error) {
	var (
		file *domainfile.File
		err  error
	)

	file, err = a.fileCache.GetFileMetadata(ctx, fileID)
	if err != nil {
		return nil, err
	}

	if file == nil {
		file, err = a.fileRepository.FindFileByID(ctx, fileID)
		if err != nil {
			return nil, err
		}
		if file == nil {
			return nil, ErrFileNotFound
		}
	}
	if file.Status != domainfile.FileStatusUploaded {
		return nil, fmt.Errorf("文件当前不可引用")
	}

	if file.UploaderID != userID {
		return nil, ErrUploadUnauthorized
	}

	_ = a.fileCache.SetFileMetadata(ctx, file, time.Duration(a.config.Storage.MinIO.CacheTTLSeconds)*time.Second)

	return toFileDTO(file), nil
}
