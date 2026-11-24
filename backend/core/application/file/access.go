package file

import (
	filecache "IM_backend/core/application/ports/cache/file"
	domainmessage "IM_backend/core/domain/message"
	"context"
	"log"
	"strings"
	"time"
)

func (a *FileApplication) AccessURLs(ctx context.Context, userID string, attachmentIDs []string) ([]*AttachmentAccessURLDTO, error) {
	if userID == "" {
		return nil, ErrUploadUnauthorized
	}

	ids := uniqueAttachmentIDs(attachmentIDs)
	if len(ids) == 0 {
		return []*AttachmentAccessURLDTO{}, nil
	}

	attachments, err := a.messageAttachmentsRepository.ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx, userID, ids)
	if err != nil {
		return nil, err
	}

	// 只保留访问 URL 缓存：签名有 CPU 成本且 URL 自带有效期，命中即可直接返回。
	accessURLs, err := a.fileCache.GetAttachmentURLBatch(ctx, ids)
	if err != nil {
		log.Printf("读取附件访问地址缓存失败，继续回源：错误=%v", err)
		accessURLs = make(map[string]*filecache.AttachmentURL)
	}

	resolved := make(map[string]*AttachmentAccessURLDTO, len(ids))
	urlValues := make([]*filecache.AttachmentURL, 0, len(ids))
	signTTL := time.Duration(a.config.Storage.MinIO.URLTTLSeconds) * time.Second

	for _, attachmentID := range ids {
		attachment := attachments[attachmentID]
		if attachment == nil {
			// 未通过权限校验或文件已不可引用，不返回任何信息。
			continue
		}

		accessURL := accessURLs[attachmentID]
		if accessURL == nil {
			mediaURL, err := a.storage.PresignedGetURL(ctx, attachment.ObjectKey, signTTL, attachment.ContentType, attachment.FileName)
			if err != nil {
				return nil, err
			}
			accessURL = &filecache.AttachmentURL{
				AttachmentID: attachmentID,
				MediaURL:     mediaURL,
				ExpiresAt:    time.Now().Add(signTTL).Unix(),
			}
			urlValues = append(urlValues, accessURL)
		}

		resolved[attachmentID] = attachmentAccessDTO(attachment, accessURL)
	}

	if err := a.fileCache.SetAttachmentURLBatch(ctx, urlValues, time.Duration(a.config.Cache.AttachmentAccess.TTLSeconds)*time.Second); err != nil {
		log.Printf("回填附件访问地址缓存失败：错误=%v", err)
	}

	result := make([]*AttachmentAccessURLDTO, 0, len(resolved))
	for _, attachmentID := range ids {
		if value := resolved[attachmentID]; value != nil {
			result = append(result, value)
		}
	}
	return result, nil
}

func (a *FileApplication) AccessURL(ctx context.Context, userID string, attachmentID string) (*AttachmentAccessURLDTO, error) {
	if attachmentID == "" {
		return nil, ErrUploadUnauthorized
	}
	values, err := a.AccessURLs(ctx, userID, []string{attachmentID})
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrFileNotFound
	}
	return values[0], nil
}

func attachmentAccessDTO(attachment *domainmessage.AccessibleAttachment, accessURL *filecache.AttachmentURL) *AttachmentAccessURLDTO {
	return &AttachmentAccessURLDTO{
		AttachmentID: attachment.AttachmentID,
		FileID:       attachment.FileID,
		FileName:     attachment.FileName,
		ContentType:  attachment.ContentType,
		Size:         attachment.Size,
		MediaURL:     accessURL.MediaURL,
		ThumbURL:     accessURL.ThumbURL,
		ExpiresAt:    accessURL.ExpiresAt,
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
