package file

import (
	keycache "IM_backend/internal/infra/cache/key"
)

func FileMetadataKey(fileID string) string {
	return keycache.FileMeta(fileID)
}

func FileByUploaderAndHashKey(uploaderID string, fileHash string) string {
	return "im:file:by-hash:" + uploaderID + ":" + fileHash
}

func UploadMetaKey(uploadID string) string {
	return "im:file:upload:" + uploadID + ":meta"
}

func FileInitLockKey(uploaderID string, fileHash string) string {
	return "im:file:init:" + uploaderID + ":" + fileHash
}

func FileCompleteLockKey(uploadID string) string {
	return "im:file:complete:" + uploadID
}

func ActiveFileUploadKey(uploaderID string, fileHash string) string {
	return "im:file:upload:" + uploaderID + ":" + fileHash
}

func AttachmentFileCardKey(attachmentID string) string {
	return keycache.Build("attachment", attachmentID, "file")
}

func AttachmentURLKey(attachmentID string) string {
	return keycache.Build("attachment", attachmentID, "url")
}
