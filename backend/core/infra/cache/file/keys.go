package file

import (
	keycache "IM_backend/core/infra/cache/key"
)

func AttachmentURLKey(attachmentID string) string {
	return keycache.Build("attachment", attachmentID, "url")
}
