package message

import (
	keycache "IM_backend/internal/infra/cache/key"
)

func MessageDedupKey(senderID, clientMsgID string) string {
	return keycache.MessageDedup(senderID, clientMsgID)
}
