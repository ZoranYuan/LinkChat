package domainmessage

type MessageSticker struct {
	MessageID string `json:"MessageId"`
	StickerID string `json:"StickerId"`
	PackID    string `json:"PackId"`
	Width     int
	Height    int
}

func NewMessageSticker(messageID, stickerID, packID string, width, height int) *MessageSticker {
	return &MessageSticker{
		MessageID: messageID,
		StickerID: stickerID,
		PackID:    packID,
		Width:     width,
		Height:    height,
	}
}
