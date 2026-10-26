package domainmessage

type MessageType int

var (
	Text    MessageType = 1
	Image   MessageType = 2
	Video   MessageType = 3
	Sticker MessageType = 4
	File    MessageType = 5
)

type Status int

var (
	Normal    Status = 1
	Withdrawn Status = 2
)
