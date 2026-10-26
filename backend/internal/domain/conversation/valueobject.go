package domainconversation

type ConvType int8

const (
	PrivateChat ConvType = iota + 1
	Room
)
