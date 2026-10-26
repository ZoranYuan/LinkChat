package domainfriend

type RequestStatus int

const (
	Pending RequestStatus = iota + 1
	Accepted
	Refused
)

type Status int

var (
	StatusFriend Status = 0 // 好友
)
