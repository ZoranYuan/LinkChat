package friendrequest

type FriendRequestRequest struct {
	PeerUserID string `json:"peerUserId" binding:"required"`
	Message    string `json:"message"` // 可选留言
}

type ActionType int

const (
	ActionAccept ActionType = 1
	ActionReject ActionType = 2
)

type FriendRequestResponse struct {
	RequestID         string `json:"requestId"`
	ApplicantUserID   string `json:"applicantUserId"`
	ApplicantNickName string `json:"applicantNickName"`
	Message           string `json:"message"`
	Status            int    `json:"status"`
	ApplyTime         int64  `json:"applyTime"`
}

type OperateRequestRequest struct {
	RequestID string     `json:"requestId" binding:"required"`
	Action    ActionType `json:"action" binding:"required"`
}
