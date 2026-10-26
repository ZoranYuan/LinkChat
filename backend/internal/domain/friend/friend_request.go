package domainfriend

import (
	"time"
)

type FriendRequest struct {
	RequestID       string        `json:"requestId"`
	ApplicantUserID string        `json:"applicantUserId"`
	PeerUserID      string        `json:"peerUserId"`
	Status          RequestStatus `json:"status"`
	Message         string        `json:"message"` // 可选留言
	ApplyTime       int64         `json:"applyTime"`
}

func NewFriendRequest(reqID, applicantUserID, peerUserID, message string) (*FriendRequest, error) {
	// TODO 对 message 做一些额外的处理
	if applicantUserID == peerUserID {
		return nil, ErrSelfRequest
	}

	var newFriendRequest = &FriendRequest{
		ApplicantUserID: applicantUserID,
		RequestID:       reqID,
		PeerUserID:      peerUserID,
		Message:         message,
		Status:          Pending,
		ApplyTime:       time.Now().UnixMilli(),
	}

	return newFriendRequest, nil
}

func (fq *FriendRequest) ReRequest(newRequestID string, message string, cooldown time.Duration) error {
	if fq.ApplicantUserID == fq.PeerUserID {
		return ErrSelfRequest
	}

	// 查询时间是否超过重新请求阈值
	if time.Since(time.UnixMilli(fq.ApplyTime)) < cooldown {
		return ErrRequestSentTooFrequently
	}

	// 上次请求未处理，视为同一个请求
	if fq.Status == Pending {
		fq.Message = message
		fq.ApplyTime = time.Now().UnixMilli()
		return nil
	}

	// 重新请求需要重新去建立新的 request id
	fq.RequestID = newRequestID
	fq.Message = message
	fq.Status = Pending
	fq.ApplyTime = time.Now().UnixMilli()

	return nil
}

func (fq *FriendRequest) Accept(userID string) error {
	if fq.Status != Pending {
		return ErrDuplicateRequestOperation
	}

	if fq.PeerUserID != userID {
		return ErrInvalidRequestOperation
	}

	fq.Status = Accepted

	return nil
}

func (fq *FriendRequest) Refuse(userID string) error {
	var err error
	if fq.Status != Pending {
		return ErrDuplicateRequestOperation
	}

	if fq.PeerUserID != userID {
		return ErrInvalidRequestOperation
	}

	fq.Status = Refused

	return err
}
