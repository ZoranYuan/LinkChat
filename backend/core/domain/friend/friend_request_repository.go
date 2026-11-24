package domainfriend

type FriendRequestRepository interface {
	FindLatestFriendRequestByApplicantUserIDAndPeerUserID(applicantUserID string, peerUserID string) (*FriendRequest, error)
	Create(domain *FriendRequest) (*FriendRequest, error)
	OperateRequest(requestID string, expectStatus, newStatus int) error
	ReRequest(domain *FriendRequest) error
	ListFriendRequestsByPeerUserID(peerUserID string) ([]*FriendRequest, error)
	FindFriendRequestByRequestID(requestID string) (*FriendRequest, error)
	WithTx(tx any) FriendRequestRepository
}
