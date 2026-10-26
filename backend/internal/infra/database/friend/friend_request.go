package infrafriend

import (
	"errors"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	domainfriend "IM_backend/internal/domain/friend"
)

type FriendRequestRepo struct {
	db *gorm.DB
}

func NewFriendRequestRepository(db *gorm.DB) *FriendRequestRepo {
	return &FriendRequestRepo{
		db: db,
	}
}

func (r *FriendRequestRepo) WithTx(tx any) domainfriend.FriendRequestRepository {
	return &FriendRequestRepo{db: tx.(*gorm.DB)}
}

func (fr *FriendRequestRepo) FindLatestFriendRequestByApplicantUserIDAndPeerUserID(applicantUserID, peerUserID string) (*domainfriend.FriendRequest, error) {
	var friendRequestModel FriendRequestModel
	err := fr.db.
		Where("applicant_user_id = ? AND peer_user_id = ?", applicantUserID, peerUserID).
		Order("created_at DESC").
		First(&friendRequestModel).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return requestToDomain(friendRequestModel), nil
}

func (fr *FriendRequestRepo) Create(domain *domainfriend.FriendRequest) (*domainfriend.FriendRequest, error) {
	m := requestToModel(domain)

	if err := fr.db.Create(&m).Error; err != nil {
		if isDuplicateKey(err) {
			return nil, domainfriend.ErrRequestSentTooFrequently
		}
		return nil, err
	}

	return requestToDomain(m), nil
}

func (fr *FriendRequestRepo) ReRequest(domain *domainfriend.FriendRequest) error {
	m := requestToModel(domain)

	result := fr.db.Model(&FriendRequestModel{}).
		Where(
			"applicant_user_id = ? AND peer_user_id = ?",
			m.ApplicantUserID,
			m.PeerUserID,
		).
		Updates(map[string]interface{}{
			"request_id": m.RequestID,
			"status":     m.Status,
			"message":    m.Message,
			"apply_time": m.ApplyTime,
		})

	if result.Error != nil {
		if isDuplicateKey(result.Error) {
			return domainfriend.ErrRequestSentTooFrequently
		}
		return result.Error
	}

	if result.RowsAffected == 0 {
		return domainfriend.ErrInvalidStatusTransition
	}

	return nil
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func (fr *FriendRequestRepo) OperateRequest(requestID string, expectStatus, newStatus int) error {
	result := fr.db.Model(&FriendRequestModel{}).
		Where(
			"request_id = ? AND status = ?",
			requestID,
			expectStatus,
		).
		Updates(map[string]interface{}{
			"status": newStatus,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return domainfriend.ErrInvalidStatusTransition
	}

	return nil
}

func (fr *FriendRequestRepo) ListFriendRequestsByPeerUserID(peerUserID string) ([]*domainfriend.FriendRequest, error) {
	var m []FriendRequestModel
	if err := fr.db.Where("peer_user_id = ?", peerUserID).
		Order("updated_at DESC").
		Find(&m).Error; err != nil {
		return nil, err
	}

	domainList := make([]*domainfriend.FriendRequest, 0, len(m))

	for _, m := range m {
		domainList = append(domainList, requestToDomain(m))
	}

	return domainList, nil
}

func (fr *FriendRequestRepo) FindFriendRequestByRequestID(requestID string) (*domainfriend.FriendRequest, error) {
	var m FriendRequestModel

	if err := fr.db.Where("request_id = ?", requestID).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainfriend.ErrFriendRequestNotFound
		}
		return nil, err
	}

	return requestToDomain(m), nil
}
