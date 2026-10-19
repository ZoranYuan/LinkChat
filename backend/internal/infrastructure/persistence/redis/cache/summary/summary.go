package summary

import (
	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	rediscache "IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var _ summarycache.RoomUnreadSnapshotCache = (*RoomUnreadSnapshotCache)(nil)
var _ summarycache.SummaryScopeRunStore = (*RoomUnreadSnapshotCache)(nil)

type RoomUnreadSnapshotCache struct {
	store *rediscache.Store
}

func NewRoomUnreadSnapshotCache(client *redis.Client) *RoomUnreadSnapshotCache {
	return &RoomUnreadSnapshotCache{store: rediscache.NewStore(client)}
}

func (c *RoomUnreadSnapshotCache) SetActive(
	ctx context.Context,
	userID, roomID string,
	snapshot summarycache.RoomUnreadSnapshot,
	ttl time.Duration,
) error {
	if userID == "" || roomID == "" || snapshot.RoomID == "" {
		return errors.New("摘要未读快照参数不能为空")
	}
	if snapshot.RoomID != roomID {
		return errors.New("摘要未读快照房间不匹配")
	}
	if !validUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
		return errors.New("摘要未读快照序号范围无效")
	}
	if ttl <= 0 {
		return errors.New("摘要未读快照 TTL 必须大于 0")
	}
	return c.store.SetJSON(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID), snapshot, ttl)
}

func (c *RoomUnreadSnapshotCache) GetActive(
	ctx context.Context,
	userID, roomID string,
) (*summarycache.RoomUnreadSnapshot, bool, error) {
	if userID == "" || roomID == "" {
		return nil, false, errors.New("用户和房间标识不能为空")
	}

	var snapshot summarycache.RoomUnreadSnapshot
	found, err := c.store.GetJSON(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID), &snapshot)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if snapshot.RoomID != roomID || !validUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
		if err := c.store.Del(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID)); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	return &snapshot, true, nil
}

func validUnreadRange(begin, end int64) bool {
	return (begin == 0 && end == 0) || (begin > 0 && end >= begin)
}

func (c *RoomUnreadSnapshotCache) DeleteActive(ctx context.Context, userID, roomID string) error {
	if userID == "" || roomID == "" {
		return errors.New("用户和房间标识不能为空")
	}
	return c.store.Del(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID))
}

type summaryScopeLease struct {
	SummaryRunID string `json:"summaryRunId"`
	LockToken    string `json:"lockToken"`
}

func encodeSummaryScopeLease(runID, lockToken string) (string, error) {
	data, err := json.Marshal(summaryScopeLease{SummaryRunID: runID, LockToken: lockToken})
	if err != nil {
		return "", fmt.Errorf("编码摘要 scope 租约失败：%w", err)
	}
	return string(data), nil
}

func decodeSummaryScopeLease(value string) (summaryScopeLease, error) {
	var lease summaryScopeLease
	if err := json.Unmarshal([]byte(value), &lease); err != nil {
		return lease, fmt.Errorf("解析摘要 scope 租约失败：%w", err)
	}
	if lease.SummaryRunID == "" || lease.LockToken == "" {
		return lease, errors.New("摘要 scope 租约内容无效")
	}
	return lease, nil
}

func (c *RoomUnreadSnapshotCache) GetSummaryRunIDByScope(
	ctx context.Context,
	userID, roomID string,
) (string, bool, error) {
	if userID == "" || roomID == "" {
		return "", false, errors.New("摘要 scope 参数不能为空")
	}
	value, err := c.store.GetString(ctx, SummaryScopeRunKey(userID, roomID))
	if err != nil {
		return "", false, err
	}
	if value == "" {
		return "", false, nil
	}
	lease, err := decodeSummaryScopeLease(value)
	if err != nil {
		return "", false, err
	}
	return lease.SummaryRunID, true, nil
}

func (c *RoomUnreadSnapshotCache) ClaimSummaryRunIDByScope(
	ctx context.Context,
	userID, roomID, runID string,
	ttl time.Duration,
) (string, string, bool, error) {
	if userID == "" || roomID == "" || runID == "" || ttl <= 0 {
		return "", "", false, errors.New("摘要 scope 绑定参数无效")
	}

	lockToken := uuid.NewString()
	leaseValue, err := encodeSummaryScopeLease(runID, lockToken)
	if err != nil {
		return "", "", false, err
	}

	key := SummaryScopeRunKey(userID, roomID)
	acquired, err := c.store.SetNXString(ctx, key, leaseValue, ttl)
	if err != nil {
		return "", "", false, err
	}
	if acquired {
		return runID, lockToken, true, nil
	}

	existingValue, err := c.store.GetString(ctx, key)
	if err != nil {
		return "", "", false, err
	}
	if existingValue == "" {
		return "", "", false, nil
	}
	existingLease, err := decodeSummaryScopeLease(existingValue)
	if err != nil {
		return "", "", false, err
	}
	return existingLease.SummaryRunID, "", false, nil
}

func (c *RoomUnreadSnapshotCache) RefreshSummaryRunIDByScope(
	ctx context.Context,
	userID, roomID, runID, lockToken string,
	ttl time.Duration,
) (bool, error) {
	if userID == "" || roomID == "" || runID == "" || lockToken == "" || ttl <= 0 {
		return false, errors.New("摘要 scope 绑定参数无效")
	}
	leaseValue, err := encodeSummaryScopeLease(runID, lockToken)
	if err != nil {
		return false, err
	}
	const script = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0`
	result, err := c.store.Eval(
		ctx,
		script,
		[]string{SummaryScopeRunKey(userID, roomID)},
		leaseValue,
		ttl.Milliseconds(),
	)
	if err != nil {
		return false, fmt.Errorf("续期摘要 scope 绑定失败：%w", err)
	}
	value, ok := result.(int64)
	return ok && value == 1, nil
}

func (c *RoomUnreadSnapshotCache) ReleaseSummaryRunIDByScope(
	ctx context.Context,
	userID, roomID, runID, lockToken string,
) error {
	if userID == "" || roomID == "" || runID == "" || lockToken == "" {
		return errors.New("摘要 scope 绑定参数无效")
	}
	leaseValue, err := encodeSummaryScopeLease(runID, lockToken)
	if err != nil {
		return err
	}
	const script = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0`
	if _, err := c.store.Eval(ctx, script, []string{SummaryScopeRunKey(userID, roomID)}, leaseValue); err != nil {
		return fmt.Errorf("释放摘要 scope 绑定失败：%w", err)
	}
	return nil
}
