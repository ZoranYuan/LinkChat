package room

import (
	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomentity "IM_backend/internal/domain/room/entity"
	"IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"IM_backend/internal/shared/protocol"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	_ roomcache.RoomCache = (*RoomCache)(nil)
)

var RoomProfileTTL = 24 * 60 * 60

const (
	defaultRecentMessageTTL        = 10 * time.Minute
	defaultRecentMessageMaxEntries = 5000
)

type RoomCache struct {
	store  *shared.Store
	config configs.MessageConfig
}

func NewRoomCache(rb *redis.Client, messageConfig configs.MessageConfig) *RoomCache {
	return &RoomCache{
		store:  shared.NewStore(rb),
		config: messageConfig,
	}
}

type activitySettings struct {
	bucketSeconds    int64
	windowBuckets    int
	cleanupBuckets   int
	keyTTLSeconds    int64
	warnFanoutWork   int64
	activeFanoutWork int64
}

func (rc *RoomCache) activitySettings() (activitySettings, error) {
	config := rc.config
	if config.RoomActivityWindowSeconds <= 0 {
		return activitySettings{}, errors.New("活跃度窗口必须大于 0")
	}
	if config.RoomActivityBucketSeconds <= 0 {
		return activitySettings{}, errors.New("活跃度时间桶必须大于 0")
	}
	if config.RoomActivityBucketSeconds > config.RoomActivityWindowSeconds {
		return activitySettings{}, errors.New("活跃度时间桶不能大于统计窗口")
	}
	if config.RoomActivityKeyTTLSeconds <= 0 {
		return activitySettings{}, errors.New("活跃度 key TTL 必须大于 0")
	}
	if config.RoomActivityWarnFanoutWork <= 0 {
		return activitySettings{}, errors.New("活跃度 WARN 扇出工作量阈值必须大于 0")
	}
	if config.RoomActivityActiveFanoutWork <= config.RoomActivityWarnFanoutWork {
		return activitySettings{}, errors.New("活跃度 ACTIVE 扇出工作量阈值必须大于 WARN 阈值")
	}

	windowBuckets := (config.RoomActivityWindowSeconds + config.RoomActivityBucketSeconds - 1) /
		config.RoomActivityBucketSeconds
	cleanupBuckets := config.RoomActivityKeyTTLSeconds/config.RoomActivityBucketSeconds + windowBuckets + 1

	return activitySettings{
		bucketSeconds:    int64(config.RoomActivityBucketSeconds),
		windowBuckets:    windowBuckets,
		cleanupBuckets:   cleanupBuckets,
		keyTTLSeconds:    int64(config.RoomActivityKeyTTLSeconds),
		warnFanoutWork:   int64(config.RoomActivityWarnFanoutWork),
		activeFanoutWork: int64(config.RoomActivityActiveFanoutWork),
	}, nil
}

func (rc *RoomCache) randCode(count int) (string, error) {
	letters := "0123456789"

	maxL := big.NewInt(int64(len(letters)))

	b := make([]byte, count)
	for i := 0; i < count; i++ {
		num, err := rand.Int(rand.Reader, maxL)
		if err != nil {
			return "", err
		}

		b[i] = letters[num.Int64()]
	}

	return string(b), nil
}

// RecordActivity 记录当前时间桶内的房间消息数量。
// 使用一个 Hash 保存多个时间桶，避免为每个时间桶创建独立 Redis key。
func (rc *RoomCache) RecordActivity(ctx context.Context, roomId string) error {
	if roomId == "" {
		return errors.New("roomID 不能为空")
	}
	if rc == nil || rc.store == nil || rc.store.Client() == nil {
		return errors.New("房间活跃度缓存未配置")
	}

	return rc.recordActivityAt(ctx, roomId, time.Now())
}

// RecordActivityBatchAndGetLevel 按同一时间桶批量记录消息，并计算房间活跃等级。
func (rc *RoomCache) RecordActivityBatchAndGetLevel(ctx context.Context, roomID string, messageCount, onlineSessions int, at time.Time) (int, error) {
	if roomID == "" {
		return roomcache.RoomActivityNormal, errors.New("roomID 不能为空")
	}
	if rc == nil || rc.store == nil || rc.store.Client() == nil {
		return roomcache.RoomActivityNormal, errors.New("房间活跃度缓存未配置")
	}
	return rc.recordActivityAndGetLevelAt(ctx, roomID, messageCount, at, onlineSessions)
}

func (rc *RoomCache) recordActivityAndGetLevelAt(ctx context.Context, roomID string, messageCount int, at time.Time, onlineSessions int) (int, error) {
	if messageCount <= 0 {
		return roomcache.RoomActivityNormal, errors.New("消息数量必须大于 0")
	}
	settings, err := rc.activitySettings()
	if err != nil {
		return roomcache.RoomActivityNormal, err
	}
	if onlineSessions < 0 {
		onlineSessions = 0
	}
	currentBucket := at.Unix() / settings.bucketSeconds
	const script = `
		redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[5])
		local current = tonumber(ARGV[1])
		local windowBuckets = tonumber(ARGV[2])
		local cleanupBuckets = tonumber(ARGV[3])
		for offset = windowBuckets, cleanupBuckets do
			redis.call('HDEL', KEYS[1], tostring(current - offset))
		end
		local fields = {}
		for offset = 0, windowBuckets - 1 do
			fields[#fields + 1] = tostring(current - offset)
		end
		local values = redis.call('HMGET', KEYS[1], unpack(fields))
		local total = 0
		for _, value in ipairs(values) do
			if value then total = total + tonumber(value) end
		end
		redis.call('EXPIRE', KEYS[1], ARGV[4])
		return total
	`
	totalMessages, err := rc.store.Client().Eval(
		ctx,
		script,
		[]string{ActivateLevelKey(roomID)},
		currentBucket,
		settings.windowBuckets,
		settings.cleanupBuckets,
		settings.keyTTLSeconds,
		messageCount,
	).Int64()
	if err != nil {
		return roomcache.RoomActivityNormal, err
	}
	return classifyActivityLevel(totalMessages, int64(onlineSessions), settings), nil
}

func (rc *RoomCache) recordActivityAt(ctx context.Context, roomId string, now time.Time) error {
	settings, err := rc.activitySettings()
	if err != nil {
		return err
	}

	bucketSeconds := settings.bucketSeconds
	currentBucket := now.Unix() / bucketSeconds
	const script = `
		redis.call('HINCRBY', KEYS[1], ARGV[1], 1)

		-- 清理已经不可能参与最近窗口统计的旧桶。
		local current = tonumber(ARGV[1])
		local windowBuckets = tonumber(ARGV[2])
		local cleanupBuckets = tonumber(ARGV[3])
		for offset = windowBuckets, cleanupBuckets do
			redis.call('HDEL', KEYS[1], tostring(current - offset))
		end

		redis.call('EXPIRE', KEYS[1], ARGV[4])
		return 1
	`

	_, err = rc.store.Eval(
		ctx,
		script,
		[]string{ActivateLevelKey(roomId)},
		currentBucket,
		settings.windowBuckets,
		settings.cleanupBuckets,
		settings.keyTTLSeconds,
	)
	return err
}

// ActivateLevel 只读取窗口消息数，按当前在线连接数计算等级，不增加计数。
func (rc *RoomCache) ActivateLevel(ctx context.Context, roomId string, onlineSessions int) (int, error) {
	if roomId == "" {
		return roomcache.RoomActivityNormal, errors.New("roomID 不能为空")
	}
	if rc == nil || rc.store == nil || rc.store.Client() == nil {
		return roomcache.RoomActivityNormal, errors.New("房间活跃度缓存未配置")
	}
	return rc.activateLevelAt(ctx, roomId, time.Now(), onlineSessions)
}

func (rc *RoomCache) activateLevelAt(ctx context.Context, roomId string, now time.Time, onlineSessions int) (int, error) {
	settings, err := rc.activitySettings()
	if err != nil {
		return roomcache.RoomActivityNormal, err
	}

	bucketSeconds := settings.bucketSeconds
	windowBuckets := settings.windowBuckets
	currentBucket := now.Unix() / bucketSeconds

	fields := make([]string, 0, windowBuckets)
	for offset := 0; offset < windowBuckets; offset++ {
		fields = append(fields, strconv.FormatInt(currentBucket-int64(offset), 10))
	}

	values, err := rc.store.Client().HMGet(ctx, ActivateLevelKey(roomId), fields...).Result()
	if err != nil {
		return roomcache.RoomActivityNormal, err
	}

	var total int64
	for _, value := range values {
		if value == nil {
			continue
		}
		var text string
		switch item := value.(type) {
		case string:
			text = item
		case []byte:
			text = string(item)
		default:
			return roomcache.RoomActivityNormal, fmt.Errorf("活跃度计数类型无效: %T", value)
		}
		count, parseErr := strconv.Atoi(text)
		if parseErr != nil {
			return roomcache.RoomActivityNormal, fmt.Errorf("解析活跃度计数失败: %w", parseErr)
		}
		total += int64(count)
	}
	return classifyActivityLevel(total, int64(max(onlineSessions, 0)), settings), nil
}

func classifyActivityLevel(totalMessages, onlineSessions int64, settings activitySettings) int {
	fanoutWork := totalMessages * onlineSessions
	switch {
	case fanoutWork >= settings.activeFanoutWork:
		return roomcache.RoomActivityActive
	case fanoutWork >= settings.warnFanoutWork:
		return roomcache.RoomActivityWarn
	default:
		return roomcache.RoomActivityNormal
	}
}

func (rc *RoomCache) AppendRecentMessageSeq(ctx context.Context, roomId string, event protocol.MessageEvent) error {
	return rc.WarmRecentMessageEvents(ctx, roomId, []protocol.MessageEvent{event})
}

// WarmRecentMessageEvents 批量回填近期消息缓存。
func (rc *RoomCache) WarmRecentMessageEvents(ctx context.Context, roomId string, events []protocol.MessageEvent) error {
	if roomId == "" {
		return errors.New("roomID 不能为空")
	}
	if rc == nil || rc.store == nil || rc.store.Client() == nil {
		return errors.New("近期消息缓存未配置")
	}
	if len(events) == 0 {
		return nil
	}

	orderedEvents := append([]protocol.MessageEvent(nil), events...)
	sort.SliceStable(orderedEvents, func(i, j int) bool {
		return orderedEvents[i].Seq < orderedEvents[j].Seq
	})

	args := make([]interface{}, 0, len(orderedEvents)*2+2)
	args = append(args, defaultRecentMessageMaxEntries, defaultRecentMessageTTL.Milliseconds())
	for _, event := range orderedEvents {
		if event.Seq <= 0 {
			return errors.New("消息 seq 必须大于 0")
		}
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("序列化消息失败: %w", err)
		}
		args = append(args, event.Seq, string(data))
	}

	const script = `
		local maxEntries = tonumber(ARGV[1])
		local ttlMilliseconds = tonumber(ARGV[2])

		for index = 3, #ARGV, 2 do
			local score = ARGV[index]
			local member = ARGV[index + 1]
			redis.call('ZREMRANGEBYSCORE', KEYS[1], score, score)
			redis.call('ZADD', KEYS[1], score, member)
		end

		redis.call(
			'ZREMRANGEBYRANK',
			KEYS[1],
			0,
			-maxEntries - 1
		)
		redis.call('PEXPIRE', KEYS[1], ttlMilliseconds)
		return 1
	`

	_, err := rc.store.Eval(
		ctx,
		script,
		[]string{RecentMessageSeqKey(roomId)},
		args...,
	)
	if err != nil {
		return fmt.Errorf("写入近期消息缓存失败: %w", err)
	}

	return nil
}

func (rc *RoomCache) ListMessageAfterSeq(ctx context.Context, roomId string, afterSeq, latestSeq int64) (roomcache.RecentMessageRange, error) {
	page := roomcache.RecentMessageRange{}

	if roomId == "" {
		return page, errors.New("roomID 不能为空")
	}
	if latestSeq <= 0 {
		return page, errors.New("latestSeq 必须大于 0")
	}
	if latestSeq <= afterSeq {
		page.Covered = true
		return page, nil
	}

	key := RecentMessageSeqKey(roomId)
	oldest, err := rc.store.Client().ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil {
		return page, err
	}

	// 缓存不存在
	if len(oldest) == 0 {
		return page, nil
	}

	oldestSeq := int64(oldest[0].Score)
	if afterSeq < oldestSeq-1 {
		return page, nil
	}

	members, err := rc.store.Client().ZRangeArgs(
		ctx,
		redis.ZRangeArgs{
			Key:     key,
			Start:   fmt.Sprintf("(%d", afterSeq),
			Stop:    strconv.FormatInt(latestSeq, 10),
			ByScore: true,
			Offset:  0,
		},
	).Result()

	if err != nil {
		return page, err
	}

	events := make([]protocol.MessageEvent, 0, len(members))
	for _, member := range members {
		var event protocol.MessageEvent
		if err := json.Unmarshal([]byte(member), &event); err != nil {
			return page, fmt.Errorf("解析缓存消息失败: %w", err)
		}
		events = append(events, event)
	}

	// 正常消息 seq 应该连续。发现中间缺口时，交给上层回源数据库。
	expectedSeq := afterSeq + 1
	for _, event := range events {
		if event.Seq != expectedSeq {
			return page, nil
		}
		expectedSeq++
	}

	if len(events) == 0 || events[len(events)-1].Seq != latestSeq {
		// ZSET 没有覆盖到最新 seq，不能把当前结果当成完整结果。
		return page, nil
	}

	page.Events = events
	page.Covered = true

	return page, nil
}

func (rc *RoomCache) UpdateInviteCode(
	ctx context.Context,
	roomId string,
	ttl time.Duration,
) (string, error) {

	roomKey := RoomInviteKey(roomId)
	if ttl <= 0 {
		return "", ErrInvalidTTL
	}

	oldCode, err := rc.store.GetRequiredString(ctx, roomKey)

	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}

	if err == nil {
		mappedRoomID, reverseErr := rc.store.GetRequiredString(ctx, InviteKey(oldCode))
		if reverseErr == nil && mappedRoomID == roomId {
			// 设立双向映射
			const refreshScript = `
				redis.call('PEXPIRE', KEYS[1], ARGV[1])
				redis.call('PEXPIRE', KEYS[2], ARGV[1])
				return 1
			`
			if _, err := rc.store.Eval(
				ctx,
				refreshScript,
				[]string{roomKey, InviteKey(oldCode)},
				ttl.Milliseconds(),
			); err != nil {
				return "", err
			}
			return oldCode, nil
		}
		if reverseErr != nil && !errors.Is(reverseErr, redis.Nil) {
			return "", reverseErr
		}
		if err := rc.store.Del(ctx, roomKey); err != nil {
			return "", err
		}
	}

	for range 9 {

		code, err := rc.randCode(9)
		if err != nil {
			continue
		}

		const createScript = `
			local existing = redis.call('GET', KEYS[1])
			if existing then
				return existing
			end
			if redis.call('EXISTS', KEYS[2]) == 1 then
				return ''
			end
			redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1])
			redis.call('PSETEX', KEYS[2], ARGV[3], ARGV[2])
			return ARGV[1]
		`
		result, err := rc.store.Eval(
			ctx,
			createScript,
			[]string{roomKey, InviteKey(code)},
			code,
			roomId,
			ttl.Milliseconds(),
		)
		if err != nil {
			continue
		}
		createdCode, ok := result.(string)
		if !ok || createdCode == "" {
			continue
		}

		return createdCode, nil
	}

	return "", ErrInviteCodeGenerationFailed
}

func (rc *RoomCache) GetInviteCode(ctx context.Context, roomId string) (string, error) {
	inviteCode, err := rc.store.GetRequiredString(ctx, RoomInviteKey(roomId))

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", roomentity.ErrRoomNotFound
		}

		return "", err
	}

	return inviteCode, err
}

func (rc *RoomCache) GetRoomIDByCode(ctx context.Context, code string) (string, error) {
	roomId, err := rc.store.GetRequiredString(ctx, InviteKey(code))

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", roomentity.ErrInviteCodeExpired
		}
		return "", err
	}

	return roomId, nil
}

func (rc *RoomCache) DeleteInviteCode(ctx context.Context, roomId string) error {
	// 双向删除，防止误删
	const script = `
		local code = redis.call('GET', KEYS[1])
		if not code then
			return 0
		end
		redis.call('DEL', KEYS[1])
		local reverseKey = ARGV[1] .. code
		if redis.call('GET', reverseKey) == ARGV[2] then
			redis.call('DEL', reverseKey)
		end
		return 1
	`
	_, err := rc.store.Eval(
		ctx,
		script,
		[]string{RoomInviteKey(roomId)},
		InviteKeyPrefix(),
		roomId,
	)
	return err
}
