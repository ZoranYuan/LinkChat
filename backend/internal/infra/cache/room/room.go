package room

import (
	"IM_backend/internal/application/ports/messaging"
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

	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/cache/room"
	domainroom "IM_backend/internal/domain/room"
	sharedcache "IM_backend/internal/infra/cache/shared"
)

var (
	_ roomcache.RoomCache = (*RoomCache)(nil)
)

type RoomCache struct {
	store  *sharedcache.Store
	config configs.Config
}

func NewRoomCache(rb *redis.Client, config configs.Config) *RoomCache {
	return &RoomCache{
		store:  sharedcache.NewStore(rb),
		config: config,
	}
}

type activitySettings struct {
	bucketSeconds    int64
	windowBuckets    int
	keyTTLSeconds    int64
	warnFanoutWork   int64
	activeFanoutWork int64
}

func (rc *RoomCache) activitySettings() (activitySettings, error) {
	config := rc.config.Message
	// 向上取整
	windowBuckets := (config.RoomActivityWindowSeconds + config.RoomActivityBucketSeconds - 1) /
		config.RoomActivityBucketSeconds

	return activitySettings{
		bucketSeconds:    int64(config.RoomActivityBucketSeconds),
		windowBuckets:    windowBuckets,
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

// UpdateRoomActivity 累计本批消息，按窗口内消息数与在线连接数返回活跃等级。
func (rc *RoomCache) UpdateRoomActivity(ctx context.Context, roomID string, messageCount, onlineSessions int, at time.Time) (int, error) {
	settings, err := rc.activitySettings()
	if err != nil {
		return roomcache.RoomActivityNormal, err
	}
	currentBucket := at.Unix() / settings.bucketSeconds
	const script = `
		redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[4])
		redis.call('HEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
		local current = tonumber(ARGV[1])
		local windowBuckets = tonumber(ARGV[2])

		local fields = {}
		for offset = 0, windowBuckets - 1 do
			fields[#fields + 1] = tostring(current - offset)
		end

		local values = redis.call('HMGET', KEYS[1], unpack(fields))
		local total = 0
		for _, value in ipairs(values) do
			if value then total = total + tonumber(value) end
		end
		redis.call('EXPIRE', KEYS[1], ARGV[3])
		local fanoutWork = total * tonumber(ARGV[5])
		local warmThreshold = tonumber(ARGV[6])
		local activateThreshold = tonumber(ARGV[7])
		if fanoutWork > activateThreshold then
			return 2
		end
		if fanoutWork > warmThreshold then
			return 1
		end
		return 0
	`
	return rc.store.Client().Eval(
		ctx,
		script,
		[]string{ActivateLevelKey(roomID)},
		currentBucket,
		settings.windowBuckets,
		settings.keyTTLSeconds,
		messageCount,
		onlineSessions,
		settings.warnFanoutWork,
		settings.activeFanoutWork,
	).Int()
}

// WarmRecentMessageEvents 批量回填近期消息缓存。
func (rc *RoomCache) WarmRecentMessageEvents(ctx context.Context, roomID string, events []messaging.MessagePushEvent) error {
	if roomID == "" {
		return errors.New("roomID 不能为空")
	}
	if rc == nil || rc.store == nil || rc.store.Client() == nil {
		return errors.New("近期消息缓存未配置")
	}
	if len(events) == 0 {
		return nil
	}

	orderedEvents := append([]messaging.MessagePushEvent(nil), events...)
	sort.SliceStable(orderedEvents, func(i, j int) bool {
		return orderedEvents[i].Seq < orderedEvents[j].Seq
	})

	args := make([]interface{}, 0, len(orderedEvents)*2+2)
	args = append(args, rc.config.Cache.RoomMessages.MaxEntries, int64(rc.config.Cache.RoomMessages.TTLSeconds)*int64(time.Second/time.Millisecond))
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
		[]string{RecentMessageSeqKey(roomID)},
		args...,
	)
	if err != nil {
		return fmt.Errorf("写入近期消息缓存失败: %w", err)
	}

	return nil
}

func (rc *RoomCache) ListMessageAfterSeq(ctx context.Context, roomID string, afterSeq int64) (roomcache.RecentMessageRange, error) {
	page := roomcache.RecentMessageRange{}

	values, err := rc.store.Client().ZRangeByScore(
		ctx,
		RecentMessageSeqKey(roomID),
		&redis.ZRangeBy{
			Min: "(" + strconv.FormatInt(afterSeq, 10),
			Max: "+inf",
		},
	).Result()
	if err != nil {
		return page, err
	}

	events := make([]messaging.MessagePushEvent, 0, len(values))
	for i, data := range values {
		var event messaging.MessagePushEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return page, err
		}
		if event.Seq != afterSeq+int64(i)+1 {
			return page, nil
		}
		events = append(events, event)
	}

	page.Events = events
	page.Covered = len(events) > 0
	return page, nil
}

func (rc *RoomCache) UpdateInviteCode(
	ctx context.Context,
	roomID string,
	ttl time.Duration,
) (string, error) {

	roomKey := RoomInviteKey(roomID)
	if ttl <= 0 {
		return "", ErrInvalidTTL
	}

	oldCode, err := rc.store.GetRequiredString(ctx, roomKey)

	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}

	if err == nil {
		mappedRoomID, reverseErr := rc.store.GetRequiredString(ctx, InviteKey(oldCode))
		if reverseErr == nil && mappedRoomID == roomID {
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

	for range rc.config.Room.InviteCodeAttempts {

		code, err := rc.randCode(rc.config.Room.InviteCodeLength)
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
			roomID,
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

func (rc *RoomCache) GetRoomIDByCode(ctx context.Context, code string) (string, error) {
	roomID, err := rc.store.GetRequiredString(ctx, InviteKey(code))

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", domainroom.ErrInviteCodeExpired
		}
		return "", err
	}

	return roomID, nil
}

func (rc *RoomCache) DeleteInviteCode(ctx context.Context, roomID string) error {
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
		[]string{RoomInviteKey(roomID)},
		InviteKeyPrefix(),
		roomID,
	)
	return err
}
