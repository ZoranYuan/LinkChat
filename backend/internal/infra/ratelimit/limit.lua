local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

if rate == nil or rate <= 0 or burst == nil or burst <= 0 then
    return redis.error_reply("限流策略参数无效")
end

-- 使用 redis 时间，避免时钟不一致
local redis_time = redis.call("TIME")
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)

local bucket = redis.call(
    'HMGET',
    KEYS[1],
    "tokens",
    "update_at_ms"
)

local tokens = tonumber(bucket[1])
local update_at_ms = tonumber(bucket[2])

-- 更新 tokens 和 update_at_ms
if tokens == nil or update_at_ms == nil then
    tokens = burst
else
    local elapse_ms = math.max(
        0,
        now_ms-update_at_ms
    )

    tokens = math.min(
        burst,
        tokens + (elapse_ms / 1000) * rate
    )

end

local allowed = 0
local retry_after_ms = 0

if tokens >= 1 then
    allowed = 1
    tokens = tokens - 1
else 
    allowed = 0
    retry_after_ms = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call(
    'HSET',
    KEYS[1],
    "tokens",
    tokens,
    "update_at_ms",
    now_ms
)

-- 为了避免 redis key 无限增长，需要为当前的 key 设置一个过期时间
local ttl_ms = math.max(
    1000,
    math.ceil((burst / rate) * 2000)
)

redis.call(
    'PEXPIRE', KEYS[1], ttl_ms
)

return {
    allowed,
    math.floor(tokens),
    retry_after_ms,
}
