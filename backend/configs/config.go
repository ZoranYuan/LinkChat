package configs

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml"
)

type Config struct {
	App         App               `yaml:"app"`
	Server      Server            `yaml:"server"`
	GRPC        GRPCConfig        `yaml:"grpc"`
	HTTP        HTTPConfig        `yaml:"http"`
	Database    Database          `yaml:"database"`
	JWT         JWT               `yaml:"jwt"`
	Security    SecurityConfig    `yaml:"security"`
	WebSocket   WebSocketConfig   `yaml:"ws"`
	Message     MessageConfig     `yaml:"message"`
	Kafka       KafkaConfig       `yaml:"kafka"`
	Storage     StorageConfig     `yaml:"storage"`
	FileCleanup FileCleanupConfig `yaml:"file_cleanup"`
	Cache       CacheConfig       `yaml:"cache"`
	Friend      FriendConfig      `yaml:"friend"`
	Room        RoomConfig        `yaml:"room"`
}

type HTTPConfig struct {
	UserAPIRateLimit UserAPIRateLimitConfig `yaml:"user_api_rate_limit"`
}

type UserAPIRateLimitConfig struct {
	RatePerSecond float64 `yaml:"rate_per_second"`
	Burst         int64   `yaml:"burst"`
}

type RoomConfig struct {
	MaxMembers           int `yaml:"max_members"`
	InviteCodeTTLSeconds int `yaml:"invite_code_ttl_seconds"`
	InviteCodeLength     int `yaml:"invite_code_length"`
	InviteCodeAttempts   int `yaml:"invite_code_attempts"`
}

type FriendConfig struct {
	RequestTimeoutSeconds  int `yaml:"request_timeout_seconds"`
	RequestCooldownSeconds int `yaml:"request_cooldown_seconds"`
}

type CacheConfig struct {
	MessageDedup       MessageDedupConfig       `yaml:"message_dedup"`
	RoomMessages       RoomMessagesConfig       `yaml:"room_messages"`
	AttachmentAccess   AttachmentAccessConfig   `yaml:"attachment_access"`
	AttachmentFileCard AttachmentFileCardConfig `yaml:"attachment_file_card"`
	RoomUnreadSnapshot RoomUnreadSnapshotConfig `yaml:"room_unread_snapshot"`
	RoomInfo           RoomInfoConfig           `yaml:"room_info"`
	ConversationInfo   ConversationInfoConfig   `yaml:"conversation_info"`
}

type MessageDedupConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type RoomMessagesConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxEntries int `yaml:"max_entries"`
}

type RoomInfoConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type ConversationInfoConfig struct {
	TTLSeconds         int `yaml:"ttl_seconds"`
	NegativeTTLSeconds int `yaml:"negative_ttl_seconds"`
}

type RoomUnreadSnapshotConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type AttachmentAccessConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type AttachmentFileCardConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`

	Topics KafkaTopics `yaml:"topics"`

	Consumer KafkaConsumerConfig `yaml:"consumer"`

	Producer KafkaProducerConfig `yaml:"producer"`
}

type KafkaTopics struct {
	Message              string `yaml:"message"`
	ReadMessageCommitted string `yaml:"read_message_committed"`
	FriendRequestCreated string `yaml:"friend_request_created"`
	RoomMemberChanged    string `yaml:"room_member_changed"`
}

type KafkaConsumerConfig struct {
	GroupID                       string `yaml:"group_id"`
	Version                       string `yaml:"version"`
	Assignor                      string `yaml:"assignor"` // range / roundrobin / sticky
	MaxAttempts                   int    `yaml:"max_attempts"`
	LeaseStaleAfterSecs           int    `yaml:"lease_stale_after_seconds"`
	ConsumeRetryIntervalSecs      int    `yaml:"consume_retry_interval_seconds"`
	DeadLetterSuffix              string `yaml:"dead_letter_suffix"`
	RetryInitialDelayMilliseconds int    `yaml:"retry_initial_delay_milliseconds"`
	RetryMaxDelayMilliseconds     int    `yaml:"retry_max_delay_milliseconds"`
}

type KafkaProducerConfig struct {
	Retries              int    `yaml:"retries"`
	BatchSize            int    `yaml:"batch_size"`
	LingerMs             int    `yaml:"linger_ms"`
	Compression          string `yaml:"compression"`
	ClaimBatchSize       int    `yaml:"claim_batch_size"`
	PollIntervalSeconds  int    `yaml:"poll_interval_seconds"`
	StaleAfterSeconds    int    `yaml:"stale_after_seconds"`
	BaseRetryWaitSeconds int    `yaml:"base_retry_wait_seconds"`
	MaxRetries           int    `yaml:"max_retries"`
	MaxRetryWaitSeconds  int    `yaml:"max_retry_wait_seconds"`
}

type StorageConfig struct {
	MinIO MinIOConfig `yaml:"minio"`
}

type FileCleanupConfig struct {
	IntervalSeconds         int `yaml:"interval_seconds"`
	BatchSize               int `yaml:"batch_size"`
	StaleAfterSeconds       int `yaml:"stale_after_seconds"`
	BaseRetryWaitSeconds    int `yaml:"base_retry_wait_seconds"`
	OperationTimeoutSeconds int `yaml:"operation_timeout_seconds"`
	MaxRetries              int `yaml:"max_retries"`
	OrphanAfterSeconds      int `yaml:"orphan_after_seconds"`
	CompleteLockTTLSeconds  int `yaml:"complete_lock_ttl_seconds"`
	MaxRetryWaitSeconds     int `yaml:"max_retry_wait_seconds"`
}

type MinIOConfig struct {
	Endpoint                        string   `yaml:"endpoint"`
	PublicEndpoint                  string   `yaml:"public_endpoint"`
	Region                          string   `yaml:"region"`
	AccessKeyID                     string   `yaml:"access_key_id"`
	SecretAccessKey                 string   `yaml:"secret_access_key"`
	Bucket                          string   `yaml:"bucket"`
	UseSSL                          bool     `yaml:"use_ssl"`
	CacheTTLSeconds                 int      `yaml:"cache_ttl_seconds"`
	URLTTLSeconds                   int      `yaml:"url_ttl_seconds"`
	MultipartTTL                    int      `yaml:"multipartTTL"`
	PartURLTTLSeconds               int      `yaml:"part_url_ttl_seconds"`
	DirectUploadURLTTLSeconds       int      `yaml:"direct_upload_url_ttl_seconds"`
	DirectUploadMaxSizeBytes        int64    `yaml:"direct_upload_max_size_bytes"`
	MultipartInitLockTTLSeconds     int      `yaml:"multipart_init_lock_ttl_seconds"`
	MultipartCompleteLockTTLSeconds int      `yaml:"multipart_complete_lock_ttl_seconds"`
	MaxFileSizeBytes                int64    `yaml:"max_file_size_bytes"`
	MaxMultipartParts               int      `yaml:"max_multipart_parts"`
	UploadRate                      float64  `yaml:"upload_rate"`
	UploadBurst                     int64    `yaml:"upload_burst"`
	CompleteLockRenewTimeoutSeconds int      `yaml:"complete_lock_renew_timeout_seconds"`
	LockReleaseTimeoutSeconds       int      `yaml:"lock_release_timeout_seconds"`
	ObjectCleanupTimeoutSeconds     int      `yaml:"object_cleanup_timeout_seconds"`
	AllowedContentTypes             []string `yaml:"allowed_content_types"`
}

type App struct {
	Env       string `yaml:"env"`
	MachineID int64  `yaml:"machineId"`
}

type WebSocketConfig struct {
	WriteWaitSeconds           int `yaml:"write_wait_seconds"`
	PongWaitSeconds            int `yaml:"pong_wait_seconds"`
	PingPeriodSeconds          int `yaml:"ping_period_seconds"`
	TimerInterval              int `yaml:"timer_interval_seconds"`
	PageHiddenTimeoutSeconds   int `yaml:"page_hidden_timeout_seconds"`
	CloseTimeoutSeconds        int `yaml:"close_timeout_seconds"`
	MaxMessageSize             int `yaml:"max_message_size"`
	MaxMessageSendBufferSize   int `yaml:"max_message_send_buffer_size"`
	MessageSequencerShardCount int `yaml:"message_sequencer_shard_count"`
	MessageBatchSize           int `yaml:"message_batch_size"`
	MessageFlushIntervalMs     int `yaml:"message_flush_interval_milliseconds"`
	SendMessageRate            int `yaml:"send_message_rate"`
	SendMessageBurst           int `yaml:"send_message_burst"`
	BatchMaxMessages           int `yaml:"batch_max_messages"`
	BatchMaxBytes              int `yaml:"batch_max_bytes"`
	BatchLingerMilliseconds    int `yaml:"batch_linger_milliseconds"`
	BatchReadyQueueSize        int `yaml:"batch_ready_queue_size"`
}

type MessageConfig struct {
	RoomRealtimeFanoutLimit           int   `yaml:"room_realtime_fanout_limit"`
	LargeRoomNoticeLingerMilliseconds int   `yaml:"large_room_notice_linger_milliseconds"`
	LargeRoomNoticeShardCount         int   `yaml:"large_room_notice_shard_count"`
	RoomActivityWindowSeconds         int   `yaml:"room_activity_window_seconds"`
	RoomActivityHoldSeconds           int   `yaml:"room_activity_hold_seconds"`
	RoomActivitySweepSeconds          int   `yaml:"room_activity_sweep_seconds"`
	RoomActivityBucketSeconds         int   `yaml:"room_activity_bucket_seconds"`
	RoomActivityKeyTTLSeconds         int   `yaml:"room_activity_key_ttl_seconds"`
	RoomActivityWarnFanoutWork        int   `yaml:"room_activity_warn_fanout_work"`
	RoomActivityActiveFanoutWork      int   `yaml:"room_activity_active_fanout_work"`
	RoomMemberStateTTLSeconds         int   `yaml:"room_member_state_ttl_seconds"`
	RoomMemberNegativeTTLSeconds      int   `yaml:"room_member_negative_ttl_seconds"`
	HistoryDefaultLimit               int   `yaml:"history_default_limit"`
	HistoryMaxLimit                   int   `yaml:"history_max_limit"`
	OfflinePageSize                   int   `yaml:"offline_page_size"`
	MaxSyncSeqs                       int   `yaml:"max_sync_seqs"`
	MemberQueryTimeoutSeconds         int   `yaml:"member_query_timeout_seconds"`
	MaxTextRunes                      int   `yaml:"max_text_runes"`
	MaxIdentifierLength               int   `yaml:"max_identifier_length"`
	AttachmentTTLSeconds              int64 `yaml:"attachment_ttl_seconds"`
}

type Server struct {
	Port                   string `yaml:"port"`
	ReadTimeoutSeconds     int    `yaml:"read_timeout_seconds"`
	IdleTimeoutSeconds     int    `yaml:"idle_timeout_seconds"`
	ShutdownTimeoutSeconds int    `yaml:"shutdown_timeout_seconds"`
	HealthTimeoutSeconds   int    `yaml:"health_timeout_seconds"`
	MaxHeaderBytes         int    `yaml:"max_header_bytes"`
}

type GRPCConfig struct {
	RoomAgentAddress string `yaml:"room_agent_address"`
}

type Database struct {
	MySQL MySQL `yaml:"mysql"`
	Redis Redis `yaml:"redis"`
}

type Redis struct {
	DSN string `yaml:"dsn"`
}

type MySQL struct {
	DSN                       string `yaml:"dsn"`
	MaxOpenConns              int    `yaml:"max_open_conns"`
	MaxIdleConns              int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeSeconds    int    `yaml:"conn_max_lifetime_seconds"`
	SlowThresholdMilliseconds int    `yaml:"slow_threshold_milliseconds"`
	LogLevel                  string `yaml:"log_level"`
}

type JWT struct {
	Issuer                         string `yaml:"issuer"`
	Secret                         string `yaml:"secret"`
	AccessExpireMinutes            int    `yaml:"access_expire_minutes"`
	RefreshExpireHours             int    `yaml:"refresh_expire_hours"`
	SessionOperationTimeoutSeconds int    `yaml:"session_operation_timeout_seconds"`
}

type SecurityConfig struct {
	AllowedOrigins  string `yaml:"allowed_origins"`
	BcryptCost      int    `yaml:"bcrypt_cost"`
	RateLimitPrefix string `yaml:"rate_limit_prefix"`
}

func LoadConfig(path string) Config {
	data, err := os.ReadFile(path)

	if err != nil {
		log.Fatal("加载配置文件失败：", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatal("解析配置文件失败：", err)
	}
	if err := applyEnvironmentOverrides(&config); err != nil {
		log.Fatal("环境变量配置无效：", err)
	}

	return config
}

func applyEnvironmentOverrides(config *Config) error {
	if value := os.Getenv("GORM_LOG_LEVEL"); value != "" {
		config.Database.MySQL.LogLevel = value
	}
	if value := os.Getenv("APP_ENV"); value != "" {
		config.App.Env = value
	}
	if value := os.Getenv("SERVER_PORT"); value != "" {
		config.Server.Port = value
	}
	if value := os.Getenv("GRPC_ROOM_AGENT_ADDRESS"); value != "" {
		config.GRPC.RoomAgentAddress = value
	}
	if value := os.Getenv("MYSQL_DSN"); value != "" {
		config.Database.MySQL.DSN = value
	}
	if value := os.Getenv("REDIS_DSN"); value != "" {
		config.Database.Redis.DSN = value
	}
	if value := os.Getenv("JWT_SECRET"); value != "" {
		config.JWT.Secret = value
	}
	if value := os.Getenv("KAFKA_BROKERS"); value != "" {
		config.Kafka.Brokers = splitCSV(value)
	}
	if value := os.Getenv("KAFKA_GROUP_ID"); value != "" {
		config.Kafka.Consumer.GroupID = value
	}
	if value := os.Getenv("MINIO_ENDPOINT"); value != "" {
		config.Storage.MinIO.Endpoint = value
	}
	if value := os.Getenv("MINIO_PUBLIC_ENDPOINT"); value != "" {
		config.Storage.MinIO.PublicEndpoint = value
	}
	if value := os.Getenv("MINIO_ACCESS_KEY_ID"); value != "" {
		config.Storage.MinIO.AccessKeyID = value
	}
	if value := os.Getenv("MINIO_SECRET_ACCESS_KEY"); value != "" {
		config.Storage.MinIO.SecretAccessKey = value
	}
	if value := os.Getenv("MINIO_BUCKET"); value != "" {
		config.Storage.MinIO.Bucket = value
	}
	if value := os.Getenv("MINIO_USE_SSL"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return errors.New("MINIO_USE_SSL 必须为布尔值")
		}
		config.Storage.MinIO.UseSSL = parsed
	}
	if value := os.Getenv("ALLOWED_ORIGINS"); value != "" {
		config.Security.AllowedOrigins = value
	}
	return nil
}

func splitCSV(value string) []string {
	items := strings.Split(value, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
