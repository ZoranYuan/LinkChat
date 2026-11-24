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
	Group       GroupConfig       `yaml:"group"`
}

type KafkaTopics struct {
	MessageSendPersist string `yaml:"message.send.persist"`
	MessageSendCache   string `yaml:"message.send.cache"`
	MessageSendPush    string `yaml:"message.send.push"`
	MessageReadReceipt string `yaml:"message.read.receipt"`
}

type HTTPConfig struct {
	UserAPIRateLimit UserAPIRateLimitConfig `yaml:"user_api_rate_limit"`
}

type UserAPIRateLimitConfig struct {
	RatePerSecond float64 `yaml:"rate_per_second"`
	Burst         int64   `yaml:"burst"`
}

type GroupConfig struct {
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
	MessageDedup        MessageDedupConfig        `yaml:"message_dedup"`
	GroupMessages       GroupMessagesConfig       `yaml:"group_messages"`
	AttachmentAccess    AttachmentAccessConfig    `yaml:"attachment_access"`
	GroupUnreadSnapshot GroupUnreadSnapshotConfig `yaml:"group_unread_snapshot"`
	GroupInfo           GroupInfoConfig           `yaml:"group_info"`
	ConversationInfo    ConversationInfoConfig    `yaml:"conversation_info"`
	FriendRelation      FriendRelationCacheConfig `yaml:"friend_relation"`
}

type MessageDedupConfig struct {
	TTLSeconds           int `yaml:"ttl_seconds"`
	ProcessingTTLSeconds int `yaml:"processing_ttl_seconds"`
}

type GroupMessagesConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxEntries int `yaml:"max_entries"`
}

type GroupInfoConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type ConversationInfoConfig struct {
	TTLSeconds         int `yaml:"ttl_seconds"`
	NegativeTTLSeconds int `yaml:"negative_ttl_seconds"`
}

type FriendRelationCacheConfig struct {
	TTLSeconds         int `yaml:"ttl_seconds"`
	NegativeTTLSeconds int `yaml:"negative_ttl_seconds"`
}

type GroupUnreadSnapshotConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type AttachmentAccessConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`

	Topics KafkaTopics `yaml:"topics"`

	Consumer KafkaConsumerConfig `yaml:"consumer"`

	Producer KafkaProducerConfig `yaml:"producer"`
}

type KafkaConsumerConfig struct {
	GroupID                       string `yaml:"group_id"`
	Version                       string `yaml:"version"`
	Assignor                      string `yaml:"assignor"` // range / roundrobin / sticky
	MaxAttempts                   int    `yaml:"max_attempts"`
	ConsumeRetryIntervalSecs      int    `yaml:"consume_retry_interval_seconds"`
	RetryInitialDelayMilliseconds int    `yaml:"retry_initial_delay_milliseconds"`
	RetryMaxDelayMilliseconds     int    `yaml:"retry_max_delay_milliseconds"`
	// offset 异步刷脏：Sarama AutoCommit 后台协程提交间隔（毫秒）
	AutoCommitIntervalMs int `yaml:"auto_commit_interval_ms"`
	// fetch 节流调优：单次 fetch 最多等待时长（毫秒），调小可降低空等
	FetchMaxWaitMs int `yaml:"fetch_max_wait_ms"`
	// fetch 节流调优：单轮 ConsumeClaim 处理预算时长（毫秒），调大可减少暂停切换
	FetchMaxProcessingMs int `yaml:"fetch_max_processing_ms"`

	// 消息攒批：按会话（conversationID）聚合，一批共享一次事务 / fsync，摊薄刷盘固定开销
	Batch MessageBatchConfig `yaml:"batch"`
}

// MessageBatchConfig 消息攒批配置。
// 按会话聚合消息，达到 Size 条或 FlushIntervalMs 毫秒触发一次批量落库事务。
type MessageBatchConfig struct {
	Size            int `yaml:"size"`              // 攒批条数上限，默认 500
	FlushIntervalMs int `yaml:"flush_interval_ms"` // 攒批超时（毫秒），默认 100；到点不足一批也 flush
}

type KafkaProducerConfig struct {
	Retries     int    `yaml:"retries"`
	BatchSize   int    `yaml:"batch_size"`
	LingerMs    int    `yaml:"linger_ms"`
	Compression string `yaml:"compression"`
}

type StorageConfig struct {
	MinIO MinIOConfig `yaml:"minio"`
}

type FileCleanupConfig struct {
	IntervalSeconds         int `yaml:"interval_seconds"`
	BatchSize               int `yaml:"batch_size"`
	OperationTimeoutSeconds int `yaml:"operation_timeout_seconds"`
	OrphanAfterSeconds      int `yaml:"orphan_after_seconds"`
}

type MinIOConfig struct {
	Endpoint                    string   `yaml:"endpoint"`
	PublicEndpoint              string   `yaml:"public_endpoint"`
	Region                      string   `yaml:"region"`
	AccessKeyID                 string   `yaml:"access_key_id"`
	SecretAccessKey             string   `yaml:"secret_access_key"`
	Bucket                      string   `yaml:"bucket"`
	UseSSL                      bool     `yaml:"use_ssl"`
	URLTTLSeconds               int      `yaml:"url_ttl_seconds"`
	MultipartTTL                int      `yaml:"multipartTTL"`
	PartURLTTLSeconds           int      `yaml:"part_url_ttl_seconds"`
	MaxFileSizeBytes            int64    `yaml:"max_file_size_bytes"`
	MaxMultipartParts           int      `yaml:"max_multipart_parts"`
	UploadRate                  float64  `yaml:"upload_rate"`
	UploadBurst                 int64    `yaml:"upload_burst"`
	ObjectCleanupTimeoutSeconds int      `yaml:"object_cleanup_timeout_seconds"`
	AllowedContentTypes         []string `yaml:"allowed_content_types"`
}

type App struct {
	Env       string `yaml:"env"`
	MachineID int64  `yaml:"machineId"`
}

type WebSocketConfig struct {
	WriteWaitSeconds          int `yaml:"write_wait_seconds"`
	PongWaitSeconds           int `yaml:"pong_wait_seconds"`
	PingPeriodSeconds         int `yaml:"ping_period_seconds"`
	TimerInterval             int `yaml:"timer_interval_seconds"`
	PageHiddenTimeoutSeconds  int `yaml:"page_hidden_timeout_seconds"`
	CloseTimeoutSeconds       int `yaml:"close_timeout_seconds"`
	MaxMessageSize            int `yaml:"max_message_size"`
	MaxMessageSendBufferSize  int `yaml:"max_message_send_buffer_size"`
	SendMessageRate           int `yaml:"send_message_rate"`
	SendMessageBurst          int `yaml:"send_message_burst"`
	BatchMaxMessages          int `yaml:"batch_max_messages"`
	BatchMaxBytes             int `yaml:"batch_max_bytes"`
	BatchLingerMilliseconds   int `yaml:"batch_linger_milliseconds"`
	BatchReadyQueueSize       int `yaml:"batch_ready_queue_size"`
	BatchAckTimeoutSeconds    int `yaml:"batch_ack_timeout_seconds"`
	BatchRetryIntervalSeconds int `yaml:"batch_retry_interval_seconds"`
	BatchMaxRetries           int `yaml:"batch_max_retries"`
}

type MessageConfig struct {
	LargeGroupNoticeLingerMilliseconds int   `yaml:"large_group_notice_linger_milliseconds"`
	LargeGroupNoticeShardCount         int   `yaml:"large_group_notice_shard_count"`
	GroupActivityWindowSeconds         int   `yaml:"group_activity_window_seconds"`
	GroupActivityBucketSeconds         int   `yaml:"group_activity_bucket_seconds"`
	GroupActivityKeyTTLSeconds         int   `yaml:"group_activity_key_ttl_seconds"`
	GroupActivityWarnFanoutWork        int   `yaml:"group_activity_warn_fanout_work"`
	GroupActivityActiveFanoutWork      int   `yaml:"group_activity_active_fanout_work"`
	GroupMemberStateTTLSeconds         int   `yaml:"group_member_state_ttl_seconds"`
	GroupMemberNegativeTTLSeconds      int   `yaml:"group_member_negative_ttl_seconds"`
	HistoryDefaultLimit                int   `yaml:"history_default_limit"`
	HistoryMaxLimit                    int   `yaml:"history_max_limit"`
	OfflinePageSize                    int   `yaml:"offline_page_size"`
	MaxSyncSeqs                        int   `yaml:"max_sync_seqs"`
	MemberQueryTimeoutSeconds          int   `yaml:"member_query_timeout_seconds"`
	MaxIdentifierLength                int   `yaml:"max_identifier_length"`
	AttachmentTTLSeconds               int64 `yaml:"attachment_ttl_seconds"`
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
	GroupAgentAddress                string `yaml:"group_agent_address"`
	GroupMessageSyncDefaultBatchSize int    `yaml:"group_message_sync_default_batch_size"`
	GroupMessageSyncMaxBatchSize     int    `yaml:"group_message_sync_max_batch_size"`
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
	if value := os.Getenv("GRPC_GROUP_AGENT_ADDRESS"); value != "" {
		config.GRPC.GroupAgentAddress = value
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
