package configs

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml"
)

type Config struct {
	App         App               `yaml:"app"`
	Server      Server            `yaml:"server"`
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
	Agent       AgentConfig       `yaml:"agent"`
}

type HTTPConfig struct {
	UserAPIRateLimit UserAPIRateLimitConfig `yaml:"user_api_rate_limit"`
}

type UserAPIRateLimitConfig struct {
	RatePerSecond float64 `yaml:"rate_per_second"`
	Burst         int64   `yaml:"burst"`
}

type AgentConfig struct {
	RoomSummaryEndpoint string `yaml:"room_summary_endpoint"`
}

type UserProfile struct {
	NegativeTTL int `yaml:"negative_ttl"`
	TTL         int `yaml:"ttl"`
}

type CacheConfig struct {
	UserProfile        UserProfile              `yaml:"user_profile"`
	AttachmentAccess   AttachmentAccessConfig   `yaml:"attachment_access"`
	AttachmentFileCard AttachmentFileCardConfig `yaml:"attachment_file_card"`
	RoomUnreadSnapshot RoomUnreadSnapshotConfig `yaml:"room_unread_snapshot"`
	RoomInfo           RoomInfoConfig           `yaml:"room_info"`
}

type RoomInfoConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
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

	Partition KafkaPartitionConfig `yaml:"partition"`
}

type KafkaTopics struct {
	Message              string `yaml:"message"`
	ReadMessageCommitted string `yaml:"read_message_committed"`
	FriendRequestCreated string `yaml:"friend_request_created"`
	RoomMemberChanged    string `yaml:"room_member_changed"`
	FileCardWarmup       string `yaml:"file_card_warmup"`
}

type KafkaConsumerConfig struct {
	GroupID                  string `yaml:"group_id"`
	Version                  string `yaml:"version"`
	Assignor                 string `yaml:"assignor"` // range / roundrobin / sticky
	MaxRetries               int    `yaml:"max_retries"`
	LeaseStaleAfterSecs      int    `yaml:"lease_stale_after_seconds"`
	ConsumeRetryIntervalSecs int    `yaml:"consume_retry_interval_seconds"`
	DeadLetterSuffix         string `yaml:"dead_letter_suffix"`
}

type KafkaProducerConfig struct {
	Acks                 string `yaml:"acks"`
	Retries              int    `yaml:"retries"`
	BatchSize            int    `yaml:"batch_size"`
	LingerMs             int    `yaml:"linger_ms"`
	Compression          string `yaml:"compression"`
	ClaimBatchSize       int    `yaml:"claim_batch_size"`
	PollIntervalSeconds  int    `yaml:"poll_interval_seconds"`
	StaleAfterSeconds    int    `yaml:"stale_after_seconds"`
	BaseRetryWaitSeconds int    `yaml:"base_retry_wait_seconds"`
	MaxRetries           int    `yaml:"max_retries"`
}

type KafkaPartitionConfig struct {
	Strategy string `yaml:"strategy"` // conversation_hash / user_hash / random
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
}

type MinIOConfig struct {
	Endpoint                        string  `yaml:"endpoint"`
	PublicEndpoint                  string  `yaml:"public_endpoint"`
	Region                          string  `yaml:"region"`
	AccessKeyID                     string  `yaml:"access_key_id"`
	SecretAccessKey                 string  `yaml:"secret_access_key"`
	Bucket                          string  `yaml:"bucket"`
	UseSSL                          bool    `yaml:"use_ssl"`
	CacheTTLSeconds                 int     `yaml:"cache_ttl_seconds"`
	URLTTLSeconds                   int     `yaml:"url_ttl_seconds"`
	MultipartTTL                    int     `yaml:"multipartTTL"`
	PartURLTTLSeconds               int     `yaml:"part_url_ttl_seconds"`
	DirectUploadURLTTLSeconds       int     `yaml:"direct_upload_url_ttl_seconds"`
	DirectUploadMaxSizeBytes        int64   `yaml:"direct_upload_max_size_bytes"`
	MultipartInitLockTTLSeconds     int     `yaml:"multipart_init_lock_ttl_seconds"`
	MultipartCompleteLockTTLSeconds int     `yaml:"multipart_complete_lock_ttl_seconds"`
	MaxFileSizeBytes                int64   `yaml:"max_file_size_bytes"`
	MaxMultipartParts               int     `yaml:"max_multipart_parts"`
	UploadRate                      float64 `yaml:"upload_rate"`
	UploadBurst                     int64   `yaml:"upload_burst"`
}

type App struct {
	Name      string `yaml:"name"`
	Version   string `yaml:"version"`
	Env       string `yaml:"env"`
	MachineID int64  `yaml:"machineId"`
}

type WebSocketConfig struct {
	WriteWaitSeconds         int `yaml:"write_wait_seconds"`
	PongWaitSeconds          int `yaml:"pong_wait_seconds"`
	PingPeriodSeconds        int `yaml:"ping_period_seconds"`
	TimerInterval            int `yaml:"timer_interval_seconds"`
	MaxMessageSize           int `yaml:"max_message_size"`
	MaxMessageSendBufferSize int `yaml:"max_message_send_buffer_size"`
	InboundWorkerCount       int `yaml:"inbound_worker_count"`
	SendMessageRate          int `yaml:"send_message_rate"`
	SendMessageBurst         int `yaml:"send_message_burst"`
	BatchMaxMessages         int `yaml:"batch_max_messages"`
	BatchMaxBytes            int `yaml:"batch_max_bytes"`
	BatchLingerMilliseconds  int `yaml:"batch_linger_milliseconds"`
	BatchReadyQueueSize      int `yaml:"batch_ready_queue_size"`
}

type MessageConfig struct {
	RoomRealtimeFanoutLimit           int   `yaml:"room_realtime_fanout_limit"`
	LargeRoomNoticeLingerMilliseconds int   `yaml:"large_room_notice_linger_milliseconds"`
	LargeRoomNoticeShardCount         int   `yaml:"large_room_notice_shard_count"`
	RoomActivityWindowSeconds         int   `yaml:"room_activity_window_seconds"`
	RoomActivityHoldSeconds           int   `yaml:"room_activity_hold_seconds"`
	RoomActivityBucketSeconds         int   `yaml:"room_activity_bucket_seconds"`
	RoomActivityKeyTTLSeconds         int   `yaml:"room_activity_key_ttl_seconds"`
	RoomActivityWarnFanoutWork        int   `yaml:"room_activity_warn_fanout_work"`
	RoomActivityActiveFanoutWork      int   `yaml:"room_activity_active_fanout_work"`
	RoomMemberStateTTLSeconds         int   `yaml:"room_member_state_ttl_seconds"`
	RoomMemberNegativeTTLSeconds      int   `yaml:"room_member_negative_ttl_seconds"`
	HistoryDefaultLimit               int   `yaml:"history_default_limit"`
	HistoryMaxLimit                   int   `yaml:"history_max_limit"`
	MaxTextRunes                      int   `yaml:"max_text_runes"`
	MaxWidth                          int   `yaml:"max_width"`
	MaxHeight                         int   `yaml:"max_height"`
	MaxVideoMs                        int64 `yaml:"max_video_ms"`
	MaxIdentifierLength               int   `yaml:"max_identifier_length"`
	AttachmentTTLSeconds              int64 `yaml:"attachment_ttl_seconds"`
}

type Server struct {
	Port string `yaml:"port"`
}

type Database struct {
	MySQL MySQL `yaml:"mysql"`
	Redis Redis `yaml:"redis"`
}

type Redis struct {
	DSN string `yaml:"dsn"`
}

type MySQL struct {
	DSN string `yaml:"dsn"`
}

type JWT struct {
	Secret              string `yaml:"secret"`
	AccessExpireMinutes int    `yaml:"access_expire_minutes"`
	RefreshExpireHours  int    `yaml:"refresh_expire_hours"`
}

type SecurityConfig struct {
	AllowedOrigins string `yaml:"allowed_origins"`
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
	applyEnvironmentOverrides(&config)

	return config
}

func applyEnvironmentOverrides(config *Config) {
	if value := os.Getenv("APP_ENV"); value != "" {
		config.App.Env = value
	}
	if value := os.Getenv("SERVER_PORT"); value != "" {
		config.Server.Port = value
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
		if parsed, err := strconv.ParseBool(value); err == nil {
			config.Storage.MinIO.UseSSL = parsed
		}
	}
	if value := os.Getenv("ALLOWED_ORIGINS"); value != "" {
		config.Security.AllowedOrigins = value
	}
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

func validateFileEndPoint(publicEndpoint string, allowedOrigins []string) error {
	fileURL, err := url.Parse(publicEndpoint)
	if err != nil || fileURL.Host == "" {
		return errors.New("MinIO public_endpoint 无效")
	}

	for _, orign := range allowedOrigins {
		// 禁止 minio 访问域名与后端域名相同
		orign, err := url.Parse(orign)
		if err == nil && orign.Host == fileURL.Host {
			return errors.New("文件域名不能与主站域名相同")
		}
	}
	return nil
}

func (c Config) Validate() error {
	if c.Server.Port == "" {
		return fmt.Errorf("server.port 不能为空")
	}
	if c.Database.MySQL.DSN == "" || c.Database.Redis.DSN == "" {
		return fmt.Errorf("数据库配置不完整")
	}
	if c.JWT.Secret == "" || c.JWT.AccessExpireMinutes <= 0 || c.JWT.RefreshExpireHours <= 0 {
		return fmt.Errorf("JWT 配置无效")
	}
	if len(c.Kafka.Brokers) == 0 || c.Kafka.Consumer.GroupID == "" {
		return fmt.Errorf("Kafka 配置不完整")
	}
	if c.Storage.MinIO.Endpoint == "" || c.Storage.MinIO.AccessKeyID == "" ||
		c.Storage.MinIO.SecretAccessKey == "" || c.Storage.MinIO.Bucket == "" {
		return fmt.Errorf("MinIO 配置不完整")
	}
	if c.Storage.MinIO.MaxFileSizeBytes <= 0 || c.Storage.MinIO.MaxMultipartParts <= 0 {
		return fmt.Errorf("文件上传限制必须大于 0")
	}
	if c.WebSocket.MaxMessageSize <= 0 || c.WebSocket.MaxMessageSendBufferSize <= 0 ||
		c.WebSocket.InboundWorkerCount <= 0 ||
		c.WebSocket.WriteWaitSeconds <= 0 || c.WebSocket.PongWaitSeconds <= 0 ||
		c.WebSocket.PingPeriodSeconds <= 0 || c.WebSocket.PingPeriodSeconds >= c.WebSocket.PongWaitSeconds {
		return fmt.Errorf("WebSocket 配置无效")
	}
	if c.Message.RoomActivityHoldSeconds <= 0 {
		return fmt.Errorf("房间 ACTIVE 保持时间必须大于 0")
	}
	if c.Message.RoomMemberStateTTLSeconds <= 0 ||
		c.Message.RoomMemberNegativeTTLSeconds <= 0 {
		return fmt.Errorf("房间成员缓存 TTL 配置无效")
	}
	if c.Cache.RoomInfo.TTLSeconds <= 0 {
		return fmt.Errorf("房间基础信息缓存 TTL 配置无效")
	}
	if strings.EqualFold(c.App.Env, "production") {
		if len(c.JWT.Secret) < 32 || c.JWT.Secret == "U2FsdGVkX19anQGSRtiUwgRLpWV333jI4xjlCF32dek=" {
			return fmt.Errorf("生产环境必须使用随机 JWT secret")
		}
		if c.Storage.MinIO.AccessKeyID == "minioadmin" || c.Storage.MinIO.SecretAccessKey == "minioadmin" {
			return fmt.Errorf("生产环境禁止使用默认 MinIO 凭据")
		}
		if c.Security.AllowedOrigins == "" {
			return fmt.Errorf("生产环境必须配置 security.allowed_origins")
		}
	}

	if c.App.Env == "production" && c.Storage.MinIO.PublicEndpoint == "" {
		return errors.New("生产环境必须配置 minio.public_endpoint")
	}

	if err := validateFileEndPoint(c.Storage.MinIO.PublicEndpoint, []string{}); err != nil {
		return err
	}

	return nil
}
