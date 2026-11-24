package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// KafkaPublishMessageDuration 记录 flushBatch 中单条 producer.Publish 的墙钟耗时。
	// 用于验证"逐条 SendMessage 是否被 SyncProducer 的 Flush.Frequency(默认 10ms) 放大"：
	// 若直方图在 ~10ms 出现明显堆积，则假设成立；若集中在亚毫秒则假设不成立。
	KafkaPublishMessageDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "linkchat_kafka_publish_message_duration_seconds",
			Help:    "flushBatch 中单条 producer.Publish 的墙钟耗时，按 topic 区分。",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.008, 0.01, 0.012, 0.015, 0.02, 0.05, 0.1, 0.5, 1},
		},
		[]string{"topic"},
	)

	// KafkaPublishBatchDuration 记录一批 pending 消息全部 publish 完成的总墙钟耗时。
	KafkaPublishBatchDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "linkchat_kafka_publish_batch_duration_seconds",
			Help:    "flushBatch 中整批消息 publish 段的总墙钟耗时。",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		},
	)

	// KafkaPublishBatchMessages 记录一批里实际 publish 的消息条数（pending 长度）。
	KafkaPublishBatchMessages = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "linkchat_kafka_publish_batch_messages",
			Help:    "flushBatch 每次投递的消息条数（pending 长度）。",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 500, 1000, 2000, 4000},
		},
	)
)
