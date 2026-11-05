package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	KafkaDBTransactionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "linkchat_kafka_db_transaction_duration_seconds",
			Help:    "Kafka消息消费过程中数据库事务的完整耗时，包含事务开始、SQL执行和提交。",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"stage"},
	)
	KafkaDBTransactionBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "linkchat_kafka_db_transaction_batch_size",
			Help:    "Kafka消息消费数据库事务处理的事件数量。",
			Buckets: []float64{1, 4, 8, 16, 32, 64, 128, 256, 500, 1000},
		},
		[]string{"stage"},
	)
	KafkaDBTransactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "linkchat_kafka_db_transactions_total",
			Help: "Kafka消息消费过程中数据库事务的执行次数。",
		},
		[]string{"stage", "outcome"},
	)
)

func ObserveKafkaDBTransaction(stage string, batchSize int, startedAt time.Time, err error) {
	KafkaDBTransactionDuration.WithLabelValues(stage).Observe(time.Since(startedAt).Seconds())
	KafkaDBTransactionBatchSize.WithLabelValues(stage).Observe(float64(batchSize))
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	KafkaDBTransactionsTotal.WithLabelValues(stage, outcome).Inc()
}
