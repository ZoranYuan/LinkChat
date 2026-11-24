package kafka

import (
	"context"
	"fmt"
	"strconv"

	deadletterport "IM_backend/core/application/ports/deadletter"

	"github.com/IBM/sarama"
)

// DeadLetterWriter 是标准 DLT 模式的实现：
// 把消费失败的消息发到「原 topic + .DLT」，异常上下文放进 header，payload 原样保留。
// 对照 Spring Kafka 的 DeadLetterPublishingRecoverer（默认 <topic>.DLT + 保留原 headers）。
type DeadLetterWriter struct {
	producer *Producer
}

func NewDeadLetterWriter(producer *Producer) *DeadLetterWriter {
	return &DeadLetterWriter{producer: producer}
}

// DltTopic 标准命名：<原 topic>.DLT
func DltTopic(original string) string { return original + ".DLT" }

func (w *DeadLetterWriter) Write(ctx context.Context, rec deadletterport.Record) error {
	// 用原 message key（eventID）作 partition key：
	// 重放时同一条消息仍落同分区，且 DLT 分区数需 >= 原 topic（建 topic 时保证）。
	partitionKey := rec.EventID
	if partitionKey == "" {
		partitionKey = fmt.Sprintf("%s-%d-%d", rec.Topic, rec.Partition, rec.Offset)
	}

	// 标准 DLT headers：原消息定位 + 失败原因 + 重试次数。
	// 与 Spring Kafka 的 x-* / kafka_dlt-* 同思路，名称按本项目风格统一加 dlt: 前缀。
	extra := []sarama.RecordHeader{
		{Key: []byte("dlt:original-topic"), Value: []byte(rec.Topic)},
		{Key: []byte("dlt:original-partition"), Value: []byte(strconv.Itoa(int(rec.Partition)))},
		{Key: []byte("dlt:original-offset"), Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		{Key: []byte("dlt:exception"), Value: []byte(rec.Err)},
		{Key: []byte("dlt:attempts"), Value: []byte(strconv.Itoa(rec.Attempts))},
	}

	// eventType 用原值（若为空则兜底），eventID 复用原始 eventID 便于串联排查。
	eventType := rec.EventType
	if eventType == "" {
		eventType = "dead_letter"
	}

	return w.producer.PublishWithHeaders(
		ctx,
		DltTopic(rec.Topic), // message.read.receipt.DLT
		eventType,
		rec.EventID,
		partitionKey,
		rec.Payload, // 原始消息体原样保留，重放时直接投回原 topic
		extra,
	)
}
