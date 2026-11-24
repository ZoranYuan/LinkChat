package kafka

import (
	"context"
	"errors"

	eventport "IM_backend/core/application/ports/events"

	"github.com/IBM/sarama"
)

type Producer struct {
	client *Client
}

func NewProducer(client *Client) *Producer {
	return &Producer{client: client}
}

func (p *Producer) PublishBatch(ctx context.Context, msgs []eventport.PublishMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	batch := make([]*sarama.ProducerMessage, 0, len(msgs))
	for _, m := range msgs {
		if m.EventType == "" || m.EventID == "" {
			return errors.New("Kafka 批量事件缺少 event_type / event_id")
		}
		batch = append(batch, &sarama.ProducerMessage{
			Topic: m.Topic,
			Key:   sarama.StringEncoder(m.PartitionKey),
			Value: sarama.ByteEncoder(m.Payload),
			Headers: []sarama.RecordHeader{
				{Key: []byte("event_type"), Value: []byte(m.EventType)},
				{Key: []byte("event_id"), Value: []byte(m.EventID)},
			},
		})
	}
	return p.client.Producer.SendMessages(batch)
}

// PublishWithHeaders 带自定义 header 投递，供死信序列化异常上下文使用。
// 与 Publish 的区别：允许追加额外 header；原 event_type / event_id 仍强制写入。
func (p *Producer) PublishWithHeaders(
	ctx context.Context, topic string, eventType, eventID, partitionKey string,
	payload []byte, extra []sarama.RecordHeader,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if eventType == "" {
		return errors.New("Kafka 事件缺少 event_type")
	}
	if eventID == "" {
		return errors.New("Kafka 事件缺少 event_id")
	}
	headers := make([]sarama.RecordHeader, 0, len(extra)+2)
	headers = append(headers,
		sarama.RecordHeader{Key: []byte("event_type"), Value: []byte(eventType)},
		sarama.RecordHeader{Key: []byte("event_id"), Value: []byte(eventID)},
	)
	headers = append(headers, extra...)
	message := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(partitionKey),
		Value:   sarama.ByteEncoder(payload),
		Headers: headers,
	}
	_, _, err := p.client.Producer.SendMessage(message)
	return err
}

func (p *Producer) Publish(ctx context.Context, topic string, eventType, eventID, partitionKey string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if eventType == "" {
		return errors.New("Kafka 事件缺少 event_type")
	}
	if eventID == "" {
		return errors.New("Kafka 事件缺少 event_id")
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(partitionKey),
		Value: sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_type"), Value: []byte(eventType)},
			{Key: []byte("event_id"), Value: []byte(eventID)},
		},
	}
	_, _, err := p.client.Producer.SendMessage(message)
	return err
}
