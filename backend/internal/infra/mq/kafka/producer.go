package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
)

type Producer struct {
	client *Client
	topic  string
}

func NewProducer(client *Client, topic string) *Producer {
	return &Producer{client: client, topic: topic}
}

func (p *Producer) Publish(ctx context.Context, eventID, eventType, partitionKey string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if eventID == "" {
		return errors.New("Kafka 事件缺少 event_id")
	}
	if eventType == "" {
		return errors.New("Kafka 事件缺少 event_type")
	}
	message := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(partitionKey),
		Value: sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_id"), Value: []byte(eventID)},
			{Key: []byte("event_type"), Value: []byte(eventType)},
		},
	}
	_, _, err := p.client.Producer.SendMessage(message)
	return err
}
