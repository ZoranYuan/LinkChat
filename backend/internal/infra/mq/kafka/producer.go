package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"

	"IM_backend/internal/application/ports/messaging"
)

type Producer struct {
	client *Client
	router *TopicRouter
}

func NewProducer(client *Client, router *TopicRouter) *Producer {
	return &Producer{client: client, router: router}
}

func (p *Producer) newProducerMessage(event messaging.IntegrationEvent) (*sarama.ProducerMessage, error) {
	if event.EventID == "" {
		return nil, errors.New("Kafka 事件缺少 event_id")
	}
	targetTopic, err := p.router.TopicFor(event.Name)
	if err != nil {
		return nil, err
	}
	message := &sarama.ProducerMessage{
		Topic: targetTopic,
		Key:   sarama.StringEncoder(event.PartitionKey),
		Value: sarama.ByteEncoder(event.Payload),
	}
	message.Headers = append(message.Headers, sarama.RecordHeader{
		Key:   []byte("event_id"),
		Value: []byte(event.EventID),
	})
	return message, nil
}

func (p *Producer) Publish(ctx context.Context, event messaging.IntegrationEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message, err := p.newProducerMessage(event)
	if err != nil {
		return err
	}
	_, _, err = p.client.Producer.SendMessage(message)
	return err
}
