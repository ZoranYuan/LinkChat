package kafka

import (
	"bytes"
	"context"
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"

	"IM_backend/configs"
	"IM_backend/internal/application/ports/messaging"
	mqhandler "IM_backend/internal/infra/mq/handlers"
)

var (
	ErrInvalidClient = errors.New("消息队列客户端无效")
	ErrEmptyTopics   = errors.New("消息主题不能为空")
)

type Consumer struct {
	group          sarama.ConsumerGroup
	consumeBatcher *consumeBatcher
	topicRouter    *TopicRouter
	topics         []string
	retryInterval  time.Duration
	messageHandler *mqhandler.MessageHandler
}

func NewConsumer(
	client *Client,
	topics []string,
	topicRouter *TopicRouter,
	messageHandler *mqhandler.MessageHandler,
	config configs.KafkaConsumerConfig,
) (*Consumer, error) {
	if client == nil || client.ConsumerGroup == nil {
		return nil, ErrInvalidClient
	}

	if len(topics) == 0 {
		return nil, ErrEmptyTopics
	}

	if topicRouter == nil || messageHandler == nil {
		return nil, errors.New("Kafka Consumer 依赖未配置")
	}
	if config.ConsumeRetryIntervalSecs <= 0 {
		return nil, errors.New("Kafka Consumer 重试配置无效")
	}
	if config.BatchSize <= 0 || config.InputQueueSize <= 0 || config.WorkerCount <= 0 || config.WorkerQueueSize <= 0 {
		return nil, errors.New("Kafka Consumer 批处理配置无效")
	}
	consumer := &Consumer{
		group:          client.ConsumerGroup,
		consumeBatcher: newConsumeBatcher(config),
		topics:         topics,
		topicRouter:    topicRouter,
		retryInterval:  time.Duration(config.ConsumeRetryIntervalSecs) * time.Second,
		messageHandler: messageHandler,
	}
	return consumer, nil
}

func eventIDFromHeaders(headers []*sarama.RecordHeader) string {
	for _, header := range headers {
		if string(header.Key) == "event_id" {
			return string(header.Value)
		}
	}
	return ""
}

func (c *Consumer) incomingEvent(message *sarama.ConsumerMessage) (messaging.IncomingEvent, error) {
	name, err := c.topicRouter.EventFor(message.Topic)
	if err != nil {
		return messaging.IncomingEvent{}, err
	}

	return messaging.IncomingEvent{
		EventID: eventIDFromHeaders(message.Headers),
		Name:    name,
		Key:     bytes.Clone(message.Key),
		Payload: bytes.Clone(message.Value),
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	c.consumeBatcher.Start(ctx, c.processBatch)
	defer c.consumeBatcher.Close()

	// 外部 ctx 没有被取消，在消费者退出后，开始重新加入
	for ctx.Err() == nil {
		// 加入消费者组
		err := c.group.Consume(ctx, c.topics, c)

		if err == nil {
			continue
		}

		// 消费组已经关闭了，无法等待重试
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return err
		}

		log.Printf("Kafka Consumer 运行失败，稍后重试：%v", err)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(c.retryInterval):
		}
	}

	return ctx.Err()
}

func (c *Consumer) processBatch(ctx context.Context, batch *consumeBatch) {
	events := make([]messaging.IncomingEvent, len(batch.items))
	for i, item := range batch.items {
		event, err := c.incomingEvent(item.message)
		if err != nil {
			for _, item := range batch.items {
				item.failed(err)
			}
			return
		}
		events[i] = event
	}

	successMessages, failedMessages, err := c.messageHandler.HandleSendMessages(ctx, batch.conversationID, events)
	if err != nil {
		for _, item := range batch.items {
			item.failed(err)
		}
		return
	}

	log.Printf("Kafka 消息批次处理完成：conversation=%s persisted=%d inbox_skipped=%d", batch.conversationID, len(successMessages), len(failedMessages))
	for _, item := range batch.items {
		item.completed()
	}
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	err := c.group.Close()
	c.consumeBatcher.Close()
	return err
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	messages := claim.Messages()

	for {
		select {
		case <-ctx.Done():
			return nil

		case message, ok := <-messages:
			if !ok {
				return nil
			}
			item := &consumeBatchItem{
				message:   message,
				completed: func() { session.MarkMessage(message, "") },
				failed: func(err error) {
					log.Printf("Kafka 消息批次处理失败：topic=%s partition=%d offset=%d err=%v", message.Topic, message.Partition, message.Offset, err)
				},
			}

			if err := c.consumeBatcher.Submit(ctx, item); err != nil {
				// 目前的 Submit 退出只能是 ctx.Done 了，也就是服务退出或者发生了重平衡
				return nil
			}
		}
	}
}
