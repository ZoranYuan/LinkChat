package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"IM_backend/configs"
	mqhandler "IM_backend/internal/infra/mq/handlers"
)

var (
	ErrInvalidClient = errors.New("消息队列客户端无效")
	ErrEmptyTopics   = errors.New("消息主题不能为空")
)

type Consumer struct {
	group          sarama.ConsumerGroup
	consumeBatcher *consumeBatcher
	topics         []string
	retryInterval  time.Duration
	retryInitial   time.Duration
	retryMax       time.Duration
	messageHandler *mqhandler.MessageHandler
}

func NewConsumer(
	client *Client,
	topics []string,
	messageHandler *mqhandler.MessageHandler,
	config configs.KafkaConsumerConfig,
) (*Consumer, error) {
	if client == nil || client.ConsumerGroup == nil {
		return nil, ErrInvalidClient
	}

	if len(topics) == 0 {
		return nil, ErrEmptyTopics
	}

	if messageHandler == nil {
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
		retryInterval:  time.Duration(config.ConsumeRetryIntervalSecs) * time.Second,
		retryInitial:   time.Duration(config.RetryInitialDelayMilliseconds) * time.Millisecond,
		retryMax:       time.Duration(config.RetryMaxDelayMilliseconds) * time.Millisecond,
		messageHandler: messageHandler,
	}
	return consumer, nil
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
	messages := make([]*sarama.ConsumerMessage, len(batch.items))
	for i, item := range batch.items {
		messages[i] = item.message
	}

	delay := c.retryInitial
	for ctx.Err() == nil {
		persisted, skipped, err := c.messageHandler.HandleSendMessages(ctx, batch.conversationID, messages)
		if err == nil {
			log.Printf("Kafka 消息批次处理完成：conversation=%s persisted=%d skipped=%d", batch.conversationID, persisted, skipped)
			for _, item := range batch.items {
				item.completed()
			}
			return
		}
		for _, item := range batch.items {
			item.failed(err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, c.retryMax)
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
	var completionMu sync.Mutex
	var pending []*sarama.ConsumerMessage
	completed := make(map[int64]struct{})
	// 消息并行处理，offset 只推进到按接收顺序连续完成的位置。
	complete := func(message *sarama.ConsumerMessage) {
		completionMu.Lock()
		defer completionMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		completed[message.Offset] = struct{}{}
		var last *sarama.ConsumerMessage
		for len(pending) > 0 {
			if _, done := completed[pending[0].Offset]; !done {
				break
			}
			last = pending[0]
			delete(completed, last.Offset)
			pending[0] = nil
			pending = pending[1:]
		}
		if last != nil {
			session.MarkMessage(last, "")
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case message, ok := <-messages:
			if !ok {
				return nil
			}
			completionMu.Lock()
			pending = append(pending, message)
			completionMu.Unlock()
			item := &consumeBatchItem{
				message:   message,
				completed: func() { complete(message) },
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
