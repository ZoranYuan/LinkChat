package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"IM_backend/configs"
	deadletterport "IM_backend/core/application/ports/deadletter"
	eventport "IM_backend/core/application/ports/events"

	"github.com/IBM/sarama"
)

const (
	defaultMaxAttempts   = 3
	defaultRetryInitial  = 200 * time.Millisecond
	defaultRetryMaxDelay = 800 * time.Millisecond
)

var (
	ErrInvalidClient = errors.New("消息队列客户端无效")
	ErrEmptyTopics   = errors.New("消息主题不能为空")
)

// ManagedHandler 可选能力接口。
// 实现者：Handle 内部不调用 Complete，成功返回 nil / 失败返回 error，
//
//	由 consumer 统一「重试 → 死信 → Complete」收口。
//
// 未实现者（攒批的 persist）：保持原语义，Handle 自行管理 Complete；
//
//	consumer 仅在 Handle 返回 error 时退出 claim 触发重投。
//
// 这样做的理由：persist 的 Handle 只做 Submit，真正的 Complete 在异步 flushBatch 里，
// consumer 若在 Handle 返回后立刻 Complete 会导致「未落库先提交 offset」的消息丢失。
type ManagedHandler interface {
	EventHandler
	Managed()
}

type Consumer struct {
	eventHandlers map[string]EventHandler
	topics        []string
	group         sarama.ConsumerGroup
	retryInterval time.Duration
	maxAttempts   int
	retryInitial  time.Duration
	retryMaxDelay time.Duration
	dlqWriter     deadletterport.Writer
}

func NewConsumer(
	client *Client,
	topics []string,
	handlers []EventHandler,
	config configs.KafkaConsumerConfig,
	dlqWriter deadletterport.Writer,
) (*Consumer, error) {
	if client == nil || client.ConsumerGroup == nil {
		return nil, ErrInvalidClient
	}
	if len(topics) == 0 {
		return nil, ErrEmptyTopics
	}
	if len(handlers) == 0 {
		return nil, errors.New("Kafka Consumer 未注册任何事件处理器")
	}
	if config.ConsumeRetryIntervalSecs <= 0 {
		return nil, errors.New("Kafka Consumer 重试配置无效")
	}

	registry := make(map[string]EventHandler, len(handlers))
	for _, h := range handlers {
		if h == nil {
			continue
		}
		registry[h.Topic()] = h
	}

	maxAttempts := config.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	retryInitial := time.Duration(config.RetryInitialDelayMilliseconds) * time.Millisecond
	if retryInitial <= 0 {
		retryInitial = defaultRetryInitial
	}
	retryMaxDelay := time.Duration(config.RetryMaxDelayMilliseconds) * time.Millisecond
	if retryMaxDelay <= 0 {
		retryMaxDelay = defaultRetryMaxDelay
	}
	if retryMaxDelay < retryInitial {
		retryMaxDelay = retryInitial
	}

	return &Consumer{
		group:         client.ConsumerGroup,
		eventHandlers: registry,
		topics:        topics,
		retryInterval: time.Duration(config.ConsumeRetryIntervalSecs) * time.Second,
		maxAttempts:   maxAttempts,
		retryInitial:  retryInitial,
		retryMaxDelay: retryMaxDelay,
		dlqWriter:     dlqWriter,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	for ctx.Err() == nil {
		err := c.group.Consume(ctx, c.topics, c)
		if err == nil {
			continue
		}
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(c.retryInterval):
		}
	}
	return ctx.Err()
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	committer := newOrderedCommitter(session)
	committer.init(claim.Topic(), claim.Partition(), claim.InitialOffset())

	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			topic := message.Topic
			handler, ok := c.eventHandlers[topic]
			if !ok {
				log.Printf("Kafka 未注册 topic，跳过：topic=%s offset=%d", topic, message.Offset)
				committer.complete(message)
				continue
			}

			inFlight.Add(1)
			messageItem := &MessageItem{
				Record: message,
				Complete: func() {
					committer.complete(message)
					inFlight.Done()
				},
			}

			if _, managed := handler.(ManagedHandler); managed {
				// 新路径：consumer 统一收口重试 / 死信 / Complete。
				if err := c.handleManaged(ctx, handler, messageItem); err != nil {
					inFlight.Done() // handleManaged 出错时未 Complete，补 Done 后退出 claim
					return err
				}
				continue
			}

			// 老路径（攒批 handler）：Handle 自行 Complete；失败退出 claim 靠重投。
			if err := handler.Handle(ctx, messageItem); err != nil {
				inFlight.Done()
				return err
			}
		}
	}
}

// handleManaged 统一收口一条「受管」消息：
//
//	成功                → Complete 推进水位
//	暂时故障            → 分区内指数退避重试（不 rebalance、不阻塞其它分区）
//	重试耗尽 / 毒消息   → 写死信 + Complete 跳过（避免卡住 orderedCommitter 水位）
//	死信写入也失败      → return err 退出 claim，靠 rebalance 重投兜底
//
// inFlight.Done 契约：Complete 内部会调一次；仅当本函数返回 error（未 Complete）时，
// 由调用方补调 Done。ctx 取消同理，返回 error 由调用方补 Done。
func (c *Consumer) handleManaged(ctx context.Context, handler EventHandler, item *MessageItem) error {
	var err error
	attempt := 1
	for {
		err = handler.Handle(ctx, item)
		if err == nil {
			item.Complete()
			return nil
		}
		// 毒消息（不可重试）或重试次数耗尽 → 跳出，走死信
		if eventport.IsNonRetryable(err) || attempt >= c.maxAttempts {
			break
		}
		delay := c.retryInitial * time.Duration(1<<(attempt-1))
		if delay > c.retryMaxDelay {
			delay = c.retryMaxDelay
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(delay):
		}
		attempt++
	}

	rec := deadletterport.Record{
		Topic:     item.Record.Topic,
		Partition: item.Record.Partition,
		Offset:    item.Record.Offset,
		EventType: headerValue(item.Record, "event_type"),
		EventID:   headerValue(item.Record, "event_id"),
		Payload:   item.Record.Value,
		Err:       err.Error(),
		Attempts:  attempt,
	}
	if c.dlqWriter != nil {
		if dlqErr := c.dlqWriter.Write(ctx, rec); dlqErr != nil {
			log.Printf("死信写入失败，等待重投：topic=%s partition=%d offset=%d err=%v",
				rec.Topic, rec.Partition, rec.Offset, dlqErr)
			return dlqErr
		}
	} else {
		log.Printf("死信写入器未配置，仅记录不落库：topic=%s partition=%d offset=%d err=%v",
			rec.Topic, rec.Partition, rec.Offset, err)
	}
	item.Complete()
	return nil
}

func headerValue(msg *sarama.ConsumerMessage, key string) string {
	for _, h := range msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}
