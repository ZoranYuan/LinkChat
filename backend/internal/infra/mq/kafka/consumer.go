package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"IM_backend/configs"
	inboxport "IM_backend/internal/application/ports/inbox"
	"IM_backend/internal/application/ports/messaging"
	"IM_backend/internal/application/ports/transaction"
)

var (
	ErrHandlerNotFound = errors.New("消息处理器不存在")
	ErrInvalidClient   = errors.New("消息队列客户端无效")
	ErrEmptyTopics     = errors.New("消息主题不能为空")
)

type Consumer struct {
	group               sarama.ConsumerGroup
	topicRouter         *TopicRouter
	topics              []string
	retryInterval       time.Duration
	retryInitialDelay   time.Duration
	retryMaxDelay       time.Duration
	handlers            map[string]messaging.Handler
	inbox               inboxport.InboxRepository
	txManager           transaction.Manager
	leaseStaleAfter     time.Duration
	maxAttempts         int
	deadLetterPublisher messaging.Publisher
	deadLetterSuffix    string
}

// claimRetryInterval 根据已经校验过的 Inbox 租约推导抢占检查间隔。
func (c *Consumer) claimRetryInterval() time.Duration {
	return min(c.leaseStaleAfter/10, c.retryMaxDelay)
}

type inboxClaim struct {
	claimed      bool
	lockToken    string
	attemptCount int
}

func (c *Consumer) claim(ctx context.Context, message messaging.IncomingEvent) (inboxClaim, error) {
	if c == nil {
		return inboxClaim{}, ErrHandlerNotFound
	}

	if _, ok := c.handlers[message.Name]; !ok {
		return inboxClaim{}, messaging.NonRetryable(ErrHandlerNotFound)
	}
	if message.EventID == "" {
		return inboxClaim{}, messaging.NonRetryable(inboxport.ErrEventIDRequired)
	}
	if c.inbox == nil || c.txManager == nil {
		return inboxClaim{}, errors.New("Inbox 依赖未配置")
	}
	claim := inboxClaim{}
	now := time.Now()
	err := c.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		claim.claimed, claim.lockToken, claim.attemptCount, err = c.inbox.WithTx(tx).TryClaim(
			ctx,
			message.EventID,
			string(message.Name),
			now,
			now.Add(-c.leaseStaleAfter),
		)
		return err
	})
	if err != nil {
		return claim, err
	}
	return claim, nil
}

func (c *Consumer) publishDeadLetter(ctx context.Context, message messaging.IncomingEvent) error {
	if c == nil || c.deadLetterPublisher == nil {
		return errors.New("死信发布器未配置")
	}
	eventID := message.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	}

	if err := c.deadLetterPublisher.Publish(
		ctx,
		messaging.IntegrationEvent{
			EventID:      eventID,
			Name:         message.Name + c.deadLetterSuffix,
			PartitionKey: string(message.Key),
			Payload:      bytes.Clone(message.Payload),
		},
	); err != nil {
		return fmt.Errorf("发布死信消息失败：%w", err)
	}

	return nil
}

func (c *Consumer) handle(ctx context.Context, message messaging.IncomingEvent) error {
	inboxState, err := c.claim(ctx, message)
	if err != nil {
		if messaging.IsNonRetryable(err) {
			if publishErr := c.publishDeadLetter(ctx, message); publishErr != nil {
				return publishErr
			}
			return nil
		}

		if errors.Is(err, inboxport.ErrEventInProgress) {
			return err
		}
		log.Printf("消息抢占失败：event_id=%s，error=%v", message.EventID, err)
		return err
	}

	// 已经 completed 或 dead，不需要重复执行业务逻辑。
	if !inboxState.claimed {
		return nil
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	stopRefresh := c.refreshLease(workCtx, cancel, []string{message.EventID}, inboxState.lockToken)
	defer cancel(nil)
	defer stopRefresh()
	handlerErr := c.handlers[message.Name].Handle(workCtx, message)
	stopRefresh()
	if workCtx.Err() != nil {
		return context.Cause(workCtx)
	}
	if handlerErr != nil {
		if messaging.IsNonRetryable(handlerErr) ||
			(c.maxAttempts > 0 && inboxState.attemptCount >= c.maxAttempts) {
			if err := c.publishDeadLetter(ctx, message); err != nil {
				return err
			}
			if err := c.inbox.MarkDead(ctx, message.EventID, inboxState.lockToken, handlerErr.Error(), time.Now()); err != nil {
				return fmt.Errorf("标记 Inbox 死信状态失败：%w", err)
			}
			log.Printf(
				"消息进入死信队列：event_id=%s，attempt_count=%d，error=%v",
				message.EventID,
				inboxState.attemptCount,
				handlerErr,
			)
			return nil
		}

		if err := c.inbox.MarkRetry(ctx, message.EventID, inboxState.lockToken, handlerErr.Error()); err != nil {
			return fmt.Errorf("释放 Inbox 重试租约失败：%w", err)
		}

		log.Printf(
			"消息处理失败，释放 Inbox 租约并准备重试：event_id=%s，error=%v",
			message.EventID,
			handlerErr,
		)
		return handlerErr
	}

	if err := c.inbox.MarkCompleted(ctx, message.EventID, inboxState.lockToken, time.Now()); err != nil {
		return fmt.Errorf("标记 Inbox 完成状态失败：%w", err)
	}

	return nil
}

func eventIDFromHeaders(headers []*sarama.RecordHeader) string {
	for _, header := range headers {
		if header != nil && string(header.Key) == "event_id" {
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
	event := messaging.IncomingEvent{
		EventID: eventIDFromHeaders(message.Headers),
		Name:    name,
		Key:     bytes.Clone(message.Key),
		Payload: bytes.Clone(message.Value),
	}
	return event, nil
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// 当前分区内重试当前消息，避免后续 offset 越过尚未完成的消息；不同分区仍并行消费。
func (c *Consumer) process(ctx context.Context, event messaging.IncomingEvent) error {
	delay := c.retryInitialDelay
	for ctx.Err() == nil {
		err := c.handle(ctx, event)
		if err == nil {
			return nil
		}
		retryDelay := delay
		if errors.Is(err, inboxport.ErrEventInProgress) {
			retryDelay = c.claimRetryInterval()
		} else {
			log.Printf("Kafka Consumer 消息处理失败，准备重试：event_id=%s，error=%v", event.EventID, err)
			delay = min(delay*2, c.retryMaxDelay)
		}
		if err := waitForInboxRetry(ctx, retryDelay); err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}

func waitForInboxRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func normalizeTopics(topics []string) []string {
	result := make([]string, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))

	for _, topic := range topics {
		if topic == "" {
			continue
		}

		if _, exists := seen[topic]; exists {
			continue
		}

		seen[topic] = struct{}{}
		result = append(result, topic)
	}

	return result
}

func NewConsumer(
	client *Client,
	topics []string,
	topicRouter *TopicRouter,
	handlers map[string]messaging.Handler,
	inbox inboxport.InboxRepository,
	txManager transaction.Manager,
	config configs.KafkaConsumerConfig,
	deadLetterPublisher messaging.Publisher,
) (*Consumer, error) {
	if client == nil || client.ConsumerGroup == nil {
		return nil, ErrInvalidClient
	}

	validTopics := normalizeTopics(topics)
	if len(validTopics) == 0 {
		return nil, ErrEmptyTopics
	}

	if topicRouter == nil || inbox == nil || txManager == nil || deadLetterPublisher == nil {
		return nil, errors.New("Kafka Consumer 依赖未配置")
	}
	if config.MaxAttempts <= 0 ||
		config.LeaseStaleAfterSecs <= 0 ||
		config.ConsumeRetryIntervalSecs <= 0 {
		return nil, errors.New("Kafka Consumer 重试配置无效")
	}
	if config.DeadLetterSuffix == "" {
		return nil, errors.New("Kafka DLQ 后缀未配置")
	}

	registered := make(map[string]messaging.Handler, len(handlers))
	for eventName, handler := range handlers {
		if eventName != "" && handler != nil {
			registered[eventName] = handler
		}
	}
	if len(registered) == 0 {
		return nil, errors.New("Kafka Consumer 未配置消息处理器")
	}
	consumer := &Consumer{
		group:               client.ConsumerGroup,
		topics:              validTopics,
		topicRouter:         topicRouter,
		retryInterval:       time.Duration(config.ConsumeRetryIntervalSecs) * time.Second,
		retryInitialDelay:   time.Duration(config.RetryInitialDelayMilliseconds) * time.Millisecond,
		retryMaxDelay:       time.Duration(config.RetryMaxDelayMilliseconds) * time.Millisecond,
		handlers:            registered,
		inbox:               inbox,
		txManager:           txManager,
		leaseStaleAfter:     time.Duration(config.LeaseStaleAfterSecs) * time.Second,
		maxAttempts:         config.MaxAttempts,
		deadLetterPublisher: deadLetterPublisher,
		deadLetterSuffix:    config.DeadLetterSuffix,
	}
	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for ctx.Err() == nil {
		if err := c.group.Consume(ctx, c.topics, c); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return err
			}
			log.Printf(
				"消费消息主题失败：主题=%v，重试间隔=%s，错误=%v",
				c.topics,
				c.retryInterval,
				err,
			)

			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-time.After(c.retryInterval):
			}
		}
	}
	// 返回上下文取消的具体原因
	return context.Cause(ctx)
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}

	return c.group.Close()
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	source := claim.Messages()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-source:
			if !ok {
				return nil
			}
			event, err := c.incomingEvent(message)
			if err != nil {
				return err
			}
			// ConsumeClaim 对每个 Kafka 分区独立运行，因此这里串行处理本分区消息即可；其他分区仍由各自的 ConsumeClaim 并行处理。
			if err := c.process(ctx, event); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			session.MarkMessage(message, "")
		}
	}
}

func (c *Consumer) refreshLease(ctx context.Context, cancel context.CancelCauseFunc, ids []string, token string) func() {
	interval := c.leaseStaleAfter / 3
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				renewCtx, finish := context.WithTimeout(ctx, interval)
				err := c.inbox.RenewBatch(renewCtx, ids, token, time.Now())
				finish()
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(stop); <-done }) }
}
