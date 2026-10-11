package kafka

import (
	configs "IM_backend/configs"
	eventbus "IM_backend/internal/application/ports/eventbus"
	inboxport "IM_backend/internal/application/ports/inbox"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	"IM_backend/internal/shared/diagnostics"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
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
	handlers            map[string]eventbus.Handler
	inbox               inboxport.InboxRepository
	txManager           txmanager.TxManager
	leaseStaleAfter     time.Duration
	maxRetries          int
	deadLetterPublisher eventbus.Publisher
	deadLetterSuffix    string
}

const (
	consumerRetryInitialDelay = 200 * time.Millisecond
	consumerRetryMaxDelay     = 800 * time.Millisecond
)

// claimRetryInterval 根据 Inbox 租约时间推导抢占失败后的检查间隔，避免额外增加配置项。
func (c *Consumer) claimRetryInterval() time.Duration {
	if c == nil || c.leaseStaleAfter <= 0 {
		return consumerRetryMaxDelay
	}
	interval := c.leaseStaleAfter / 10
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	if interval > consumerRetryMaxDelay {
		interval = consumerRetryMaxDelay
	}
	return interval
}

type inboxClaim struct {
	claimed    bool
	lockToken  string
	retryCount int
}

func (c *Consumer) claim(ctx context.Context, message eventbus.IncomingEvent) (inboxClaim, error) {
	if c == nil {
		return inboxClaim{}, ErrHandlerNotFound
	}

	if _, ok := c.handlers[message.Name]; !ok {
		return inboxClaim{}, eventbus.NonRetryable(ErrHandlerNotFound)
	}
	if message.EventID == "" {
		return inboxClaim{}, eventbus.NonRetryable(inboxport.ErrEventIDRequired)
	}
	if c.inbox == nil || c.txManager == nil {
		return inboxClaim{}, errors.New("Inbox 依赖未配置")
	}
	claim := inboxClaim{}
	now := time.Now()
	err := c.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		claim.claimed, claim.lockToken, claim.retryCount, err = c.inbox.WithTx(tx).TryClaim(
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

func (c *Consumer) execute(ctx context.Context, message eventbus.IncomingEvent) error {
	if c == nil {
		return ErrHandlerNotFound
	}

	handler, ok := c.handlers[message.Name]
	if !ok {
		return eventbus.NonRetryable(ErrHandlerNotFound)
	}
	return handler.Handle(ctx, message)
}

func (c *Consumer) publishDeadLetter(ctx context.Context, message eventbus.IncomingEvent) error {
	if c == nil || c.deadLetterPublisher == nil {
		return errors.New("死信发布器未配置")
	}
	eventID := message.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	}

	if err := c.deadLetterPublisher.Publish(
		ctx,
		eventbus.IntegrationEvent{
			EventID:      eventID,
			Name:         message.Name + c.deadLetterSuffix,
			PartitionKey: string(message.Key),
			Payload:      cloneBytes(message.Payload),
		},
	); err != nil {
		return fmt.Errorf("发布死信消息失败：%w", err)
	}

	return nil
}

func (c *Consumer) handle(ctx context.Context, message eventbus.IncomingEvent) error {
	startedAt := time.Now()
	claimStartedAt := time.Now()
	inboxState, err := c.claim(ctx, message)
	claimDuration := time.Since(claimStartedAt)
	if err != nil {
		diagnostics.Logf("stage=kafka_consumer event_id=%s event_type=%s claim_us=%d total_us=%d outcome=claim_error error=%q",
			message.EventID, message.Name, claimDuration.Microseconds(), time.Since(startedAt).Microseconds(), err.Error())
		if eventbus.IsNonRetryable(err) {
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
		diagnostics.Logf("stage=kafka_consumer event_id=%s event_type=%s claim_us=%d total_us=%d outcome=duplicate_or_terminal",
			message.EventID, message.Name, claimDuration.Microseconds(), time.Since(startedAt).Microseconds())
		return nil
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	stopRefresh := c.refreshLease(workCtx, cancel, []string{message.EventID}, inboxState.lockToken)
	defer cancel(nil)
	defer stopRefresh()
	handlerStartedAt := time.Now()
	handlerErr := c.execute(workCtx, message)
	handlerDuration := time.Since(handlerStartedAt)
	stopRefresh()
	if workCtx.Err() != nil {
		return context.Cause(workCtx)
	}
	if handlerErr != nil {
		diagnostics.Logf("stage=kafka_consumer event_id=%s event_type=%s claim_us=%d handler_us=%d total_us=%d outcome=handler_error error=%q",
			message.EventID, message.Name, claimDuration.Microseconds(), handlerDuration.Microseconds(), time.Since(startedAt).Microseconds(), handlerErr.Error())
		if eventbus.IsNonRetryable(handlerErr) ||
			(c.maxRetries > 0 && inboxState.retryCount >= c.maxRetries) {
			if err := c.publishDeadLetter(ctx, message); err != nil {
				return err
			}
			if err := c.markDead(ctx, message, inboxState.lockToken, handlerErr.Error()); err != nil {
				return fmt.Errorf("标记 Inbox 死信状态失败：%w", err)
			}
			log.Printf(
				"消息进入死信队列：event_id=%s，retry_count=%d，error=%v",
				message.EventID,
				inboxState.retryCount,
				handlerErr,
			)
			return nil
		}

		if err := c.markRetry(ctx, message, inboxState.lockToken, handlerErr.Error()); err != nil {
			return fmt.Errorf("释放 Inbox 重试租约失败：%w", err)
		}

		log.Printf(
			"消息处理失败，释放 Inbox 租约并准备重试：event_id=%s，error=%v",
			message.EventID,
			handlerErr,
		)
		return handlerErr
	}

	markCompletedStartedAt := time.Now()
	if err := c.markCompleted(ctx, message, inboxState.lockToken); err != nil {
		diagnostics.Logf("stage=kafka_consumer event_id=%s event_type=%s claim_us=%d handler_us=%d mark_completed_us=%d total_us=%d outcome=complete_error error=%q",
			message.EventID, message.Name, claimDuration.Microseconds(), handlerDuration.Microseconds(), time.Since(markCompletedStartedAt).Microseconds(), time.Since(startedAt).Microseconds(), err.Error())
		return fmt.Errorf("标记 Inbox 完成状态失败：%w", err)
	}
	diagnostics.Logf("stage=kafka_consumer event_id=%s event_type=%s claim_us=%d handler_us=%d mark_completed_us=%d total_us=%d outcome=completed",
		message.EventID, message.Name, claimDuration.Microseconds(), handlerDuration.Microseconds(), time.Since(markCompletedStartedAt).Microseconds(), time.Since(startedAt).Microseconds())

	return nil
}

func (c *Consumer) markRetry(ctx context.Context, message eventbus.IncomingEvent, lockToken, lastError string) error {
	if c == nil || c.inbox == nil || message.EventID == "" || lockToken == "" {
		return errors.New("Inbox 重试状态参数无效")
	}
	return c.inbox.MarkRetry(ctx, message.EventID, lockToken, lastError)
}

func (c *Consumer) markDead(ctx context.Context, message eventbus.IncomingEvent, lockToken, lastError string) error {
	if c == nil || c.inbox == nil || message.EventID == "" || lockToken == "" {
		return errors.New("Inbox 死信状态参数无效")
	}
	return c.inbox.MarkDead(ctx, message.EventID, lockToken, lastError, time.Now())
}

func (c *Consumer) markCompleted(ctx context.Context, message eventbus.IncomingEvent, lockToken string) error {
	if c == nil || c.inbox == nil || message.EventID == "" || lockToken == "" {
		return errors.New("Inbox 完成状态参数无效")
	}
	return c.inbox.MarkCompleted(ctx, message.EventID, lockToken, time.Now())
}

type saramaAdapter struct {
	consumer    *Consumer
	topicRouter *TopicRouter
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}

	copied := make([]byte, len(data))
	copy(copied, data)

	return copied
}

func eventIDFromHeaders(headers []*sarama.RecordHeader) string {
	for _, header := range headers {
		if header != nil && string(header.Key) == "event_id" {
			return string(header.Value)
		}
	}
	return ""
}

func diagnosticPublishAge(headers []*sarama.RecordHeader, now time.Time) time.Duration {
	for _, header := range headers {
		if header == nil || string(header.Key) != diagnosticPublishedAtHeader {
			continue
		}
		publishedAt, err := strconv.ParseInt(string(header.Value), 10, 64)
		if err != nil || publishedAt <= 0 {
			return 0
		}
		return now.Sub(time.Unix(0, publishedAt))
	}
	return 0
}

func (h saramaAdapter) incomingEvent(claim sarama.ConsumerGroupClaim, message *sarama.ConsumerMessage) (eventbus.IncomingEvent, error) {
	name, err := h.topicRouter.EventFor(message.Topic)
	if err != nil {
		return eventbus.IncomingEvent{}, err
	}
	event := eventbus.IncomingEvent{
		EventID: eventIDFromHeaders(message.Headers),
		Name:    name,
		Key:     cloneBytes(message.Key),
		Payload: cloneBytes(message.Value),
	}
	behind := max(claim.HighWaterMarkOffset()-message.Offset-1, 0)
	diagnostics.Logf("stage=kafka_consumer_received event_id=%s topic=%s partition=%d offset=%d partition_records_behind=%d published_age_us=%d",
		event.EventID, message.Topic, message.Partition, message.Offset, behind,
		diagnosticPublishAge(message.Headers, time.Now()).Microseconds())
	return event, nil
}

func (saramaAdapter) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (saramaAdapter) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// 当前分区内重试当前消息，避免后续 offset 越过尚未完成的消息；不同分区仍并行消费。
func (c *Consumer) process(ctx context.Context, event eventbus.IncomingEvent) error {
	delay := consumerRetryInitialDelay
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
			delay = min(delay*2, consumerRetryMaxDelay)
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
	handlers map[string]eventbus.Handler,
	inbox inboxport.InboxRepository,
	txManager txmanager.TxManager,
	config configs.KafkaConsumerConfig,
	deadLetterPublisher eventbus.Publisher,
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
	if config.MaxRetries <= 0 ||
		config.LeaseStaleAfterSecs <= 0 ||
		config.ConsumeRetryIntervalSecs <= 0 {
		return nil, errors.New("Kafka Consumer 重试配置无效")
	}
	if config.DeadLetterSuffix == "" {
		return nil, errors.New("Kafka DLQ 后缀未配置")
	}

	registered := make(map[string]eventbus.Handler, len(handlers))
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
		handlers:            registered,
		inbox:               inbox,
		txManager:           txManager,
		leaseStaleAfter:     time.Duration(config.LeaseStaleAfterSecs) * time.Second,
		maxRetries:          config.MaxRetries,
		deadLetterPublisher: deadLetterPublisher,
		deadLetterSuffix:    config.DeadLetterSuffix,
	}
	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	handler := saramaAdapter{
		consumer:    c,
		topicRouter: c.topicRouter,
	}
	for ctx.Err() == nil {
		if err := c.group.Consume(ctx, c.topics, handler); err != nil {
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

func (h saramaAdapter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx, cancel := context.WithCancel(session.Context())
	defer cancel()
	source := claim.Messages()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-source:
			if !ok {
				return nil
			}
			event, err := h.incomingEvent(claim, message)
			if err != nil {
				return err
			}
			// ConsumeClaim 对每个 Kafka 分区独立运行，因此这里串行处理本分区消息即可；其他分区仍由各自的 ConsumeClaim 并行处理。
			if err := h.consumer.process(ctx, event); err != nil {
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
	if interval <= 0 {
		interval = time.Second
	}
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
