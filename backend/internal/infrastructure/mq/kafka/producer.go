package kafka

import (
	"IM_backend/configs"
	eventbus "IM_backend/internal/application/ports/eventbus"
	outboxport "IM_backend/internal/application/ports/outbox"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	"IM_backend/internal/shared/diagnostics"
	"context"
	"errors"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	client     *Client
	router     *TopicRouter
	txManager  txmanager.TxManager
	repository outboxport.OutboxRepository
	options    configs.KafkaProducerConfig
	wake       chan time.Time
}

const diagnosticPublishedAtHeader = "im_diag_published_at_unix_nano"

type producerMessageMetadata struct {
	EventID    string
	LockToken  string
	RetryCount int
	EventType  string
	CreatedAt  time.Time
}

func NewProducer(
	client *Client,
	router *TopicRouter,
	txManager txmanager.TxManager,
	repository outboxport.OutboxRepository,
	options configs.KafkaProducerConfig,
) *Producer {
	return &Producer{
		client:     client,
		router:     router,
		txManager:  txManager,
		repository: repository,
		options:    normalizeProducerConfig(options),
		wake:       make(chan time.Time, 1),
	}
}

func normalizeProducerConfig(options configs.KafkaProducerConfig) configs.KafkaProducerConfig {
	if options.ClaimBatchSize <= 0 {
		options.ClaimBatchSize = 100
	}
	if options.PollIntervalSeconds <= 0 {
		options.PollIntervalSeconds = 2
	}
	if options.StaleAfterSeconds <= 0 {
		options.StaleAfterSeconds = 30
	}
	if options.BaseRetryWaitSeconds <= 0 {
		options.BaseRetryWaitSeconds = 2
	}
	if options.MaxRetries <= 0 {
		options.MaxRetries = 10
	}
	return options
}

// Produce 将单个事件交给 Kafka；Outbox 领取和状态维护由 Producer 自身负责。
func (p *Producer) Produce(ctx context.Context, event eventbus.IntegrationEvent) error {
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

func (p *Producer) newProducerMessage(event eventbus.IntegrationEvent) (*sarama.ProducerMessage, error) {
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
	if diagnostics.Enabled() {
		message.Headers = append(message.Headers, sarama.RecordHeader{
			Key:   []byte(diagnosticPublishedAtHeader),
			Value: []byte(strconv.FormatInt(time.Now().UnixNano(), 10)),
		})
	}
	message.Headers = append(message.Headers, sarama.RecordHeader{
		Key:   []byte("event_id"),
		Value: []byte(event.EventID),
	})
	return message, nil
}

// Publish 让 Producer 同时可作为 Consumer 的死信发布器。
func (p *Producer) Publish(ctx context.Context, event eventbus.IntegrationEvent) error {
	return p.Produce(ctx, event)
}

// Notify 在业务事务提交后唤醒 Producer，容量为 1 的 channel 会合并密集通知。
func (p *Producer) Notify() {
	if p == nil {
		return
	}
	select {
	case p.wake <- time.Now():
	default:
	}
}

// Start 持续领取 Outbox 批次，直接批量投递并更新每条记录的状态。
func (p *Producer) Start(ctx context.Context) {
	log.Printf("Kafka Producer 已启动：claimBatchSize=%d", p.options.ClaimBatchSize)

	ticker := time.NewTicker(time.Duration(p.options.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	drain := func(source string, triggeredAt time.Time) {
		drainStartedAt := time.Now()
		diagnostics.Logf("stage=producer_wakeup source=%s signal_wait_us=%d", source, drainStartedAt.Sub(triggeredAt).Microseconds())
		firstClaim := true
		for ctx.Err() == nil {
			processed, err := p.producePendingOnce(ctx, source, drainStartedAt, firstClaim)
			firstClaim = false
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Kafka Producer 处理待投递批次失败：%v", err)
				}
				return
			}
			if !processed {
				return
			}
		}
	}
	drain("startup", time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-ticker.C:
			drain("poll", tickAt)
		case notifiedAt := <-p.wake:
			drain("notify", notifiedAt)
		}
	}
}

func (p *Producer) producePendingOnce(ctx context.Context, trigger string, drainStartedAt time.Time, firstClaim bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if p.txManager == nil || p.repository == nil || p.client == nil || p.client.Producer == nil {
		return false, errors.New("Kafka Producer 依赖未配置")
	}
	var batch []*outboxport.Entry
	now := time.Now()
	claimStartedAt := time.Now()
	err := p.txManager.WithinTransaction(ctx, func(tx any) error {
		items, err := p.repository.WithTx(tx).ClaimPending(
			ctx,
			now,
			now.Add(-time.Duration(p.options.StaleAfterSeconds)*time.Second),
			p.options.ClaimBatchSize,
		)
		if err != nil {
			return err
		}
		batch = items
		return nil
	})
	claimDuration := time.Since(claimStartedAt)
	drainElapsed := time.Since(drainStartedAt)
	if err != nil {
		diagnostics.Logf("stage=producer_claim_result source=%s first_in_drain=%t outcome=error drain_elapsed_us=%d claim_us=%d error=%q", trigger, firstClaim, drainElapsed.Microseconds(), claimDuration.Microseconds(), err.Error())
		return false, err
	}
	diagnostics.Logf("stage=producer_claim_result source=%s rows=%d first_in_drain=%t drain_elapsed_us=%d claim_us=%d outcome=ok", trigger, len(batch), firstClaim, drainElapsed.Microseconds(), claimDuration.Microseconds())
	if len(batch) == 0 {
		return false, nil
	}
	// 领取事务已经提交；等待 Kafka 确认期间不持有数据库锁。
	return true, p.dispatchBatch(ctx, batch)
}

func (p *Producer) dispatchBatch(ctx context.Context, items []*outboxport.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	messages := make([]*sarama.ProducerMessage, 0, len(items))
	sent := make([]outboxport.Lease, 0)
	retry := make([]outboxport.RetryUpdate, 0)
	dead := make([]outboxport.DeadUpdate, 0)
	failedAt := time.Now()
	collectFailure := func(metadata producerMessageMetadata, lastError string) {
		lease := outboxport.Lease{EventID: metadata.EventID, LockToken: metadata.LockToken}
		if metadata.RetryCount >= p.options.MaxRetries {
			dead = append(dead, outboxport.DeadUpdate{Lease: lease, LastError: lastError})
			return
		}
		retryDelay := min(time.Duration(float64(time.Duration(p.options.BaseRetryWaitSeconds)*time.Second)*math.Pow(2, float64(metadata.RetryCount))), 5*time.Minute)
		retry = append(retry, outboxport.RetryUpdate{Lease: lease, NextRetryAt: failedAt.Add(retryDelay), LastError: lastError})
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		metadata := producerMessageMetadata{
			EventID: item.EventID, LockToken: item.LockToken, RetryCount: item.RetryCount,
			EventType: item.EventType, CreatedAt: item.CreatedAt,
		}
		message, err := p.newProducerMessage(eventbus.IntegrationEvent{
			EventID: item.EventID, Name: item.EventType, PartitionKey: item.PartitionKey, Payload: item.Payload,
		})
		if err != nil {
			log.Printf("构造 Kafka 消息失败：event_id=%s err=%v", metadata.EventID, err)
			collectFailure(metadata, err.Error())
			continue
		}
		message.Metadata = metadata
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return p.markBatchResults(ctx, sent, retry, dead)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	publishStartedAt := time.Now()
	batchErrs := p.client.Producer.SendMessages(messages)
	publishDuration := time.Since(publishStartedAt)
	failedAt = time.Now()
	var publishErrs sarama.ProducerErrors
	partialFailure := errors.As(batchErrs, &publishErrs)
	failedMessages := make(map[*sarama.ProducerMessage]error, len(publishErrs))
	for _, failure := range publishErrs {
		failedMessages[failure.Msg] = failure.Err
	}
	diagnostics.Logf("stage=producer_publish_batch size=%d publish_us=%d outcome=%s", len(messages), publishDuration.Microseconds(), producerOutcome(batchErrs))

	// 批量投递后需要逐条分析哪个消息没有投递成功
	for _, message := range messages {
		metadata := message.Metadata.(producerMessageMetadata)

		publishErr := batchErrs
		if partialFailure {
			publishErr = failedMessages[message]
		}
		createdAge := time.Duration(0)
		if !metadata.CreatedAt.IsZero() {
			createdAge = publishStartedAt.Sub(metadata.CreatedAt)
		}
		if publishErr != nil {
			diagnostics.Logf("stage=producer_publish event_id=%s event_type=%s created_age_ms=%d publish_us=%d outcome=failed_pending_update error=%q", metadata.EventID, metadata.EventType, createdAge.Milliseconds(), publishDuration.Microseconds(), publishErr.Error())
			collectFailure(metadata, publishErr.Error())
			continue
		}
		diagnostics.Logf("stage=producer_publish event_id=%s event_type=%s created_age_ms=%d publish_us=%d outcome=published_pending_mark_sent", metadata.EventID, metadata.EventType, createdAge.Milliseconds(), publishDuration.Microseconds())
		sent = append(sent, outboxport.Lease{EventID: metadata.EventID, LockToken: metadata.LockToken})
	}
	return p.markBatchResults(ctx, sent, retry, dead)
}

func (p *Producer) markBatchResults(ctx context.Context, sent []outboxport.Lease, retry []outboxport.RetryUpdate, dead []outboxport.DeadUpdate) error {
	var result error
	if len(sent) > 0 {
		startedAt := time.Now()
		err := p.repository.MarkSentBatch(ctx, sent, startedAt)
		diagnostics.Logf("stage=producer_mark_sent_batch size=%d update_us=%d error=%v", len(sent), time.Since(startedAt).Microseconds(), err)
		result = errors.Join(result, err)
	}
	if len(retry) > 0 {
		startedAt := time.Now()
		err := p.repository.MarkRetryBatch(ctx, retry)
		diagnostics.Logf("stage=producer_mark_retry_batch size=%d update_us=%d error=%v", len(retry), time.Since(startedAt).Microseconds(), err)
		result = errors.Join(result, err)
	}
	if len(dead) > 0 {
		startedAt := time.Now()
		err := p.repository.MarkDeadBatch(ctx, dead)
		diagnostics.Logf("stage=producer_mark_dead_batch size=%d update_us=%d error=%v", len(dead), time.Since(startedAt).Microseconds(), err)
		result = errors.Join(result, err)
	}
	return result
}

func producerOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "sent"
}
