package kafka

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"github.com/IBM/sarama"

	"IM_backend/configs"
	"IM_backend/internal/application/ports/messaging"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/application/ports/transaction"
)

type Producer struct {
	client     *Client
	router     *TopicRouter
	txManager  transaction.Manager
	repository outboxport.OutboxRepository
	options    configs.KafkaProducerConfig
	wake       chan struct{}
}

type producerMessageMetadata struct {
	EventID    string
	LockToken  string
	RetryCount int
}

func NewProducer(
	client *Client,
	router *TopicRouter,
	txManager transaction.Manager,
	repository outboxport.OutboxRepository,
	options configs.KafkaProducerConfig,
) *Producer {
	return &Producer{
		client:     client,
		router:     router,
		txManager:  txManager,
		repository: repository,
		options:    options,
		wake:       make(chan struct{}, 1),
	}
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

// Publish 直接发布单个事件，供 Consumer 投递死信使用。
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

// Notify 在业务事务提交后唤醒 Producer，容量为 1 的 channel 会合并密集通知。
func (p *Producer) Notify() {
	if p == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Start 持续领取 Outbox 批次，直接批量投递并更新每条记录的状态。
func (p *Producer) Start(ctx context.Context) {
	log.Printf("Kafka Producer 已启动：claimBatchSize=%d", p.options.ClaimBatchSize)

	ticker := time.NewTicker(time.Duration(p.options.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	drain := func() {
		for ctx.Err() == nil {
			processed, err := p.producePendingOnce(ctx)
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
	drain()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drain()
		case <-p.wake:
			drain()
		}
	}
}

func (p *Producer) producePendingOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if p.txManager == nil || p.repository == nil || p.client == nil || p.client.Producer == nil {
		return false, errors.New("Kafka Producer 依赖未配置")
	}
	var batch []*outboxport.Entry
	now := time.Now()
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
	if err != nil {
		return false, err
	}
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
		retryDelay := min(time.Duration(float64(time.Duration(p.options.BaseRetryWaitSeconds)*time.Second)*math.Pow(2, float64(metadata.RetryCount))), time.Duration(p.options.MaxRetryWaitSeconds)*time.Second)
		retry = append(retry, outboxport.RetryUpdate{Lease: lease, NextRetryAt: failedAt.Add(retryDelay), LastError: lastError})
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		metadata := producerMessageMetadata{
			EventID: item.EventID, LockToken: item.LockToken, RetryCount: item.RetryCount,
		}
		message, err := p.newProducerMessage(messaging.IntegrationEvent{
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

	batchErrs := p.client.Producer.SendMessages(messages)
	failedAt = time.Now()
	var publishErrs sarama.ProducerErrors
	partialFailure := errors.As(batchErrs, &publishErrs)
	failedMessages := make(map[*sarama.ProducerMessage]error, len(publishErrs))
	for _, failure := range publishErrs {
		failedMessages[failure.Msg] = failure.Err
	}

	// 批量投递后需要逐条分析哪个消息没有投递成功
	for _, message := range messages {
		metadata := message.Metadata.(producerMessageMetadata)

		publishErr := batchErrs
		if partialFailure {
			publishErr = failedMessages[message]
		}
		if publishErr != nil {
			collectFailure(metadata, publishErr.Error())
			continue
		}
		sent = append(sent, outboxport.Lease{EventID: metadata.EventID, LockToken: metadata.LockToken})
	}
	return p.markBatchResults(ctx, sent, retry, dead)
}

func (p *Producer) markBatchResults(ctx context.Context, sent []outboxport.Lease, retry []outboxport.RetryUpdate, dead []outboxport.DeadUpdate) error {
	return errors.Join(
		p.repository.MarkSentBatch(ctx, sent, time.Now()),
		p.repository.MarkRetryBatch(ctx, retry),
		p.repository.MarkDeadBatch(ctx, dead),
	)
}
