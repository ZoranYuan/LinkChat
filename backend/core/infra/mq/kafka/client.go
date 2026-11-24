package kafka

import (
	"time"

	"github.com/IBM/sarama"

	"IM_backend/configs"
)

type Client struct {
	Producer      sarama.SyncProducer
	ConsumerGroup sarama.ConsumerGroup
}

func NewClient(cfg configs.KafkaConfig) (*Client, error) {
	pcfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(cfg.Consumer.Version)
	if err != nil {
		return nil, err
	}
	pcfg.Version = version

	// 生产者在发送消息之前，尝试将多个消息批量发送，
	pcfg.Producer.Flush.Bytes = cfg.Producer.BatchSize

	// 生产者投递消息的频率
	pcfg.Producer.Flush.Frequency = time.Duration(cfg.Producer.LingerMs) * time.Millisecond
	pcfg.Producer.Compression = map[string]sarama.CompressionCodec{"none": sarama.CompressionNone, "gzip": sarama.CompressionGZIP, "snappy": sarama.CompressionSnappy, "lz4": sarama.CompressionLZ4, "zstd": sarama.CompressionZSTD}[cfg.Producer.Compression]
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.Retry.Max = cfg.Producer.Retries
	pcfg.Producer.RequiredAcks = sarama.WaitForAll
	pcfg.Producer.Idempotent = true
	pcfg.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(cfg.Brokers, pcfg)
	if err != nil {
		return nil, err
	}

	ccfg := sarama.NewConfig()
	ccfg.Version = version
	ccfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	ccfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{map[string]sarama.BalanceStrategy{"range": sarama.NewBalanceStrategyRange(), "roundrobin": sarama.NewBalanceStrategyRoundRobin(), "sticky": sarama.NewBalanceStrategySticky()}[cfg.Consumer.Assignor]}

	// offset 异步刷脏：交给 Sarama AutoCommit 后台协程按间隔提交，不在处理路径里逐条同步 Commit。
	ccfg.Consumer.Offsets.AutoCommit.Enable = true
	if cfg.Consumer.AutoCommitIntervalMs > 0 {
		ccfg.Consumer.Offsets.AutoCommit.Interval = time.Duration(cfg.Consumer.AutoCommitIntervalMs) * time.Millisecond
	}

	// fetch 节流调优：默认 MaxWaitTime=500ms / MaxProcessingTime=100ms 会把消费吞吐压到秒级，
	// 这里放开处理预算、收紧空等，让消费循环真正跑起来。
	if cfg.Consumer.FetchMaxWaitMs > 0 {
		ccfg.Consumer.MaxWaitTime = time.Duration(cfg.Consumer.FetchMaxWaitMs) * time.Millisecond
	}
	if cfg.Consumer.FetchMaxProcessingMs > 0 {
		ccfg.Consumer.MaxProcessingTime = time.Duration(cfg.Consumer.FetchMaxProcessingMs) * time.Millisecond
	}

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.Consumer.GroupID, ccfg)
	if err != nil {
		_ = producer.Close()
		return nil, err
	}

	return &Client{
		Producer:      producer,
		ConsumerGroup: consumerGroup,
	}, nil
}

func (client *Client) Close() error {
	if client == nil {
		return nil
	}

	var firstErr error
	if client.ConsumerGroup != nil {
		firstErr = client.ConsumerGroup.Close()
	}
	if client.Producer != nil {
		if err := client.Producer.Close(); firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
