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
	pcfg.Producer.Flush.Bytes = cfg.Producer.BatchSize
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
