package deadletter

import "context"

// Record 是一条死信记录的通用形态。
// 不绑定具体业务：consumer 层只负责把「消费失败的消息」原样落库，
// 业务方后续按 Topic/EventType 分类排查、重放。
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	EventType string
	EventID   string
	Payload   []byte
	Err       string
	Attempts  int
}

// Writer 死信写入端口。
// 基础设施层（MySQL / Kafka）实现它，consumer 只依赖此接口。
type Writer interface {
	Write(ctx context.Context, rec Record) error
}
