package kafka

import (
	"context"
	"log"
	"strconv"

	eventport "IM_backend/core/application/ports/events"
)

// DLTReplayHandler 消费 *.DLT，把修复后的消息重投回原 topic。
// 标准 DLT 模式的最后一环：死信不是终点，修完 bug 要能重放。
// 注册到一个独立 consumer group（如 im-dlt-replay-group），只订阅各 *.DLT topic。
type DLTReplayHandler struct {
	producer *Producer
}

func NewDLTReplayHandler(producer *Producer) *DLTReplayHandler {
	return &DLTReplayHandler{producer: producer}
}

// Topic 返回它要消费的 DLT topic（示例固定一个；若要消费多个 DLT，建多个实例分别注册）。
func (h *DLTReplayHandler) Topic() string {
	return DltTopic("message.read.receipt")
}

// Handle 读 DLT header 还原原 topic/key/payload，重投回原 topic。
// 注意：重投回原 topic 后，幂等表（inbox）会识别 duplicate 并跳过——这正是我们想要的：
// 若死信其实已被其它路径处理过，不会重复生效。
func (h *DLTReplayHandler) Handle(ctx context.Context, item *MessageItem) error {
	originalTopic := headerValue(item.Record, "dlt:original-topic")
	if originalTopic == "" {
		// 无法还原原 topic 的消息：确认掉，避免卡住 DLT 分区。
		log.Printf("DLT 消息缺少 original-topic header，跳过：offset=%d", item.Record.Offset)
		item.Complete()
		return nil
	}
	attempts, _ := strconv.Atoi(headerValue(item.Record, "dlt:attempts"))
	eventType := headerValue(item.Record, "event_type")
	if eventType == "" {
		eventType = eventport.KafkaEventReadReceipt
	}

	// partitionKey 用原 DLT 消息的 key（写入时就是原 eventID），重投保持同分区语义。
	key := string(item.Record.Key)
	if err := h.producer.Publish(ctx, originalTopic, eventType, key, key, item.Record.Value); err != nil {
		return err
	}
	log.Printf("DLT 重放成功：from=%s to=%s offset=%d attempts=%d",
		item.Record.Topic, originalTopic, item.Record.Offset, attempts)
	item.Complete()
	return nil
}
