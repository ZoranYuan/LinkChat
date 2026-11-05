package ws

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"encoding/json"
	"log"
)

type DispatchHandler func(ctx context.Context, connection *Connection, data []byte) error

type Dispatcher struct {
	handlers map[string]DispatchHandler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[string]DispatchHandler),
	}
}

func (d *Dispatcher) RegisterHandler(op string, dh DispatchHandler) {
	d.handlers[op] = dh
}

func (r *Dispatcher) Dispatch(ctx context.Context, connection *Connection, op string, data []byte) {
	handler, ok := r.handlers[op]
	if !ok {
		log.Printf("未知的消息类型 %s", op)
		payload, err := json.Marshal(messaging.ErrorNoticeEvent{
			RequestOp: op,
			Code:      "unknown_operation",
			Message:   "未知的请求类型",
		})
		if err == nil {
			if replyErr := connection.PushEvent(messaging.TypeErrorNotice, payload); replyErr != nil {
				log.Printf("发送 WebSocket 未知操作错误事件失败：%v", replyErr)
			}
		}
		return
	}

	err := handler(ctx, connection, data)

	if err != nil {
		log.Println("处理 WebSocket 消息失败：", err)
		payload, marshalErr := json.Marshal(messaging.ErrorNoticeEvent{
			RequestOp: op,
			Code:      "request_failed",
			Message:   "请求处理失败",
		})
		if marshalErr == nil {
			if replyErr := connection.PushEvent(messaging.TypeErrorNotice, payload); replyErr != nil {
				log.Printf("发送 WebSocket 错误事件失败：%v", replyErr)
			}
		}
	}
}
