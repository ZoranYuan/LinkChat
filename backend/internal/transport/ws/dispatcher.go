package ws

import (
	realtimews "IM_backend/internal/infrastructure/realtime/websocket"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"log"
)

type DispatchHandle func(ctx context.Context, connection *realtimews.Connection, data []byte) error

type Dispatcher struct {
	handlers map[string]DispatchHandle
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[string]DispatchHandle),
	}
}

func (d *Dispatcher) RegisterHandler(op string, dh DispatchHandle) {
	d.handlers[op] = dh
}

func (r *Dispatcher) Dispatch(ctx context.Context, connection *realtimews.Connection, op string, data []byte) {
	handler, ok := r.handlers[op]
	if !ok {
		log.Printf("未知的消息类型 %s", op)
		payload, err := json.Marshal(protocol.WSErrorEvent{
			RequestOp: op,
			Code:      "unknown_operation",
			Message:   "未知的请求类型",
		})
		if err == nil {
			if replyErr := connection.PushEvent(protocol.EventTypeWSError, payload); replyErr != nil {
				log.Printf("发送 WebSocket 未知操作错误事件失败：%v", replyErr)
			}
		}
		return
	}

	ctx = context.WithValue(ctx, "op", op)
	err := handler(ctx, connection, data)

	if err != nil {
		log.Println("处理 WebSocket 消息失败：", err)
		payload, marshalErr := json.Marshal(protocol.WSErrorEvent{
			RequestOp: op,
			Code:      "request_failed",
			Message:   "请求处理失败",
		})
		if marshalErr == nil {
			if replyErr := connection.PushEvent(protocol.EventTypeWSError, payload); replyErr != nil {
				log.Printf("发送 WebSocket 错误事件失败：%v", replyErr)
			}
		}
	}
}
