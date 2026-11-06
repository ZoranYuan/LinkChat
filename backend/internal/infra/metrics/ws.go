package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var WSOutboundToReadyDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name: "linkchat_ws_outbound_to_ready_duration_seconds",
		Help: "从尝试进入 outbound 到成功提交 readyQueue 的耗时。",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.02, 0.05,
			0.1, 0.25, 0.5, 1, 2, 5,
			10, 30, 60, 300,
		},
	},
)

var WSBinaryWriteDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "linkchat_ws_binary_write_duration_seconds",
		Help:    "WebSocket 二进制批次从开始写入到 WriteMessage 返回的耗时。",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	},
)

var WSBinaryWrites = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "linkchat_ws_binary_writes_total",
		Help: "成功写入的 WebSocket 二进制批次数。",
	},
)

var WSBinaryWritePayloadBytes = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "linkchat_ws_binary_write_payload_bytes_total",
		Help: "成功写入的 WebSocket 二进制批次 payload 字节数，不包含 WebSocket/TCP 协议头。",
	},
)

var WSBinaryWriteErrors = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "linkchat_ws_binary_write_errors_total",
		Help: "失败的 WebSocket 二进制批次写入次数。",
	},
)
