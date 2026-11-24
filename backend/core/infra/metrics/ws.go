package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WSActiveConnections 当前本机网关注册的在线 WebSocket 连接数。
// 在 Gateway.registerLocked 中 Inc，在 Gateway.Unregister 成功清理时 Dec，
// 与 g.connections 的实际长度保持一致。
var WSActiveConnections = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "linkchat_ws_active_connections",
		Help: "当前本机网关注册的在线 WebSocket 连接数。",
	},
)

// WSConnectionsTotal 累计连接事件，outcome 取值：opened / closed / rejected。
// opened - closed 应等于 WSActiveConnections，可用于校验 Gauge 是否漂移；
// rejected 记录因网关关闭而拒绝的握手次数。
var WSConnectionsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "linkchat_ws_connections_total",
		Help: "WebSocket 连接事件累计数。",
	},
	[]string{"outcome"},
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
