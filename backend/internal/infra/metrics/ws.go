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
