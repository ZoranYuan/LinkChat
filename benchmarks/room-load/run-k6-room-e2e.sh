#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export K6_PROMETHEUS_RW_SERVER_URL="${K6_PROMETHEUS_RW_SERVER_URL:-http://prometheus:9090/api/v1/write}"
export K6_PROMETHEUS_RW_TREND_STATS="${K6_PROMETHEUS_RW_TREND_STATS:-p(95),p(99),avg}"
export K6_PROMETHEUS_RW_PUSH_INTERVAL="${K6_PROMETHEUS_RW_PUSH_INTERVAL:-2s}"

exec k6 run --out experimental-prometheus-rw "$@" "${script_dir}/k6-room-e2e.js"
