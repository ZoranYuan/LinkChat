#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fixture_file="${FIXTURE_FILE:-/tmp/linkchat-room-load-fixture.json}"
vus="${VUS:-300}"
rate_per_user="${RATE_PER_USER:-1}"
duration_seconds="${DURATION_SECONDS:-300}"
ramp_seconds="${RAMP_SECONDS:-60}"
connection_grace_seconds="${CONNECTION_GRACE_SECONDS:-15}"
drain_seconds="${DRAIN_SECONDS:-30}"
run_id="${RUN_ID:-room-load-$(date -u +%Y%m%dT%H%M%SZ)}"
result_dir="${RESULT_DIR:-/tmp/linkchat-room-load-results}"
ws_url="${WS_URL:-ws://127.0.0.1:8081/api/v1/ws}"
api_base_url="${API_BASE_URL:-http://127.0.0.1:8081/api/v1}"
web_origin="${WEB_ORIGIN:-http://127.0.0.1:5173}"
prometheus_rw_url="${PROMETHEUS_RW_URL:-http://prometheus:9090/api/v1/write}"

if [[ ! -f "$fixture_file" ]]; then
  echo "压测账号文件不存在：$fixture_file" >&2
  exit 1
fi

mkdir -p "$result_dir"
export K6_PROMETHEUS_RW_SERVER_URL="$prometheus_rw_url"
export K6_PROMETHEUS_RW_TREND_STATS='p(95),p(99),avg'

echo "run_id=$run_id users=$vus rate_per_user=${rate_per_user}/s duration=${duration_seconds}s"
echo "计划消息数=$((vus * rate_per_user * duration_seconds))，连接爬坡=${ramp_seconds}s，就绪缓冲=${connection_grace_seconds}s，发送结束后排空等待=${drain_seconds}s"

k6 run \
  --out experimental-prometheus-rw \
  --tag "run_id=$run_id" \
  --env "FIXTURE_FILE=$fixture_file" \
  --env "VUS=$vus" \
  --env "RATE_PER_USER=$rate_per_user" \
  --env "DURATION_SECONDS=$duration_seconds" \
  --env "RAMP_SECONDS=$ramp_seconds" \
  --env "CONNECTION_GRACE_SECONDS=$connection_grace_seconds" \
  --env "DRAIN_SECONDS=$drain_seconds" \
  --env "RUN_ID=$run_id" \
  --env "RESULT_FILE=$result_dir/$run_id.json" \
  --env "WS_URL=$ws_url" \
  --env "API_BASE_URL=$api_base_url" \
  --env "WEB_ORIGIN=$web_origin" \
  "$script_dir/k6-room-load.js"
