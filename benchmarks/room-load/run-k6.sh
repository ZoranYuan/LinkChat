#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fixture_file="${FIXTURE_FILE:-/tmp/linkchat-room-load-fixture.json}"
receivers="${RECEIVERS:-300}"
senders="${SENDERS:-20}"
send_rate="${SEND_RATE:-200}"
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

while (($#)); do
  case "$1" in
    --receivers)
      if (($# < 2)); then echo "--receivers 缺少数量" >&2; exit 2; fi
      receivers="$2"
      shift 2
      ;;
    *)
      echo "未知参数：$1（用法：$0 --receivers 数量）" >&2
      exit 2
      ;;
  esac
done

if ! [[ "$receivers" =~ ^[1-9][0-9]*$ ]]; then
  echo "接收端数量必须是正整数：$receivers" >&2
  exit 2
fi

if [[ ! -f "$fixture_file" ]]; then
  echo "压测账号文件不存在：$fixture_file" >&2
  exit 1
fi

mkdir -p "$result_dir"
export K6_PROMETHEUS_RW_SERVER_URL="$prometheus_rw_url"
export K6_PROMETHEUS_RW_TREND_STATS='p(95),p(99),avg'

echo "run_id=$run_id receivers=$receivers senders=$senders send_rate=${send_rate}/s duration=${duration_seconds}s"
if ! [[ "$senders" =~ ^[1-9][0-9]*$ ]] || ! [[ "$send_rate" =~ ^[1-9][0-9]*$ ]] || ((send_rate % senders != 0)); then
  echo "发送端数量和总发送速率必须为正整数，且总速率须能被发送端数整除" >&2
  exit 2
fi
echo "计划消息数=$((send_rate * duration_seconds))，总连接数=$((receivers + senders))，连接爬坡=${ramp_seconds}s，就绪缓冲=${connection_grace_seconds}s，发送结束后排空等待=${drain_seconds}s"

k6 run \
  --out experimental-prometheus-rw \
  --tag "run_id=$run_id" \
  --env "FIXTURE_FILE=$fixture_file" \
  --env "RECEIVERS=$receivers" \
  --env "SENDERS=$senders" \
  --env "SEND_RATE=$send_rate" \
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
