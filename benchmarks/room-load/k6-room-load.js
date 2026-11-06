import { WebSocket } from 'k6/websockets';
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { setInterval, clearInterval, setTimeout, clearTimeout } from 'k6/timers';

const fixture = JSON.parse(open(__ENV.FIXTURE_FILE || './fixture.json'));
const RECEIVER_VUS = Number(__ENV.RECEIVERS || 300);
const SENDER_VUS = Number(__ENV.SENDERS || 20);
const SEND_RATE = Number(__ENV.SEND_RATE || 200);
const SEND_RATE_PER_VU = SEND_RATE / SENDER_VUS;
const TOTAL_VUS = RECEIVER_VUS + SENDER_VUS;
const SECONDS = Number(__ENV.DURATION_SECONDS || 300);
const RAMP = Number(__ENV.RAMP_SECONDS || 30);
const DRAIN = Number(__ENV.DRAIN_SECONDS || 30);
const API = (__ENV.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const WS = __ENV.WS_URL || `${API.replace(/^http/, 'ws')}/ws`;
const ORIGIN = __ENV.WEB_ORIGIN || 'http://127.0.0.1:5173';

if (!Number.isInteger(RECEIVER_VUS) || RECEIVER_VUS < 1 || !Number.isInteger(SENDER_VUS) || SENDER_VUS < 1) {
  throw new Error('接收端和发送端 VU 数量必须是正整数');
}
if (SEND_RATE % SENDER_VUS !== 0) throw new Error('总发送速率必须能被发送端 VU 数整除');
if (fixture.users.length < TOTAL_VUS) throw new Error(`fixture 需要至少 ${TOTAL_VUS} 个独立账号`);

export const options = {
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    room: {
      executor: 'per-vu-iterations',
      vus: TOTAL_VUS,
      iterations: 1,
      maxDuration: `${RAMP + SECONDS + DRAIN + 30}s`,
    },
  },
};

const sent = new Counter('im_ws_messages_sent');
const sendErrors = new Counter('im_ws_send_errors');
const connected = new Counter('im_ws_connection_opened');
const connectionErrors = new Counter('im_ws_connection_errors');
const batches = new Counter('im_ws_batches_received');
const duplicates = new Counter('im_ws_duplicate_batches');
const parseErrors = new Counter('im_ws_frame_parse_errors');
const ackSuccess = new Counter('im_ws_ack_success');
const ackFailed = new Counter('im_ws_ack_failed');
const ackMissing = new Counter('im_ws_ack_missing');
const syncRequests = new Counter('im_ws_sync_requests');
const syncFailures = new Counter('im_ws_sync_failures');
const syncGaps = new Counter('im_ws_sync_gaps');
const batchAckMs = new Trend('im_ws_batch_ack_ms');
const ackBatchMs = new Trend('im_ws_send_result_batch_ms');
const pushBatchMs = new Trend('im_ws_push_batch_e2e_ms');
const syncBatchMs = new Trend('im_ws_sync_batch_e2e_ms');
const syncHttpMs = new Trend('im_ws_sync_http_latency_ms');
const timerLagMs = new Trend('im_ws_send_timer_lag_ms');

function varint(n) {
  const bytes = [];
  while (n > 127) {
    bytes.push((n & 127) | 128);
    n = Math.floor(n / 128);
  }
  bytes.push(n);
  return bytes;
}

function field(n, bytes) {
  return [...varint(n * 8 + 2), ...varint(bytes.length), ...bytes];
}

// 压测内容、事件名称、批次标识和会话标识均使用 ASCII。
function str(n, text) {
  return field(n, Array.from(text, (c) => c.charCodeAt(0)));
}

function frame(op, data) {
  return new Uint8Array([...str(1, op), ...field(2, data)]).buffer;
}

function decode(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  const fields = {};
  let offset = 0;
  function read() {
    let value = 0;
    let scale = 1;
    while (offset < bytes.length) {
      const b = bytes[offset++];
      value += (b & 127) * scale;
      if (b < 128) return value;
      scale *= 128;
    }
    throw new Error('protobuf varint 不完整');
  }
  while (offset < bytes.length) {
    const tag = read();
    const number = Math.floor(tag / 8);
    const wire = tag & 7;
    let value;
    if (wire === 0) value = read();
    else if (wire === 2) {
      const length = read();
      if (offset + length > bytes.length) throw new Error('protobuf 字段不完整');
      value = bytes.subarray(offset, offset + length);
      offset += length;
    } else if (wire === 1 || wire === 5) {
      offset += wire === 1 ? 8 : 4;
      continue;
    } else throw new Error(`不支持的 protobuf wire type: ${wire}`);
    (fields[number] ||= []).push(value);
  }
  return fields;
}

const text = (fields, n) => String.fromCharCode(...(fields[n]?.[0] || []));
const number = (fields, n) => Number(fields[n]?.[0] || 0);

export function setup() {
  const res = http.get(`${API}/conversations`, {
    headers: { Authorization: `Bearer ${fixture.users[0].token}`, Origin: ORIGIN },
    tags: { name: 'room_setup' },
  });
  if (res.status !== 200) throw new Error(`读取初始水位失败：HTTP ${res.status}`);
  const payload = res.json();
  const conversation = payload.data?.find((c) => c.conversationId === fixture.conversationId);
  if (payload.code !== 200 || !conversation) throw new Error('未找到压测房间会话');
  return {
    initialSeq: Number(conversation.latestSeq) || 0,
    startAt: Date.now() + (RAMP + 5) * 1000,
    runId: __ENV.RUN_ID || `room-${Date.now()}`,
  };
}

export default function (data) {
  sleep((RAMP * (__VU - 1)) / TOTAL_VUS);
  const user = fixture.users[__VU - 1];
  const isSender = __VU <= SENDER_VUS;
  const senderIndex = __VU - 1;
  const pending = new Map();
  const seenBatches = new Set();
  const receivedSeqs = new Set();
  let cursor = data.initialSeq;
  let target = cursor;
  let syncing = false;
  let syncBlocked = false;
  let closed = false;
  let index = 0;
  let sendTimer, startTimer, closeTimer;

  const socket = new WebSocket(WS, null, {
    headers: {
      Authorization: `Bearer ${user.token}`,
      Origin: ORIGIN,
      'X-Device-ID': `${data.runId}-${__VU}`,
      'X-Platform': 'web',
    },
    tags: { name: 'room_ws' },
  });
  socket.binaryType = 'arraybuffer';

  function advance(seq) {
    if (seq <= cursor) return;
    receivedSeqs.add(seq);
    while (receivedSeqs.delete(cursor + 1)) cursor++;
  }

  function sentAt(content) {
    if (!content.startsWith(`k6|${data.runId}-`)) return Infinity;
    const timestamp = Number(content.slice(content.lastIndexOf('|') + 1));
    return timestamp > 0 ? timestamp : Infinity;
  }

  async function sync() {
    if (syncing || syncBlocked || closed || target <= cursor) return;
    syncing = true;
    try {
      while (!closed && cursor < target) {
        const after = cursor;
        syncRequests.add(1);
        const res = await http.asyncRequest('GET', `${API}/messages/sync?conversationId=${fixture.conversationId}&afterSeq=${after}`, null, {
          headers: { Authorization: `Bearer ${user.token}`, Origin: ORIGIN },
          tags: { name: 'room_sync' },
          timeout: '15s',
        });
        const receivedAt = Date.now();
        if (closed) return;
        syncHttpMs.add(res.timings.duration);
        if (res.status !== 200) throw new Error(`HTTP ${res.status}`);
        const payload = res.json();
        if (payload.code !== 200 || !Array.isArray(payload.data?.messages)) throw new Error('同步响应无效');
        let oldest = Infinity;
        for (const message of payload.data.messages) {
          if (Number(message.seq) <= cursor || receivedSeqs.has(Number(message.seq))) continue;
          oldest = Math.min(oldest, sentAt(message.content || ''));
          advance(Number(message.seq));
        }
        if (!isSender && oldest !== Infinity) syncBatchMs.add(receivedAt - oldest);
        if (payload.data.messages.length && Number(payload.data.messages[0].seq) > after + 1 && cursor === after) {
          syncGaps.add(1);
          syncBlocked = true;
          console.warn(`VU ${__VU}: /sync 缺失 seq=${after + 1}，停止重复补拉，缺口不视为已收到`);
          return;
        }
        if (cursor === after) return;
      }
    } catch (_) {
      syncFailures.add(1);
    } finally {
      syncing = false;
    }
  }

  socket.onmessage = ({ data: raw }) => {
    const receivedAt = Date.now();
    try {
      const outer = decode(raw);
      if (text(outer, 1) !== 'event_batch') return;
      const batch = decode(outer[2][0]);
      const batchId = text(batch, 2);
      if (!batchId) throw new Error('缺少 batch_id');
      socket.send(frame('event_batch_ack', str(1, batchId)));
      batchAckMs.add(Date.now() - receivedAt);
      if (seenBatches.has(batchId)) {
        duplicates.add(1);
        return;
      }
      seenBatches.add(batchId);
      if (seenBatches.size > 1024) seenBatches.delete(seenBatches.values().next().value);
      batches.add(1);
      let oldestPush = Infinity;
      let oldestAck = Infinity;
      let accepted = 0;
      let failed = 0;
      for (const rawFrame of batch[1] || []) {
        const item = decode(rawFrame);
        const op = text(item, 1);
        const body = decode(item[2][0]);
        if (op === 'message_send_result') {
          const id = text(body, 1);
          const timestamp = pending.get(id);
          if (timestamp === undefined) continue;
          pending.delete(id);
          if (['processing', 'completed'].includes(text(body, 3))) {
            accepted++;
            oldestAck = Math.min(oldestAck, timestamp);
          } else failed++;
        } else if (op === 'message_push' && text(body, 2) === fixture.conversationId) {
          const seq = number(body, 5);
          if (seq <= cursor || receivedSeqs.has(seq)) continue;
          oldestPush = Math.min(oldestPush, sentAt(text(body, 8)));
          advance(seq);
        } else if (op === 'room_message_notice' && text(body, 1) === fixture.conversationId) {
          target = Math.max(target, number(body, 3));
        }
      }
      if (accepted) ackSuccess.add(accepted);
      if (failed) ackFailed.add(failed);
      if (oldestAck !== Infinity) ackBatchMs.add(receivedAt - oldestAck);
      if (!isSender && oldestPush !== Infinity) pushBatchMs.add(receivedAt - oldestPush);
      void sync();
    } catch (_) {
      parseErrors.add(1);
    }
  };

  function stop() {
    if (closed) return;
    closed = true;
    if (sendTimer !== undefined) clearInterval(sendTimer);
    if (startTimer !== undefined) clearTimeout(startTimer);
    if (closeTimer !== undefined) clearTimeout(closeTimer);
    ackMissing.add(pending.size);
    pending.clear();
    receivedSeqs.clear();
    seenBatches.clear();
  }
  socket.onclose = stop;
  socket.onerror = () => {
    connectionErrors.add(1);
    stop();
    socket.close();
  };
  socket.onopen = () => {
    connected.add(1);
    const intervalMs = 1000 / SEND_RATE_PER_VU;
    let expectedAt = data.startAt + (senderIndex * intervalMs) / SENDER_VUS;
    const endAt = data.startAt + SECONDS * 1000;
    function send() {
      if (!isSender || closed || Date.now() >= endAt) {
        clearInterval(sendTimer);
        return;
      }
      const now = Date.now();
      timerLagMs.add(Math.max(0, now - expectedAt));
      expectedAt += intervalMs;
      const id = `${data.runId}-${__VU}-${++index}`;
      pending.set(id, now);
      try {
        const isProbe = senderIndex === 0 && index % SEND_RATE_PER_VU === 0;
        const content = isProbe ? `k6|${id}|${now}` : `msg|${id}|${now}`;
        const request = [...str(1, id), ...varint(4 * 8), ...varint(1), ...str(5, content), ...str(19, fixture.conversationId)];
        socket.send(frame('message_send', request));
        sent.add(1);
      } catch (_) {
        pending.delete(id);
        sendErrors.add(1);
      }
    }
    startTimer = setTimeout(() => {
      if (closed) return;
      send();
      if (isSender) sendTimer = setInterval(send, intervalMs);
    }, Math.max(0, expectedAt - Date.now()));
  };
  closeTimer = setTimeout(() => {
    stop();
    socket.close();
  }, Math.max(0, data.startAt + (SECONDS + DRAIN) * 1000 - Date.now()));
}
