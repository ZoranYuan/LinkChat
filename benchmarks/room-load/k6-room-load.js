import { WebSocket } from 'k6/websockets';
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';
import { setInterval, clearInterval, setTimeout, clearTimeout } from 'k6/timers';

const fixture = JSON.parse(open(__ENV.FIXTURE_FILE || '/tmp/linkchat-room-load-fixture.json'));
const users = fixture.users || [];
const vus = Number(__ENV.VUS || 300);
const durationSeconds = Number(__ENV.DURATION_SECONDS || 300);
const ratePerUser = Number(__ENV.RATE_PER_USER || 1);
const rampSeconds = Number(__ENV.RAMP_SECONDS || 60);
const connectionGraceSeconds = Number(__ENV.CONNECTION_GRACE_SECONDS || 15);
const drainSeconds = Number(__ENV.DRAIN_SECONDS || 30);
const wsUrl = __ENV.WS_URL || 'ws://127.0.0.1:8081/api/v1/ws';
const apiBaseUrl = (__ENV.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const webOrigin = __ENV.WEB_ORIGIN || 'http://127.0.0.1:5173';

if (users.length < vus) throw new Error(`需要至少 ${vus} 个测试账号，fixture 中只有 ${users.length} 个`);
const selectedUsers = users.slice(0, vus);
const userIDs = selectedUsers.map((user) => user.userId);
const tokens = selectedUsers.map((user) => user.token);
if (new Set(userIDs).size !== vus || new Set(tokens).size !== vus || tokens.some((token) => !token)) {
  throw new Error('压测账号必须是 token 有效且 userId 唯一的独立用户');
}
if (!fixture.roomId || !fixture.conversationId || !Number.isInteger(ratePerUser) || ratePerUser < 1) {
  throw new Error('压测 fixture 或 RATE_PER_USER 参数无效');
}

const plannedMessages = vus * ratePerUser * durationSeconds;
const intervalMs = 1000 / ratePerUser;

export const options = {
  summaryTrendStats: ['avg', 'p(95)', 'p(99)'],
  scenarios: {
    room_members: {
      executor: 'per-vu-iterations',
      vus,
      iterations: 1,
      maxDuration: `${rampSeconds + connectionGraceSeconds + durationSeconds + drainSeconds + 60}s`,
      gracefulStop: '5s',
    },
  },
};

const messagesSent = new Counter('im_ws_messages_sent');
const sendErrors = new Counter('im_ws_send_errors');
const ackSuccess = new Counter('im_ws_ack_success');
const ackFailed = new Counter('im_ws_ack_failed');
const ackMissing = new Counter('im_ws_ack_missing');
const ackUnmatched = new Counter('im_ws_ack_unmatched');
const connectionOpened = new Counter('im_ws_connection_opened');
const connectionOpenedBeforeLoad = new Counter('im_ws_connection_opened_before_load');
const connectionFailed = new Counter('im_ws_connection_failed');
const lateConnection = new Counter('im_ws_connection_late');
const wsBatchesReceived = new Counter('im_ws_batches_received');
const messageEventsReceived = new Counter('im_ws_message_events_received');
const roomNoticesReceived = new Counter('im_ws_room_notices_received');
const uniqueMessageDeliveries = new Counter('im_ws_unique_message_deliveries');
const syncRequests = new Counter('im_ws_sync_requests');
const syncFailures = new Counter('im_ws_sync_failures');
const syncNoProgress = new Counter('im_ws_sync_no_progress');
const syncMessagesReceived = new Counter('im_ws_sync_messages_received');
const otherEventsReceived = new Counter('im_ws_other_events_received');
const frameParseErrors = new Counter('im_ws_frame_parse_errors');
const ackLatency = new Trend('im_ws_ack_latency_ms');
const messageE2ELatency = new Trend('im_ws_message_e2e_ms');
const pushMessageE2ELatency = new Trend('im_ws_message_push_e2e_ms');
const syncMessageE2ELatency = new Trend('im_ws_message_sync_e2e_ms');
const syncLatency = new Trend('im_ws_sync_http_latency_ms');
const sendTimerLag = new Trend('im_ws_send_timer_lag_ms');
const initialSeqMetric = new Gauge('im_ws_initial_seq');

function varint(value) {
  let n = Number(value);
  const output = [];
  while (n > 127) {
    output.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
  }
  output.push(n);
  return output;
}

function bytesField(number, value) {
  return [...varint((number << 3) | 2), ...varint(value.length), ...value];
}

function utf8Encode(value) {
  const escaped = encodeURIComponent(value).replace(/%([0-9A-F]{2})|./g, (match, hex) =>
    hex ? String.fromCharCode(parseInt(hex, 16)) : match,
  );
  return Array.from(escaped, (character) => character.charCodeAt(0));
}

function utf8Decode(bytes) {
  let escaped = '';
  for (const byte of bytes) escaped += `%${byte.toString(16).padStart(2, '0')}`;
  return decodeURIComponent(escaped);
}

function stringField(number, value) {
  return bytesField(number, utf8Encode(value));
}

function parseFields(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  const fields = [];
  let offset = 0;

  function readVarint() {
    let value = 0;
    let shift = 0;
    while (offset < bytes.length) {
      const byte = bytes[offset++];
      value += (byte & 0x7f) * (2 ** shift);
      if ((byte & 0x80) === 0) return value;
      shift += 7;
      if (shift > 49) throw new Error('protobuf varint too large');
    }
    throw new Error('truncated protobuf varint');
  }

  while (offset < bytes.length) {
    const tag = readVarint();
    const number = Math.floor(tag / 8);
    const wire = tag & 7;
    if (wire === 0) {
      fields.push({ number, wire, value: readVarint() });
    } else if (wire === 2) {
      const length = readVarint();
      const end = offset + length;
      if (end > bytes.length) throw new Error('truncated protobuf field');
      fields.push({ number, wire, value: bytes.slice(offset, end) });
      offset = end;
    } else if (wire === 1 || wire === 5) {
      const length = wire === 1 ? 8 : 4;
      fields.push({ number, wire, value: bytes.slice(offset, offset + length) });
      offset += length;
    } else {
      throw new Error(`unsupported protobuf wire type ${wire}`);
    }
  }
  return fields;
}

function firstBytes(fields, number) {
  const field = fields.find((item) => item.number === number && item.wire === 2);
  return field ? field.value : new Uint8Array(0);
}

function firstString(fields, number) {
  return utf8Decode(firstBytes(fields, number));
}

function firstNumber(fields, number) {
  return fields.find((item) => item.number === number && item.wire === 0)?.value || 0;
}

function sendChatMessage(socket, conversationId, clientMsgId, sentAt) {
  const content = `k6|${clientMsgId}|${sentAt}`;
  const request = new Uint8Array([
    ...stringField(1, clientMsgId),
    ...stringField(5, content),
    ...stringField(19, conversationId),
  ]);
  const frame = new Uint8Array([...stringField(1, 'message_send'), ...bytesField(2, request)]);
  socket.send(frame.buffer);
}

export function setup() {
  const response = http.get(`${apiBaseUrl}/conversations`, {
    headers: {
      Authorization: `Bearer ${tokens[0]}`,
      Origin: webOrigin,
    },
    timeout: '15s',
    tags: { name: 'linkchat_benchmark_setup' },
  });
  if (response.status !== 200) {
    throw new Error(`压测前读取会话水位失败：status=${response.status}`);
  }

  const payload = response.json();
  const conversation = Array.isArray(payload.data)
    ? payload.data.find((item) => item.conversationId === fixture.conversationId)
    : null;
  if (payload.code !== 200 || !conversation) {
    throw new Error('压测前未找到房间会话');
  }

  const initialSeq = Number(conversation.latestSeq) || 0;
  initialSeqMetric.add(initialSeq);
  return {
    loadStartAt: Date.now() + (rampSeconds + connectionGraceSeconds) * 1000,
    runId: __ENV.RUN_ID || `room-load-${Date.now()}`,
    initialSeq,
  };
}

export default function (data) {
  const vuIndex = __VU - 1;
  sleep((rampSeconds * vuIndex) / vus);

  const token = tokens[vuIndex];
  const pendingAcks = {};
  const acknowledged = new Set();
  const pendingDirectSeqs = new Set();
  const pendingBatchAcks = new Map();
  let syncCursor = Number(data.initialSeq) || 0;
  let pendingSyncTarget = 0;
  let syncInProgress = false;
  let connectionErrorCounted = false;
  let metricsFinalized = false;
  let socketClosed = false;
  let nextMessageIndex = 0;
  let sendTimer;
  let startTimer;
  let closeTimer;
  let handshakeTimer;
  let syncRetryTimer;
  let syncFailureSamples = 0;

  function recordMessage(message, source) {
    const seq = Number(message.seq) || 0;
    if (seq > 0 && seq <= syncCursor) return;

    const alreadyReceived = seq > 0 && pendingDirectSeqs.has(seq);
    if (!alreadyReceived) {
      const parts = String(message.content || '').split('|');
      if (parts[0] === 'k6' && parts[1]?.startsWith(`${data.runId}-`) && parts.length >= 3) {
        uniqueMessageDeliveries.add(1);
        if (source === 'sync') syncMessagesReceived.add(1);
        const sentAt = Number(parts[parts.length - 1]);
        if (Number.isFinite(sentAt) && sentAt > 0) {
          const e2eMs = Math.max(0, Date.now() - sentAt);
          messageE2ELatency.add(e2eMs);
          if (source === 'sync') syncMessageE2ELatency.add(e2eMs);
          else pushMessageE2ELatency.add(e2eMs);
        }
      }
    }

    if (seq <= 0 || alreadyReceived) return;
    pendingDirectSeqs.add(seq);
    while (pendingDirectSeqs.delete(syncCursor + 1)) syncCursor += 1;
  }

  async function syncAfterNotice(targetSeq) {
    if (targetSeq <= syncCursor) return;
    pendingSyncTarget = Math.max(pendingSyncTarget, targetSeq);
    if (syncInProgress || syncRetryTimer !== undefined || socketClosed) return;

    syncInProgress = true;
    try {
      while (!socketClosed && pendingSyncTarget > syncCursor) {
        const requestedTarget = pendingSyncTarget;
        const afterSeq = syncCursor;
        const url = `${apiBaseUrl}/messages/sync?conversationId=${encodeURIComponent(fixture.conversationId)}&afterSeq=${afterSeq}`;
        syncRequests.add(1);

        try {
          const response = await http.asyncRequest('GET', url, null, {
            headers: {
              Authorization: `Bearer ${token}`,
              Origin: webOrigin,
            },
            timeout: '15s',
            tags: { name: 'linkchat_message_sync' },
          });
          if (socketClosed) break;
          syncLatency.add(response.timings.duration);
          if (response.status !== 200) throw new Error(`HTTP ${response.status}`);
          const payload = response.json();
          if (payload.code !== 200 || !Array.isArray(payload.data?.messages)) {
            throw new Error(`业务返回无效：code=${payload.code}`);
          }

          for (const message of payload.data.messages) recordMessage(message, 'sync');
          acknowledgeBatches();
          if (syncCursor < requestedTarget) {
            syncNoProgress.add(1);
            break;
          }
        } catch (error) {
          syncFailures.add(1);
          if (syncFailureSamples++ < 3) console.warn(`/sync 失败：${error.message}`);
          break;
        }
      }
    } finally {
      syncInProgress = false;
      if (!socketClosed && pendingSyncTarget > syncCursor) {
        syncRetryTimer = setTimeout(() => {
          syncRetryTimer = undefined;
          void syncAfterNotice(pendingSyncTarget);
        }, 250);
      }
    }
  }

  function finalizeAcks() {
    if (metricsFinalized) return;
    metricsFinalized = true;
    for (const clientMsgId of Object.keys(pendingAcks)) {
      ackMissing.add(1);
      delete pendingAcks[clientMsgId];
    }
  }

  const socket = new WebSocket(wsUrl, null, {
    headers: {
      Authorization: `Bearer ${token}`,
      Origin: webOrigin,
      'X-Device-ID': `${data.runId}-${vuIndex}`,
      'X-Platform': 'web',
    },
    tags: { name: 'linkchat_room_ws' },
  });
  socket.binaryType = 'arraybuffer';

  function acknowledgeBatches() {
    if (socketClosed) return;
    for (const [batchId, targetSeq] of pendingBatchAcks) {
      if (targetSeq > syncCursor) continue;
      const ack = stringField(1, batchId);
      const frame = new Uint8Array([
        ...stringField(1, 'event_batch_ack'),
        ...bytesField(2, ack),
      ]);
      socket.send(frame.buffer);
      pendingBatchAcks.delete(batchId);
    }
  }

  function stopTimers() {
    pendingBatchAcks.clear();
    if (sendTimer !== undefined) clearInterval(sendTimer);
    for (const timer of [startTimer, closeTimer, handshakeTimer, syncRetryTimer]) {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  function failConnection() {
    if (!connectionErrorCounted) {
      connectionFailed.add(1);
      connectionErrorCounted = true;
    }
  }

  handshakeTimer = setTimeout(() => {
    failConnection();
    socketClosed = true;
    stopTimers();
    socket.close();
  }, 30000);

  socket.onmessage = ({ data: raw }) => {
    const receivedAt = Date.now();
    try {
      const outer = parseFields(raw);
      if (firstString(outer, 1) !== 'event_batch') return;
      wsBatchesReceived.add(1);

      const batch = parseFields(firstBytes(outer, 2));
      const batchId = firstString(batch, 2);
      if (!batchId) throw new Error('事件批次缺少 batchId');
      let targetSeq = 0;
      const frames = batch.filter((field) => field.number === 1 && field.wire === 2);
      for (const frame of frames) {
        const inner = parseFields(frame.value);
        const op = firstString(inner, 1);
        const body = firstBytes(inner, 2);

        if (op === 'message_send_result') {
          const ack = parseFields(body);
          const clientMsgId = firstString(ack, 1);
          const status = firstString(ack, 3);
          const sentAt = pendingAcks[clientMsgId];
          if (sentAt === undefined) {
            if (!acknowledged.has(clientMsgId)) ackUnmatched.add(1);
            continue;
          }
          delete pendingAcks[clientMsgId];
          if (status === 'processing' || status === 'completed') {
            acknowledged.add(clientMsgId);
            ackSuccess.add(1);
            ackLatency.add(receivedAt - sentAt);
          } else {
            ackFailed.add(1);
          }
          continue;
        }

        if (op === 'message_push') {
          const event = parseFields(body);
          messageEventsReceived.add(1);
          recordMessage({
            seq: firstNumber(event, 5),
            content: firstString(event, 8),
          }, 'message');
          continue;
        }

        if (op === 'room_message_notice') {
          roomNoticesReceived.add(1);
          const notice = parseFields(body);
          const noticeSeq = firstNumber(notice, 3);
          targetSeq = Math.max(targetSeq, noticeSeq);
          void syncAfterNotice(noticeSeq);
          continue;
        }

        otherEventsReceived.add(1);
      }
      pendingBatchAcks.set(batchId, targetSeq);
      acknowledgeBatches();
    } catch (_) {
      frameParseErrors.add(1);
    }
  };

  socket.onerror = () => {
    socketClosed = true;
    stopTimers();
    finalizeAcks();
    failConnection();
    socket.close();
  };
  socket.onclose = () => {
    socketClosed = true;
    stopTimers();
    finalizeAcks();
    const plannedCloseAt = data.loadStartAt + (durationSeconds + drainSeconds) * 1000;
    if (Date.now() < plannedCloseAt) failConnection();
  };
  socket.onopen = () => {
    clearTimeout(handshakeTimer);
    check(socket, { 'WebSocket upgraded': (connection) => connection.readyState === WebSocket.OPEN });
    connectionOpened.add(1);
    if (Date.now() < data.loadStartAt) connectionOpenedBeforeLoad.add(1);
    else lateConnection.add(1);

    const startDelay = Math.max(0, data.loadStartAt - Date.now());
    const stopSendingAt = data.loadStartAt + durationSeconds * 1000;
    startTimer = setTimeout(() => {
      let nextSendAt = Date.now();
      const sendNext = () => {
        if (socketClosed || Date.now() >= stopSendingAt) {
          clearInterval(sendTimer);
          return;
        }

        const clientMsgId = `${data.runId}-${vuIndex}-${++nextMessageIndex}`;
        const sentAt = Date.now();
        sendTimerLag.add(Math.max(0, sentAt - nextSendAt));
        nextSendAt += intervalMs;
        pendingAcks[clientMsgId] = sentAt;
        try {
          sendChatMessage(socket, fixture.conversationId, clientMsgId, sentAt);
          messagesSent.add(1);
        } catch (_) {
          delete pendingAcks[clientMsgId];
          sendErrors.add(1);
        }
      };
      sendNext();
      sendTimer = setInterval(sendNext, intervalMs);
    }, startDelay);

    const closeDelay = Math.max(
      0,
      data.loadStartAt + (durationSeconds + drainSeconds) * 1000 - Date.now(),
    );
    closeTimer = setTimeout(() => {
      socketClosed = true;
      stopTimers();
      finalizeAcks();
      socket.close();
    }, closeDelay);
  };
}

function metricCount(data, name) {
  return Number(data.metrics[name]?.values?.count || 0);
}

function metricPercentile(data, name, percentile) {
  const value = data.metrics[name]?.values?.[percentile];
  return Number.isFinite(value) ? Number(value) : null;
}

export function handleSummary(data) {
  const sent = metricCount(data, 'im_ws_messages_sent');
  const acked = metricCount(data, 'im_ws_ack_success');
  const connectionsReady = metricCount(data, 'im_ws_connection_opened_before_load');
  const connectionsFailed = metricCount(data, 'im_ws_connection_failed');
  const received = metricCount(data, 'im_ws_unique_message_deliveries');
  const expectedDeliveries = acked * vus;
  const report = {
    scenario: {
      users: vus,
      rate_per_user_per_second: ratePerUser,
      duration_seconds: durationSeconds,
      connection_ramp_seconds: rampSeconds,
      connection_grace_seconds: connectionGraceSeconds,
      planned_messages: plannedMessages,
      room_notice_triggers_sync: true,
      initial_seq: data.metrics.im_ws_initial_seq?.values?.value ?? null,
      room_id: fixture.roomId,
      conversation_id: fixture.conversationId,
      run_id: __ENV.RUN_ID || '',
      all_connections_ready_before_load: connectionsReady === vus,
    },
    results: {
      messages_sent: sent,
      send_errors: metricCount(data, 'im_ws_send_errors'),
      ack_success: acked,
      ack_failed: metricCount(data, 'im_ws_ack_failed'),
      ack_missing: metricCount(data, 'im_ws_ack_missing'),
      ack_unmatched: metricCount(data, 'im_ws_ack_unmatched'),
      ack_success_rate_percent: sent ? (acked / sent) * 100 : 0,
      connection_opened: metricCount(data, 'im_ws_connection_opened'),
      connection_opened_before_load: connectionsReady,
      connection_failed: metricCount(data, 'im_ws_connection_failed'),
      late_connections: metricCount(data, 'im_ws_connection_late'),
      websocket_batches_received: metricCount(data, 'im_ws_batches_received'),
      full_message_events_received: metricCount(data, 'im_ws_message_events_received'),
      room_notice_events_received: metricCount(data, 'im_ws_room_notices_received'),
      unique_message_deliveries: received,
      expected_message_deliveries: expectedDeliveries,
      message_delivery_coverage_percent: connectionsReady === vus && connectionsFailed === 0 && expectedDeliveries
        ? (received / expectedDeliveries) * 100
        : null,
      sync_requests: metricCount(data, 'im_ws_sync_requests'),
      sync_failures: metricCount(data, 'im_ws_sync_failures'),
      sync_no_progress: metricCount(data, 'im_ws_sync_no_progress'),
      sync_messages_received: metricCount(data, 'im_ws_sync_messages_received'),
      other_events_received: metricCount(data, 'im_ws_other_events_received'),
      frame_parse_errors: metricCount(data, 'im_ws_frame_parse_errors'),
      ack_p95_ms: metricPercentile(data, 'im_ws_ack_latency_ms', 'p(95)'),
      ack_p99_ms: metricPercentile(data, 'im_ws_ack_latency_ms', 'p(99)'),
      message_e2e_p95_ms: metricPercentile(data, 'im_ws_message_e2e_ms', 'p(95)'),
      message_e2e_p99_ms: metricPercentile(data, 'im_ws_message_e2e_ms', 'p(99)'),
      push_message_e2e_p95_ms: metricPercentile(data, 'im_ws_message_push_e2e_ms', 'p(95)'),
      sync_message_e2e_p95_ms: metricPercentile(data, 'im_ws_message_sync_e2e_ms', 'p(95)'),
      sync_http_p95_ms: metricPercentile(data, 'im_ws_sync_http_latency_ms', 'p(95)'),
      sync_http_p99_ms: metricPercentile(data, 'im_ws_sync_http_latency_ms', 'p(99)'),
      send_timer_lag_p95_ms: metricPercentile(data, 'im_ws_send_timer_lag_ms', 'p(95)'),
    },
  };
  const output = JSON.stringify(report, null, 2);
  const file = __ENV.RESULT_FILE || '/tmp/linkchat-room-load-summary.json';
  return { stdout: `${output}\n`, [file]: `${output}\n` };
}
