import { SharedArray } from 'k6/data';
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { clearInterval, clearTimeout, setInterval, setTimeout } from 'k6/timers';
import { WebSocket } from 'k6/websockets';

const fixtureFile = __ENV.FIXTURE_FILE || '/tmp/linkchat-room-load-fixture-1000.json';
const users = new SharedArray('room members', () => JSON.parse(open(fixtureFile)).users);
const roomUsers = Number(__ENV.ROOM_USERS || 1000);
const ratePerUser = Number(__ENV.RATE_PER_USER || 1);
const durationSeconds = Number(__ENV.DURATION_SECONDS || 20);
const rampSeconds = Number(__ENV.RAMP_SECONDS || 60);
const warmupSeconds = Number(__ENV.WARMUP_SECONDS || 10);
const drainSeconds = Number(__ENV.DRAIN_SECONDS || 15);
const runId = __ENV.RUN_ID || `room-e2e-${Date.now()}`;
const conversationId = __ENV.CONVERSATION_ID;
const apiBase = (__ENV.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const wsUrl = __ENV.WS_URL || `${apiBase.replace(/^http/, 'ws')}/ws`;
const origin = __ENV.WEB_ORIGIN || 'http://127.0.0.1:5173';

if (users.length < roomUsers) throw new Error(`fixture 只有 ${users.length} 个账号，少于指定的 ${roomUsers}`);
if (!conversationId) throw new Error('缺少 CONVERSATION_ID');
if (!Number.isInteger(roomUsers) || roomUsers < 1) throw new Error('ROOM_USERS 必须是正整数');
if (!Number.isFinite(ratePerUser) || ratePerUser <= 0) throw new Error('RATE_PER_USER 必须大于 0');
if (!Number.isInteger(durationSeconds) || durationSeconds < 1) throw new Error('DURATION_SECONDS 必须是正整数');

export const options = {
  scenarios: {
    room_e2e: {
      executor: 'per-vu-iterations',
      vus: roomUsers,
      iterations: 1,
      maxDuration: `${rampSeconds + warmupSeconds + durationSeconds + drainSeconds + 45}s`,
      gracefulStop: '10s',
    },
  },
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
};

const connectionsOpened = new Counter('im_ws_connections_opened');
const connectionErrors = new Counter('im_ws_connection_errors');
const unexpectedCloses = new Counter('im_ws_unexpected_closes');
const messagesSent = new Counter('im_ws_messages_sent');
const sendErrors = new Counter('im_ws_send_errors');
const batchesReceived = new Counter('im_ws_event_batches_received');
const batchAcksSent = new Counter('im_ws_event_batch_acks_sent');
const sendResultsReceived = new Counter('im_ws_send_results_received');
const sendResultsFailed = new Counter('im_ws_send_results_failed');
const probeDeliveries = new Counter('im_ws_probe_deliveries');
const syncRequests = new Counter('im_ws_sync_requests');
const syncFailures = new Counter('im_ws_sync_failures');
const probeE2E = new Trend('im_ws_probe_e2e_ms', true);

const encodeText = (value) => new Uint8Array([...value].map((char) => char.charCodeAt(0)));
const decodeText = (value) => String.fromCharCode(...value);

function concatBytes(...parts) {
  const length = parts.reduce((total, part) => total + part.length, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

function encodeVarint(value) {
  const bytes = [];
  let current = value;
  while (current > 127) {
    bytes.push((current & 127) | 128);
    current = Math.floor(current / 128);
  }
  bytes.push(current);
  return new Uint8Array(bytes);
}

function encodeBytesField(number, value) {
  return concatBytes(encodeVarint(number * 8 + 2), encodeVarint(value.length), value);
}

function encodeStringField(number, value) {
  return encodeBytesField(number, encodeText(value));
}

function encodeIntField(number, value) {
  return concatBytes(encodeVarint(number * 8), encodeVarint(value));
}

function encodeFrame(op, data) {
  return concatBytes(encodeStringField(1, op), encodeBytesField(2, data)).buffer;
}

function encodeSendRequest(clientMsgId, content) {
  const payload = concatBytes(
    encodeStringField(1, clientMsgId),
    encodeIntField(4, 1),
    encodeStringField(5, content),
    encodeStringField(19, conversationId),
  );
  return encodeFrame('message_send', payload);
}

function encodeBatchAck(batchId) {
  return encodeFrame('event_batch_ack', encodeStringField(1, batchId));
}

function readVarint(bytes, state) {
  let value = 0;
  let scale = 1;
  while (state.offset < bytes.length) {
    const current = bytes[state.offset++];
    value += (current & 127) * scale;
    if (current < 128) return value;
    scale *= 128;
  }
  throw new Error('protobuf varint 不完整');
}

function decodeFields(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  const fields = new Map();
  const state = { offset: 0 };
  while (state.offset < bytes.length) {
    const tag = readVarint(bytes, state);
    const number = Math.floor(tag / 8);
    const wire = tag & 7;
    let value;
    if (wire === 0) {
      value = readVarint(bytes, state);
    } else if (wire === 2) {
      const length = readVarint(bytes, state);
      const end = state.offset + length;
      if (end > bytes.length) throw new Error('protobuf 字段不完整');
      value = bytes.subarray(state.offset, end);
      state.offset = end;
    } else if (wire === 1 || wire === 5) {
      state.offset += wire === 1 ? 8 : 4;
      if (state.offset > bytes.length) throw new Error('protobuf 定长字段不完整');
      continue;
    } else {
      throw new Error(`不支持的 protobuf wire type: ${wire}`);
    }
    if (!fields.has(number)) fields.set(number, []);
    fields.get(number).push(value);
  }
  return fields;
}

function fieldBytes(fields, number) {
  return fields.get(number)?.[0] || new Uint8Array();
}

function fieldString(fields, number) {
  return decodeText(fieldBytes(fields, number));
}

function handleBusinessFrame(op, payloadBytes, vu, seenProbes) {
  const payload = decodeFields(payloadBytes);
  if (op === 'message_push') {
    const content = fieldString(payload, 8);
    const parts = content.split('|');
    if (parts.length !== 6 || parts[0] !== 'probe' || parts[1] !== runId) return;
    const clientMsgId = parts[2];
    const sentAt = Number(parts[3]);
    const targetVu = Number(parts[4]);
    if (targetVu !== vu || !Number.isFinite(sentAt) || seenProbes.has(clientMsgId)) return;
    seenProbes.add(clientMsgId);
    probeE2E.add(Date.now() - sentAt, { run_id: runId, loadgen: 'local' });
    probeDeliveries.add(1, { run_id: runId, loadgen: 'local' });
  } else if (op === 'message_send_result') {
    sendResultsReceived.add(1, { run_id: runId, loadgen: 'local' });
    if (fieldString(payload, 3) === 'failed') {
      sendResultsFailed.add(1, { run_id: runId, loadgen: 'local' });
    }
  }
}

function recordSyncedMessages(response, vu, seenProbes, cursor) {
  const messages = response?.data?.messages || [];
  for (const message of messages) {
    cursor.seq = Math.max(cursor.seq, Number(message.seq) || 0);
    const parts = String(message.content || '').split('|');
    if (parts.length !== 6 || parts[0] !== 'probe' || parts[1] !== runId) continue;
    const clientMsgId = parts[2];
    const sentAt = Number(parts[3]);
    const targetVu = Number(parts[4]);
    if (targetVu !== vu || !Number.isFinite(sentAt) || seenProbes.has(clientMsgId)) continue;
    seenProbes.add(clientMsgId);
    probeE2E.add(Date.now() - sentAt, { run_id: runId, loadgen: 'local' });
    probeDeliveries.add(1, { run_id: runId, loadgen: 'local' });
  }
}

function syncRoom(user, vu, seenProbes, cursor) {
  const url = `${apiBase}/messages/sync?conversationId=${encodeURIComponent(conversationId)}&afterSeq=${cursor.seq}`;
  const response = http.get(url, {
    headers: { Authorization: `Bearer ${user.token}` },
    tags: { name: 'room_message_sync' },
  });
  syncRequests.add(1, { run_id: runId, loadgen: 'local' });
  if (response.status !== 200) {
    syncFailures.add(1, { run_id: runId, loadgen: 'local' });
    return;
  }
  recordSyncedMessages(response.json(), vu, seenProbes, cursor);
}

export function setup() {
  return {
    measureAt: Date.now() + (rampSeconds + warmupSeconds) * 1000,
  };
}

export default function (timing) {
  const vu = __VU;
  const user = users[vu - 1];
  sleep((rampSeconds * (vu - 1)) / roomUsers);

  const socket = new WebSocket(wsUrl, null, {
    headers: {
      Authorization: `Bearer ${user.token}`,
      Origin: origin,
      'X-Device-ID': `${runId}-${vu}`,
      'X-Platform': 'web',
    },
    tags: { name: 'room_e2e_ws' },
  });
  socket.binaryType = 'arraybuffer';

  const seenProbes = new Set();
  const cursor = { seq: 0 };
  const startAt = timing.measureAt;
  const endAt = timing.measureAt + durationSeconds * 1000;
  const targetVu = (vu % roomUsers) + 1;
  let sendTimer;
  let startTimer;
  let closeTimer;
  let intentionallyClosed = false;

  const stopSending = () => {
    if (sendTimer !== undefined) clearInterval(sendTimer);
    if (startTimer !== undefined) clearTimeout(startTimer);
  };

  const sendOne = (sequence) => {
    if (Date.now() >= endAt) {
      stopSending();
      return;
    }
    const sentAt = Date.now();
    const clientMsgId = `${runId}-${vu}-${sequence}`;
    const content = `probe|${runId}|${clientMsgId}|${sentAt}|${targetVu}|e2e`;
    try {
      socket.send(encodeSendRequest(clientMsgId, content));
      messagesSent.add(1, { run_id: runId, loadgen: 'local' });
    } catch (_) {
      sendErrors.add(1, { run_id: runId, loadgen: 'local' });
    }
  };

  const handleFrame = (rawFrame) => {
    const frame = decodeFields(rawFrame);
    const op = fieldString(frame, 1);
    const payload = fieldBytes(frame, 2);
    if (op === 'event_batch') {
      const batch = decodeFields(payload);
      const batchId = fieldString(batch, 2);
      socket.send(encodeBatchAck(batchId));
      batchAcksSent.add(1, { run_id: runId, loadgen: 'local' });
      batchesReceived.add(1, { run_id: runId, loadgen: 'local' });
      for (const innerFrame of batch.get(1) || []) {
        const inner = decodeFields(innerFrame);
        handleBusinessFrame(fieldString(inner, 1), fieldBytes(inner, 2), vu, seenProbes);
      }
      return;
    }
    if (op === 'room_message_notice') {
      syncRoom(user, vu, seenProbes, cursor);
      return;
    }
    if (op === 'message_push') {
      const payloadFields = decodeFields(payload);
      cursor.seq = Math.max(cursor.seq, Number(payloadFields.get(5)?.[0] || 0));
    }
    handleBusinessFrame(op, payload, vu, seenProbes);
  };

  socket.onopen = () => {
    connectionsOpened.add(1, { run_id: runId, loadgen: 'local' });
    startTimer = setTimeout(() => {
      sendOne(0);
      sendTimer = setInterval(() => {
        const sequence = Math.floor((Date.now() - startAt) / (1000 / ratePerUser));
        sendOne(sequence);
      }, 1000 / ratePerUser);
    }, Math.max(0, startAt - Date.now()));
    closeTimer = setTimeout(() => {
      stopSending();
      intentionallyClosed = true;
      socket.close();
    }, Math.max(0, endAt + drainSeconds * 1000 - Date.now()));
  };

  socket.onmessage = ({ data }) => {
    try {
      handleFrame(data);
    } catch (_) {
      // Count malformed frames as connection errors so incomplete runs are visible.
      connectionErrors.add(1, { run_id: runId, loadgen: 'local' });
    }
  };

  socket.onerror = () => {
    connectionErrors.add(1, { run_id: runId, loadgen: 'local' });
    stopSending();
    if (closeTimer !== undefined) clearTimeout(closeTimer);
    socket.close();
  };

  socket.onclose = () => {
    stopSending();
    if (!intentionallyClosed) unexpectedCloses.add(1, { run_id: runId, loadgen: 'local' });
  };

}
