import { SharedArray } from 'k6/data';
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { clearInterval, clearTimeout, setInterval, setTimeout } from 'k6/timers';
import { WebSocket } from 'k6/websockets';

const fixtureFile = __ENV.FIXTURE_FILE;
if (!fixtureFile) throw new Error('请设置 FIXTURE_FILE');
const users = new SharedArray('room members v2', () => JSON.parse(open(fixtureFile)).users);
const roomUsers = Number(__ENV.ROOM_USERS || 1000);
const perUserRate = Number(__ENV.RATE_PER_USER || 0.5);
const durationSeconds = Number(__ENV.DURATION_SECONDS || 30);
const rampSeconds = Number(__ENV.RAMP_SECONDS || 60);
const warmupSeconds = Number(__ENV.WARMUP_SECONDS || 10);
const drainSeconds = Number(__ENV.DRAIN_SECONDS || 20);
const probeEvery = Number(__ENV.PROBE_EVERY || 100);
const runId = (__ENV.RUN_ID || `room-${Date.now()}`).replace(/[^a-zA-Z0-9_-]/g, '_');
const conversationId = __ENV.CONVERSATION_ID;
const apiBase = (__ENV.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const wsUrl = __ENV.WS_URL || `${apiBase.replace(/^http/, 'ws')}/ws`;
const origin = __ENV.WEB_ORIGIN || '';
const tags = { run_id: runId, loadgen: __ENV.LOADGEN || 'local', mode: 'active-room' };

if (!conversationId) throw new Error('请设置 CONVERSATION_ID');
if (!Number.isInteger(roomUsers) || roomUsers < 2 || users.length < roomUsers) throw new Error('ROOM_USERS 与 fixture 成员数不匹配');
if (!Number.isFinite(perUserRate) || perUserRate <= 0 || perUserRate > 50) throw new Error('RATE_PER_USER 必须在 (0, 50]');
if (!Number.isInteger(durationSeconds) || durationSeconds < 1) throw new Error('DURATION_SECONDS 必须为正整数');
if (!Number.isInteger(probeEvery) || probeEvery < 1) throw new Error('PROBE_EVERY 必须为正整数');

export const options = {
  scenarios: { room_e2e: { executor: 'per-vu-iterations', vus: roomUsers, iterations: 1,
    maxDuration: `${rampSeconds + warmupSeconds + durationSeconds + drainSeconds + 60}s`, gracefulStop: '15s' } },
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
};

const connectionsOpened = new Counter('im_ws_connections_opened');
const connectionErrors = new Counter('im_ws_connection_errors');
const unexpectedCloses = new Counter('im_ws_unexpected_closes');
const messagesSent = new Counter('im_ws_messages_sent');
const sendErrors = new Counter('im_ws_send_errors');
const sendAcksOk = new Counter('im_ws_send_acks_ok');
const sendAcksFailed = new Counter('im_ws_send_acks_failed');
const eventBatchesReceived = new Counter('im_ws_event_batches_received');
const eventBatchAcksSent = new Counter('im_ws_event_batch_acks_sent');
const probeMessagesSent = new Counter('im_ws_probe_messages_sent');
const probeDeliveries = new Counter('im_ws_probe_deliveries');
const probeE2E = new Trend('im_ws_probe_e2e_ms', true);
const syncRequests = new Counter('im_ws_sync_requests');
const syncFailures = new Counter('im_ws_sync_failures');
const syncSeqGapPages = new Counter('im_ws_sync_seq_gap_pages');

function bytes(...parts) {
  const size = parts.reduce((n, p) => n + p.length, 0), out = new Uint8Array(size);
  let at = 0; for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}
function varint(value) {
  const out = []; let n = value;
  do { const b = n % 128; n = Math.floor(n / 128); out.push(b | (n ? 128 : 0)); } while (n);
  return new Uint8Array(out);
}
function utf8(value) { return new Uint8Array([...value].map((char) => char.charCodeAt(0))); }
function bytesField(number, value) { return bytes(varint(number * 8 + 2), varint(value.length), value); }
function stringField(number, value) { return bytesField(number, utf8(value)); }
function intField(number, value) { return bytes(varint(number * 8), varint(value)); }
function frame(op, payload) { return bytes(stringField(1, op), bytesField(2, payload)).buffer; }
function decode(bytesIn) {
  const b = bytesIn instanceof Uint8Array ? bytesIn : new Uint8Array(bytesIn), fields = new Map(); let p = 0;
  while (p < b.length) {
    let tag = 0, mul = 1, v;
    do { if (p >= b.length) throw new Error('truncated_varint'); v = b[p++]; tag += (v & 127) * mul; mul *= 128; } while (v & 128);
    const number = Math.floor(tag / 8), wire = tag % 8;
    if (wire === 0) {
      let n = 0, factor = 1, x;
      do { if (p >= b.length) throw new Error('truncated_varint'); x = b[p++]; n += (x & 127) * factor; factor *= 128; } while (x & 128);
      v = n;
    } else if (wire === 2) {
      let length = 0, factor = 1, x;
      do { if (p >= b.length) throw new Error('truncated_length'); x = b[p++]; length += (x & 127) * factor; factor *= 128; } while (x & 128);
      if (p + length > b.length) throw new Error('truncated_field'); v = b.subarray(p, p + length); p += length;
    } else if (wire === 1 || wire === 5) { p += wire === 1 ? 8 : 4; if (p > b.length) throw new Error('truncated_fixed'); continue; }
    else throw new Error('unsupported_wire');
    if (!fields.has(number)) fields.set(number, []); fields.get(number).push(v);
  }
  return fields;
}
const getBytes = (f, n) => f.get(n)?.[0] || new Uint8Array();
const getText = (f, n) => String.fromCharCode(...getBytes(f, n));
function sendRequest(clientMsgId) {
  return frame('message_send', bytes(stringField(1, clientMsgId), intField(4, 1), stringField(5, 'k6 room load'), stringField(19, conversationId)));
}
function eventBatchAck(batchId) { return frame('event_batch_ack', stringField(1, batchId)); }
function probeIdentity(id) {
  const parts = String(id || '').split('.');
  if (parts.length !== 6 || parts[0] !== 'k6' || parts[1] !== runId || parts[5] === '-') return null;
  const sender = Number(parts[2]), sequence = Number(parts[3]), sentAt = Number(parts[4]), target = Number(parts[5]);
  if (![sender, sequence, sentAt, target].every(Number.isSafeInteger)) return null;
  return { key: `${sender}:${sequence}`, sentAt, target };
}
function examineMessage(message, vu, seen) {
  const probe = probeIdentity(message.clientMsgId);
  if (!probe || probe.target !== vu || seen.has(probe.key)) return;
  seen.add(probe.key);
  const elapsed = Date.now() - probe.sentAt;
  if (elapsed >= 0) { probeE2E.add(elapsed, tags); probeDeliveries.add(1, tags); }
}
function syncRoom(user, vu, cursor, seen, socket, state) {
  if (state.syncing || state.closed) { state.pending = true; return; }
  state.syncing = true;
  state.pending = false;
  const url = `${apiBase}/messages/sync?conversationId=${encodeURIComponent(conversationId)}&afterSeq=${cursor.seq}`;
  http.asyncRequest('GET', url, null, {
    headers: { Authorization: `Bearer ${user.token}` }, responseType: 'text', timeout: '10s', tags: { name: 'room_message_sync', ...tags },
  }).then((response) => {
    syncRequests.add(1, tags);
    if (response.status !== 200) { syncFailures.add(1, tags); return; }
    const messages = JSON.parse(response.body)?.data?.messages;
    if (!Array.isArray(messages)) { syncFailures.add(1, tags); return; }
    let expected = cursor.seq + 1, gap = false;
    for (const item of messages) {
      const seq = Number(item.seq);
      if (seq > cursor.seq && seq !== expected) gap = true;
      if (seq > cursor.seq) expected = seq + 1;
      cursor.seq = Math.max(cursor.seq, seq || 0);
      examineMessage(item, vu, seen);
    }
    if (gap) syncSeqGapPages.add(1, tags);
  }).catch(() => syncFailures.add(1, tags)).finally(() => {
    state.syncing = false;
    if (state.pending && !state.closed) setTimeout(() => syncRoom(user, vu, cursor, seen, socket, state), 100);
  });
}

export function setup() {
  if (!__ENV.CONVERSATION_ID) throw new Error('请设置 CONVERSATION_ID');
  const measureAt = Date.now() + (rampSeconds + warmupSeconds) * 1000;
  const stopAt = measureAt + durationSeconds * 1000;
  const closeAt = stopAt + drainSeconds * 1000;
  return { measureAt, stopAt, closeAt };
}

export default function (timing) {
  const vu = __VU;
  const user = users[vu - 1];
  sleep((rampSeconds * (vu - 1)) / roomUsers);
  const headers = { Authorization: `Bearer ${user.token}`, ...(origin ? { Origin: origin } : {}) };
  let cursorSeq = 0;
  const conversations = http.get(`${apiBase}/conversations`, { headers, tags: { name: 'prepare_conversations', ...tags } });
  if (conversations.status !== 200) { connectionErrors.add(1, tags); return; }
  try {
    const list = JSON.parse(conversations.body).data;
    const conversation = list.find((c) => c.conversationId === conversationId || c.targetId === conversationId);
    cursorSeq = Number(conversation?.latestSeq);
    if (!Number.isSafeInteger(cursorSeq) || cursorSeq < 0) throw new Error('latest_seq_missing');
  } catch (_) { connectionErrors.add(1, tags); return; }

  const socket = new WebSocket(wsUrl, null, { headers: { ...headers, 'X-Device-ID': `${runId}-${vu}`, 'X-Platform': 'web' }, tags: { name: 'room_e2e_ws', ...tags } });
  socket.binaryType = 'arraybuffer';
  const seen = new Set(), cursor = { seq: cursorSeq };
  const state = { syncing: false, pending: false, closed: false };
  const startAt = timing.measureAt, endAt = timing.stopAt, closeAt = timing.closeAt;
  const intervalMs = 1000 / perUserRate;
  const targetVu = ((vu - 1 + 1) % roomUsers) + 1;
  let sequence = 0, sendTimer, beginTimer, drainSyncTimer, closeTimer, deliberateClose = false;

  function sendOne() {
    if (Date.now() >= endAt || socket.readyState !== 1) return;
    const sentAt = Date.now(), seq = sequence++;
    const isProbe = seq % probeEvery === 0;
    const target = isProbe ? targetVu : '-';
    const clientMsgId = `k6.${runId}.${vu}.${seq}.${sentAt}.${target}`;
    try {
      socket.send(sendRequest(clientMsgId));
      messagesSent.add(1, tags);
      if (isProbe) probeMessagesSent.add(1, tags);
    } catch (_) { sendErrors.add(1, tags); }
  }

  function business(op, payload) {
    const f = decode(payload);
    if (op === 'message_send_ack') {
      if (getText(f, 3) === 'failed') sendAcksFailed.add(1, tags); else sendAcksOk.add(1, tags);
    } else if (op === 'message_push') {
      const id = getText(f, 11);
      const seq = Number(f.get(5)?.[0] || 0);
      cursor.seq = Math.max(cursor.seq, seq);
      examineMessage({ clientMsgId: id }, vu, seen);
    } else if (op === 'room_message_notice') {
      syncRoom(user, vu, cursor, seen, socket, state);
    }
  }

  socket.onopen = () => {
    connectionsOpened.add(1, tags);
    syncRoom(user, vu, cursor, seen, socket, state);
    beginTimer = setTimeout(() => {
      if (Date.now() >= startAt) { connectionErrors.add(1, tags); return; }
      sendOne();
      sendTimer = setInterval(sendOne, intervalMs);
    }, Math.max(0, startAt - Date.now()));
    drainSyncTimer = setTimeout(() => syncRoom(user, vu, cursor, seen, socket, state), Math.max(0, endAt - Date.now()));
    closeTimer = setTimeout(() => {
      if (sendTimer !== undefined) clearInterval(sendTimer);
      state.closed = true;
      deliberateClose = true;
      socket.close();
    }, Math.max(0, closeAt - Date.now()));
  };
  socket.onmessage = ({ data }) => {
    try {
      const outer = decode(data), op = getText(outer, 1), payload = getBytes(outer, 2);
      if (op === 'event_batch') {
        const batch = decode(payload), batchId = getText(batch, 2);
        for (const innerBytes of batch.get(1) || []) {
          const inner = decode(innerBytes);
          business(getText(inner, 1), getBytes(inner, 2));
        }
        if (batchId) socket.send(eventBatchAck(batchId));
        eventBatchesReceived.add(1, tags);
        eventBatchAcksSent.add(1, tags);
      } else business(op, payload);
    } catch (_) { connectionErrors.add(1, tags); }
  };
  socket.onerror = () => { connectionErrors.add(1, tags); };
  socket.onclose = () => {
    state.closed = true;
    if (sendTimer !== undefined) clearInterval(sendTimer);
    if (beginTimer !== undefined) clearTimeout(beginTimer);
    if (drainSyncTimer !== undefined) clearTimeout(drainSyncTimer);
    if (closeTimer !== undefined) clearTimeout(closeTimer);
    if (!deliberateClose) unexpectedCloses.add(1, tags);
  };
}
