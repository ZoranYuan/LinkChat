import { SharedArray } from 'k6/data';
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { clearInterval, clearTimeout, setInterval, setTimeout } from 'k6/timers';
import { WebSocket } from 'k6/websockets';

const fixturePath = __ENV.FIXTURE_FILE;
if (!fixturePath) throw new Error('请设置 FIXTURE_FILE');

const fixture = JSON.parse(open(fixturePath));
const users = new SharedArray('LinkChat room users', () => fixture.users);
const roomUsers = Number(__ENV.ROOM_USERS || 500);
const ratePerUser = Number(__ENV.RATE_PER_USER || 1);
const rampSeconds = Number(__ENV.RAMP_SECONDS || 30);
const warmupSeconds = Number(__ENV.WARMUP_SECONDS || 5);
const durationSeconds = Number(__ENV.DURATION_SECONDS || 20);
const drainSeconds = Number(__ENV.DRAIN_SECONDS || 15);
const runId = __ENV.RUN_ID || `room-host-${Date.now()}`;
const conversationId = __ENV.CONVERSATION_ID || fixture.conversationId;
const apiBase = (__ENV.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const wsUrl = __ENV.WS_URL || `${apiBase.replace(/^http/, 'ws')}/ws`;
const origin = __ENV.WEB_ORIGIN || 'http://127.0.0.1:5173';

if (!Number.isInteger(roomUsers) || roomUsers < 1 || users.length < roomUsers) {
  throw new Error(`ROOM_USERS=${roomUsers}，但凭据文件只有 ${users.length} 个账号`);
}
if (!Number.isInteger(ratePerUser) || ratePerUser < 1) throw new Error('RATE_PER_USER 必须是正整数');
if (!conversationId) throw new Error('fixture 缺少 conversationId');

const plannedSeconds = rampSeconds + warmupSeconds + durationSeconds + drainSeconds;

export const options = {
  scenarios: {
    room_host: {
      executor: 'per-vu-iterations',
      vus: roomUsers,
      iterations: 1,
      maxDuration: `${plannedSeconds + 10}s`,
      gracefulStop: '5s',
    },
  },
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
};

const runTags = { run_id: runId, loadgen: 'host' };
const connectionsOpened = new Counter('im_ws_connection_opened');
const connectionErrors = new Counter('im_ws_connection_errors');
const unexpectedCloses = new Counter('im_ws_unexpected_closes');
const messagesSent = new Counter('im_ws_messages_sent');
const sendAcksReceived = new Counter('im_ws_send_acks_received');
const sendAcksFailed = new Counter('im_ws_send_acks_failed');
const batchesReceived = new Counter('im_ws_event_batches_received');
const batchAcksSent = new Counter('im_ws_event_batch_acks_sent');
const batchProcessingErrors = new Counter('im_ws_event_batch_processing_errors');
const syncRequests = new Counter('im_ws_sync_requests');
const syncFailures = new Counter('im_ws_sync_failures');
const syncIncomplete = new Counter('im_ws_sync_incomplete');
const probeE2EReceived = new Counter('im_ws_probe_e2e_received');
const syncHTTPMs = new Trend('im_ws_sync_http_latency_ms', true);
const noticeToSyncMs = new Trend('im_ws_notice_to_sync_ms', true);
const probeE2EMs = new Trend('im_ws_probe_e2e_ms', true);
const probePushE2EMs = new Trend('im_ws_probe_push_e2e_ms', true);
const probeSyncE2EMs = new Trend('im_ws_probe_sync_e2e_ms', true);

const encodeText = (value) => new Uint8Array([...value].map((char) => char.charCodeAt(0)));
const decodeText = (value) => String.fromCharCode(...value);

function concatBytes(...parts) {
  const result = new Uint8Array(parts.reduce((size, part) => size + part.length, 0));
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

function fieldBytes(fields, number, index = 0) {
  return fields.get(number)?.[index] || new Uint8Array();
}

function fieldString(fields, number) {
  return decodeText(fieldBytes(fields, number));
}

function fieldInt(fields, number) {
  return Number(fields.get(number)?.[0] || 0);
}

function parseProbe(content) {
  const parts = String(content || '').split('|');
  if (parts.length !== 6 || parts[0] !== 'probe' || parts[1] !== runId) return null;
  const sentAt = Number(parts[3]);
  const targetVu = Number(parts[4]);
  if (!Number.isFinite(sentAt) || !Number.isInteger(targetVu)) return null;
  return { clientMsgId: parts[2], sentAt, targetVu };
}

export function setup() {
  const response = http.get(`${apiBase}/conversations`, {
    headers: { Authorization: `Bearer ${users[0].token}` },
    tags: { name: 'setup_conversations' },
    timeout: '10s',
  });
  if (response.status !== 200) {
    throw new Error(`压测前获取会话快照失败：HTTP ${response.status}`);
  }
  const payload = response.json();
  const conversations = payload?.data;
  const conversation = Array.isArray(conversations)
    ? conversations.find((item) => item.conversationId === conversationId)
    : null;
  const initialSeq = Number(conversation?.latestSeq);
  if (!Number.isInteger(initialSeq) || initialSeq < 0) {
    throw new Error(`压测前未找到会话 ${conversationId} 的有效 latestSeq`);
  }
  return {
    initialSeq,
    measureAt: Date.now() + (rampSeconds + warmupSeconds) * 1000,
  };
}

export default async function (timing) {
  const vu = __VU;
  const user = users[vu - 1];
  await sleep((rampSeconds * (vu - 1)) / roomUsers);

  const socket = new WebSocket(wsUrl, null, {
    headers: {
      Authorization: `Bearer ${user.token}`,
      Origin: origin,
      'X-Device-ID': `${runId}-${vu}`,
      'X-Platform': 'web',
    },
    tags: { name: 'room_host_ws' },
  });
  socket.binaryType = 'arraybuffer';

  const endAt = timing.measureAt + durationSeconds * 1000;
  const closeAt = endAt + drainSeconds * 1000;
  const targetVu = (vu % roomUsers) + 1;
  const seenProbes = new Set();
  let continuousSeq = timing.initialSeq;
  let requestedSyncSeq = timing.initialSeq;
  let sendSequence = 0;
  let sendTimer;
  let startTimer;
  let closeTimer;
  let syncTask;
  let intentionalClose = false;
  let resolveSocket;

  const socketClosed = new Promise((resolve) => {
    resolveSocket = resolve;
  });

  const stopSending = () => {
    if (sendTimer !== undefined) clearInterval(sendTimer);
    if (startTimer !== undefined) clearTimeout(startTimer);
  };

  const sendOne = () => {
    if (Date.now() >= endAt || socket.readyState !== 1) {
      stopSending();
      return;
    }
    const sentAt = Date.now();
    const clientMsgId = `${runId}-${vu}-${sendSequence++}`;
    const content = `probe|${runId}|${clientMsgId}|${sentAt}|${targetVu}|e2e`;
    socket.send(encodeSendRequest(clientMsgId, content));
    messagesSent.add(1, runTags);
  };

  const recordProbe = (content, path) => {
    const probe = parseProbe(content);
    if (!probe || probe.targetVu !== vu || seenProbes.has(probe.clientMsgId)) return;
    seenProbes.add(probe.clientMsgId);
    const elapsed = Date.now() - probe.sentAt;
    probeE2EReceived.add(1, runTags);
    probeE2EMs.add(elapsed, runTags);
    if (path === 'push') probePushE2EMs.add(elapsed, runTags);
    else probeSyncE2EMs.add(elapsed, runTags);
  };

  const advanceFromMessages = (messages) => {
    const ordered = [...messages].sort((left, right) => Number(left.seq) - Number(right.seq));
    for (const message of ordered) {
      recordProbe(message.content, 'sync');
      const seq = Number(message.seq) || 0;
      if (seq <= continuousSeq) continue;
      if (seq !== continuousSeq + 1) break;
      continuousSeq = seq;
    }
  };

  const performSync = async () => {
    const url = `${apiBase}/messages/sync?conversationId=${encodeURIComponent(conversationId)}&afterSeq=${continuousSeq}`;
    const startedAt = Date.now();
    syncRequests.add(1, runTags);
    const response = await http.asyncRequest('GET', url, null, {
      headers: { Authorization: `Bearer ${user.token}` },
      tags: { name: 'room_message_sync' },
      timeout: 15000,
    });
    syncHTTPMs.add(Date.now() - startedAt, runTags);
    if (response.status !== 200) {
      syncFailures.add(1, runTags);
      return;
    }
    const messages = response.json()?.data?.messages || [];
    advanceFromMessages(messages);
  };

  const syncToTarget = (targetSeq) => {
    requestedSyncSeq = Math.max(requestedSyncSeq, targetSeq);
    if (syncTask) return syncTask;

    syncTask = (async () => {
      while (continuousSeq < requestedSyncSeq && Date.now() < closeAt) {
        const beforeSeq = continuousSeq;
        await performSync();
        if (continuousSeq <= beforeSeq) {
          syncIncomplete.add(1, runTags);
          break;
        }
      }
    })().finally(() => {
      syncTask = undefined;
    });
    return syncTask;
  };

  const handleBusinessFrame = async (op, payloadBytes) => {
    const payload = decodeFields(payloadBytes);
    if (op === 'message_push') {
      const seq = fieldInt(payload, 5);
      recordProbe(fieldString(payload, 8), 'push');
      if (seq === continuousSeq + 1) continuousSeq = seq;
      else if (seq > continuousSeq + 1) await syncToTarget(seq);
      return;
    }
    if (op === 'message_send_ack') {
      sendAcksReceived.add(1, runTags);
      if (fieldString(payload, 3) === 'failed') sendAcksFailed.add(1, runTags);
      return;
    }
    if (op === 'room_message_notice') {
      const noticeAt = Date.now();
      const targetSeq = fieldInt(payload, 3);
      if (targetSeq > continuousSeq) {
        await syncToTarget(targetSeq);
        if (continuousSeq >= targetSeq) noticeToSyncMs.add(Date.now() - noticeAt, runTags);
        else syncIncomplete.add(1, runTags);
      }
    }
  };

  const handleSocketFrame = async (rawFrame) => {
    const frame = decodeFields(rawFrame);
    const op = fieldString(frame, 1);
    const payload = fieldBytes(frame, 2);
    if (op !== 'event_batch') {
      await handleBusinessFrame(op, payload);
      return;
    }

    const batch = decodeFields(payload);
    const batchId = fieldString(batch, 2);
    if (!batchId) throw new Error('event_batch 缺少 batch_id');

    batchesReceived.add(1, runTags);
    for (let index = 0; index < (batch.get(1) || []).length; index++) {
      const innerFrame = decodeFields(fieldBytes(batch, 1, index));
      await handleBusinessFrame(fieldString(innerFrame, 1), fieldBytes(innerFrame, 2));
    }

    if (socket.readyState === 1) {
      socket.send(encodeBatchAck(batchId));
      batchAcksSent.add(1, runTags);
    }
  };

  socket.onopen = () => {
    connectionsOpened.add(1, runTags);
    startTimer = setTimeout(() => {
      sendOne();
      sendTimer = setInterval(sendOne, 1000 / ratePerUser);
    }, Math.max(0, timing.measureAt - Date.now()));
    closeTimer = setTimeout(() => {
      stopSending();
      const closeSocket = () => {
        intentionalClose = true;
        socket.close();
      };
      if (syncTask) syncTask.finally(closeSocket);
      else closeSocket();
    }, Math.max(0, closeAt - Date.now()));
  };

  socket.onmessage = ({ data }) => {
    handleSocketFrame(data).catch(() => {
      batchProcessingErrors.add(1, runTags);
    });
  };

  socket.onerror = () => {
    connectionErrors.add(1, runTags);
    stopSending();
    if (closeTimer !== undefined) clearTimeout(closeTimer);
    socket.close();
  };

  socket.onclose = () => {
    stopSending();
    if (closeTimer !== undefined) clearTimeout(closeTimer);
    if (!intentionalClose) unexpectedCloses.add(1, runTags);
    resolveSocket();
  };

  await socketClosed;
}
