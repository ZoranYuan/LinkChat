import ws from 'k6/ws';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const fixture = __ENV.FIXTURE_FILE ? JSON.parse(open(__ENV.FIXTURE_FILE)) : {};
const tokens = JSON.parse(__ENV.K6_TOKENS || JSON.stringify(fixture.tokens || []));
const durationSeconds = Number(__ENV.DURATION_SECONDS || 60);
const sendRate = Number(__ENV.SEND_RATE_PER_USER || 8);
const intervalMs = Math.floor(1000 / sendRate);
const runId = `${Date.now()}`;
const metricTags = { arm: __ENV.ARM || 'current' };

export const options = {
  scenarios: {
    room_connections: {
      executor: 'per-vu-iterations',
      vus: Math.max(tokens.length, 1),
      iterations: 1,
      maxDuration: `${durationSeconds + 20}s`,
    },
  },
};

const sent = new Counter('im_ws_messages_sent');
const ackSent = new Counter('im_ws_ack_sent');
const ackFailed = new Counter('im_ws_ack_failed');
const roomMessages = new Counter('im_ws_room_messages_received');
const duplicateMessages = new Counter('im_ws_duplicate_messages');
const serverBatches = new Counter('im_ws_server_batches_with_messages');
const messageBatchSize = new Trend('im_ws_messages_per_server_batch');
const ackLatency = new Trend('im_ws_ack_latency_ms');

function varint(value) {
  let n = Number(value);
  const out = [];
  while (n > 127) {
    out.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
  }
  out.push(n);
  return out;
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
      const b = bytes[offset++];
      value += (b & 0x7f) * (2 ** shift);
      if ((b & 0x80) === 0) return value;
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
    } else if (wire === 1) {
      fields.push({ number, wire, value: bytes.slice(offset, offset + 8) });
      offset += 8;
    } else if (wire === 5) {
      fields.push({ number, wire, value: bytes.slice(offset, offset + 4) });
      offset += 4;
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

function sendFrame(socket, op, payload) {
  const encoded = new Uint8Array([
    ...stringField(1, op),
    ...bytesField(2, payload),
  ]);
  socket.sendBinary(encoded.buffer);
}

function sendChatMessage(socket, conversationId, clientMsgId) {
  const content = `k6-${clientMsgId}`;
  const request = new Uint8Array([
    ...stringField(1, clientMsgId),
    ...stringField(5, content),
    ...stringField(19, conversationId),
  ]);
  sendFrame(socket, 'message_send', request);
}

function handleServerFrame(socket, raw, pending, seen) {
  const outer = parseFields(raw);
  const op = firstString(outer, 1);
  if (op !== 'event_batch') return;

  const batch = parseFields(firstBytes(outer, 2));
  const batchId = firstString(batch, 2);
  if (!batchId) throw new Error('事件批次缺少 batchId');
  const frames = batch.filter((field) => field.number === 1 && field.wire === 2);
  let messageCount = 0;

  for (const entry of frames) {
    const inner = parseFields(entry.value);
    const eventOp = firstString(inner, 1);
    const payload = firstBytes(inner, 2);

    if (eventOp === 'message_send_result') {
      const ack = parseFields(payload);
      const clientMsgId = firstString(ack, 1);
      const status = firstString(ack, 3);
      const startedAt = pending[clientMsgId];
      if (startedAt !== undefined) {
        ackLatency.add(Date.now() - startedAt, metricTags);
        delete pending[clientMsgId];
        if (status === 'processing' || status === 'completed') ackSent.add(1, metricTags);
        else ackFailed.add(1, metricTags);
      }
      continue;
    }

    if (eventOp !== 'message_push') continue;
    const event = parseFields(payload);
    const clientMsgId = firstString(event, 11);
    const content = firstString(event, 8);
    messageCount++;
    roomMessages.add(1, metricTags);

    if (clientMsgId && seen[clientMsgId]) duplicateMessages.add(1, metricTags);
    if (clientMsgId) seen[clientMsgId] = true;

    if (!content.startsWith('k6-')) continue;
  }

  if (messageCount > 0) {
    serverBatches.add(1, metricTags);
    messageBatchSize.add(messageCount, metricTags);
  }
  sendFrame(socket, 'event_batch_ack', stringField(1, batchId));
}

export default function () {
  const conversationId = __ENV.CONVERSATION_ID || fixture.conversation_id;
  const wsUrl = __ENV.WS_URL || 'ws://127.0.0.1:8081/api/v1/ws';
  if (tokens.length === 0 || !conversationId) {
    throw new Error('Set FIXTURE_FILE or K6_TOKENS and CONVERSATION_ID before running.');
  }

  const vuIndex = (__VU - 1) % tokens.length;
  const token = tokens[vuIndex];
  const pending = {};
  const seen = {};
  let messageIndex = 0;

  const response = ws.connect(wsUrl, {
    headers: {
      Authorization: `Bearer ${token}`,
      'X-Device-ID': `k6-${runId}-${__VU}`,
      'X-Platform': 'web',
    },
    tags: { name: 'linkchat_room_ws' },
  }, (socket) => {
    socket.on('binaryMessage', (data) => {
      try {
        handleServerFrame(socket, data, pending, seen);
      } catch (_) {
        // Malformed/unrelated server frames are excluded from benchmark counters.
      }
    });

    socket.on('open', () => {
      let sending = true;
      socket.setInterval(() => {
        if (!sending) return;
        const clientMsgId = `${runId}-${__VU}-${++messageIndex}`;
        pending[clientMsgId] = Date.now();
        sendChatMessage(socket, conversationId, clientMsgId);
        sent.add(1, metricTags);
      }, intervalMs);
      socket.setTimeout(() => { sending = false; }, durationSeconds * 1000);
      socket.setTimeout(() => socket.close(), (durationSeconds + 5) * 1000);
    });
  });

  check(response, { 'WebSocket upgraded': (result) => result && result.status === 101 });
}
