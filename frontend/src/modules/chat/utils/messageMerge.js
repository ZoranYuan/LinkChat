/** 将消息 seq 转为数字，统一 WebSocket 和 HTTP 消息的内存表示。 */
export const normalizeMessage = (message = {}) => ({
  ...message,
  seq: Number(message.seq) || 0,
});

/** 按服务端 seq 升序排列消息，未确认消息放在末尾。 */
export const sortMessages = (messages = []) =>
  [...messages].sort(
    (left, right) =>
      (Number(left.seq) || Number.MAX_SAFE_INTEGER) -
      (Number(right.seq) || Number.MAX_SAFE_INTEGER),
  );

/** 合并本地、同步和实时消息，并按 seq/clientMsgId 消除重复记录。 */
export const mergeMessageLists = (existing = [], incoming = []) => {
  const bySeq = new Map();
  const pendingByClientMsgId = new Map();
  const pendingWithoutClientMsgId = [];
  const confirmedClientMsgIds = new Set();

  for (const rawMessage of [...existing, ...incoming]) {
    const message = normalizeMessage(rawMessage);
    const seq = Number(message.seq);
    const clientMsgId = message.clientMsgId || "";

    if (seq > 0) {
      const previous = bySeq.get(seq);
      bySeq.set(seq, { ...previous, ...message });
      if (clientMsgId) {
        confirmedClientMsgIds.add(clientMsgId);
        pendingByClientMsgId.delete(clientMsgId);
      }
      continue;
    }

    if (clientMsgId) {
      if (!confirmedClientMsgIds.has(clientMsgId)) {
        pendingByClientMsgId.set(clientMsgId, message);
      }
      continue;
    }

    // 没有 seq 和 clientMsgId 时没有可靠的去重键，只能保留原消息。
    pendingWithoutClientMsgId.push(message);
  }

  return sortMessages([
    ...bySeq.values(),
    ...pendingByClientMsgId.values(),
    ...pendingWithoutClientMsgId,
  ]);
};
