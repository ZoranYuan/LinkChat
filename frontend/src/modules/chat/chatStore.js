import { defineStore } from "pinia";
import { computed, reactive, watch } from "vue";
import {
  createFriendRequest,
  createRoom as createRoomRequest,
  getAttachmentAccessURLs,
  getConversations,
  getOfflineMessages,
  getFriendRequests,
  getFriends,
  joinRoom as joinRoomRequest,
  loginUser,
  logoutUser,
  operateFriendRequest,
  refreshSession,
  registerUser,
  syncMessages,
  updateUserProfile,
} from "../../services/http/api.js";
import { useFileUpload } from "../../composables/useFileUpload.js";
import { ConversationType } from "../../constants/conversation.js";
import { MessageType } from "../../constants/message.js";
import {
  clearConversations,
  insertConversations,
  queryConversationsByUserId,
  replaceConversations,
} from "../../db/conversationRepository.js";
import {
  clearMessages,
  deleteMessagesByConversation,
  insertMessages,
  queryMessagesByCursor,
} from "../../db/messageRepository.js";
import { createWsClient } from "../../services/websocket/client.js";
import { createAttachmentResolver } from "./services/attachmentService.js";
import { authUserProfile, initialUser, persistSession } from "./services/authService.js";
import {
  applyMessagesToConversation,
  findConversation,
  sortConversations,
} from "./utils/conversationState.js";
import {
  createClientMessageId,
  createConversationPreview,
  createMessageRequest,
  createOutgoingMessage,
} from "./utils/messageComposer.js";
import { mergeMessageLists, normalizeMessage } from "./utils/messageMerge.js";

const HISTORY_PAGE_SIZE = 30;
const THEME_CONTACT_AVATAR = "#8b72d6";

const state = reactive({
  authenticated: false,
  currentUser: initialUser(),
  conversations: [],
  messages: {},
  contacts: [],
  friendRequests: [],
  activeConversationId: "",
  connection: "disconnected",
  loading: false,
  historyLoading: false,
  historyHasMore: {},
  historyCursor: {},
});

const attachmentResolver = createAttachmentResolver(getAttachmentAccessURLs);
const pendingMessages = new Map();
// 独立保存 WS 请求数据，失败重试不依赖会话展示对象。
const pendingSendRequests = new Map();
// 只在内存中保存上传任务，便于失败后复用 File 或已完成的 fileId 重试。
const pendingUploadTasks = new Map();
const historyRequests = new Map();
const messageProcessingByConversation = new Map();
let pendingRoomEntryConversationID = "";

let sessionGeneration = 0;
let startupCatchupGeneration = -1;
let cacheClearPromise = Promise.resolve();
let authBootstrapPromise = null;
let historyRequestSequence = 0;

/** 返回当前登录用户 ID，作为本地缓存和会话请求的隔离边界。 */
const currentUserId = () => state.currentUser?.userId || "";

/** 捕获当前认证代次，防止旧请求在切换用户后回写新会话数据。 */
const captureSession = () => ({
  generation: sessionGeneration,
  userId: currentUserId(),
});
/** 判断异步操作对应的用户会话是否仍然有效。 */
const isSessionActive = ({ generation, userId }) =>
  state.authenticated &&
  generation === sessionGeneration &&
  userId !== "" &&
  userId === currentUserId();

const formatTime = (timestamp) => {
  if (!timestamp) return "";
  const date = new Date(Number(timestamp));
  const now = new Date();
  if (date.toDateString() === now.toDateString()) {
    return date.toLocaleTimeString("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  }
  return `${date.getMonth() + 1}/${date.getDate()}`;
};

/** 原子持久化确认消息和调用方已确认的本地连续序号。 */
const persistConfirmedMessages = async (
  messages,
  { session = captureSession(), lastContinuousSeqByConversation = {} } = {},
) => {
  if (!isSessionActive(session)) return;

  const result = await insertMessages(messages, {
    userId: session.userId,
    lastContinuousSeqByConversation,
  });

  if (!isSessionActive(session)) return result;

  for (const [conversationId, seq] of Object.entries(
    result?.lastContinuousSeqByConversation || {},
  )) {
    const conversation = findConversation(state.conversations, conversationId);
    if (!conversation) continue;

    conversation.lastContinuousSeq = Math.max(
      Number(conversation.lastContinuousSeq) || 0,
      Number(seq) || 0,
    );
  }

  return result;
};

/** 将会话快照写入当前用户的本地会话表。 */
const persistConversation = (conversation, session = captureSession()) => {
  if (!conversation || !isSessionActive(session)) return Promise.resolve();
  return insertConversations(session.userId, [conversation]).catch(() => {});
};

/** 合并已持久化的消息并解析附件展示信息。 */
const mergeMessages = async (conversationId, incomingItems, session) => {
  const mergedMessages = mergeMessageLists(state.messages[conversationId] || [], incomingItems);
  await attachmentResolver.resolve(mergedMessages);
  if (!isSessionActive(session)) return;
  state.messages[conversationId] = mergedMessages;
};

/** 按最新消息时间重新排列内存中的会话列表。 */
const updateConversationList = () => {
  state.conversations = sortConversations(state.conversations);
};

/** 按 clientMsgId 找到内存中的发送消息。 */
const findPendingMessage = (clientMsgId, conversationId) => {
  if (!clientMsgId) return null;
  return (
    (state.messages[conversationId] || []).find(
      (message) => message.clientMsgId === clientMsgId,
    ) || null
  );
};

/** 更新发送消息的前端临时投递状态，不触碰 IndexedDB。 */
const updatePendingMessage = (clientMsgId, conversationId, changes) => {
  const message = findPendingMessage(clientMsgId, conversationId);
  if (!message) return null;

  Object.assign(message, changes);
  pendingMessages.set(clientMsgId, message);
  return message;
};

const markPendingMessageFailed = (clientMsgId, conversationId, error) =>
  updatePendingMessage(clientMsgId, conversationId, {
    error: error?.message || String(error || "消息发送失败"),
  });

/** 为图片或视频生成本地预览地址；该地址只存在于内存中。 */
const createLocalPreviewURL = (file, cType) => {
  if (
    !file ||
    (Number(cType) !== MessageType.IMAGE && Number(cType) !== MessageType.VIDEO) ||
    !globalThis.URL?.createObjectURL
  )
    return "";
  return globalThis.URL.createObjectURL(file);
};

/** 服务端访问地址准备好后释放本地预览地址，避免 Blob URL 泄漏。 */
const releaseUploadPreview = (clientMsgId, message) => {
  const task = pendingUploadTasks.get(clientMsgId);
  if (!task) return;

  const hasRemoteURL = message?.mediaUrl && !message.mediaUrl.startsWith("blob:");
  if (task.previewURL && hasRemoteURL && globalThis.URL?.revokeObjectURL) {
    globalThis.URL.revokeObjectURL(task.previewURL);
    task.previewURL = "";
  }

  if (!task.previewURL) pendingUploadTasks.delete(clientMsgId);
};

/** 将统一上传组件的阶段和进度同步到对应的消息气泡。 */
const watchUploadTask = (task, session) =>
  watch([task.uploader.status, task.uploader.progress], ([uploadStatus, uploadProgress]) => {
    if (!isSessionActive(session)) return;
    updatePendingMessage(task.clientMsgId, task.conversationId, {
      uploadStage: uploadStatus,
      uploadProgress: Number(uploadProgress) || 0,
      error: ["canceled", "failed"].includes(uploadStatus) ? "上传失败" : "",
    });
  });

/** 发送独立的 WS 请求对象；失败时保留请求供用户重试。 */
const sendPendingMessage = (clientMsgId, conversation, request, session = captureSession()) => {
  const message = findPendingMessage(clientMsgId, conversation?.conversationId);
  if (!message || !request || !isSessionActive(session)) return null;

  const pendingRequest = { ...request, clientMsgId };
  if (Number(pendingRequest.cType) !== MessageType.TEXT && !pendingRequest.fileId) {
    throw new Error("媒体文件尚未上传完成，无法发送消息。");
  }
  pendingSendRequests.set(clientMsgId, pendingRequest);

  updatePendingMessage(clientMsgId, conversation.conversationId, {
    error: "",
  });

  try {
    if (!wsClient.sendMessage({ ...pendingRequest })) {
      throw new Error("消息发送失败，请重新连接后重试。");
    }
  } catch (error) {
    markPendingMessageFailed(clientMsgId, conversation.conversationId, error);
    throw error;
  }

  return findPendingMessage(clientMsgId, conversation.conversationId);
};

/** 上传一个附件任务，并在上传完成后复用同一个 clientMsgId 发消息。 */
const uploadAndSendAttachment = async (task, conversation, session) => {
  const stopWatching = watchUploadTask(task, session);
  try {
    const uploaded = await task.uploader.upload(task.file);
    if (!isSessionActive(session)) return null;

    task.fileId = uploaded.fileId;
    updatePendingMessage(task.clientMsgId, task.conversationId, {
      uploadStage: "completed",
      uploadProgress: 100,
      error: "",
    });

    const request = createMessageRequest(conversation, {
      clientMsgId: task.clientMsgId,
      cType: task.cType,
      fileId: task.fileId,
    });
    return sendPendingMessage(task.clientMsgId, conversation, request, session);
  } catch (error) {
    if (isSessionActive(session)) {
      markPendingMessageFailed(task.clientMsgId, task.conversationId, error);
    }
    throw error;
  } finally {
    stopWatching();
  }
};

const MAX_CONCURRENT_UPLOADS = 3;
const uploadQueue = [];
let activeUploadCount = 0;

/** 启动等待中的上传任务，限制同时进行的文件上传数量。 */
const drainUploadQueue = () => {
  while (activeUploadCount < MAX_CONCURRENT_UPLOADS && uploadQueue.length) {
    const task = uploadQueue.shift();
    if (!task || task.settled) continue;

    if (task.cancelRequested) {
      task.settled = true;
      task.reject(new Error("上传已取消"));
      continue;
    }

    activeUploadCount += 1;
    task.running = true;
    uploadAndSendAttachment(task, task.conversation, task.session)
      .then(task.resolve)
      .catch(task.reject)
      .finally(() => {
        activeUploadCount -= 1;
        task.running = false;
        task.settled = true;
        drainUploadQueue();
      });
  }
};

/** 将附件任务放入上传队列；占位消息在入队前就已经创建。 */
const enqueueUploadTask = (task) =>
  new Promise((resolve, reject) => {
    task.resolve = resolve;
    task.reject = reject;
    uploadQueue.push(task);
    drainUploadQueue();
  });

/** 同一会话顺序处理实时消息，避免并发请求基于相同边界重复同步。 */
const enqueueConversationMessage = (conversationId, task) => {
  const previous = messageProcessingByConversation.get(conversationId) || Promise.resolve();
  const current = previous.catch(() => {}).then(task);
  messageProcessingByConversation.set(conversationId, current);
  return current.finally(() => {
    if (messageProcessingByConversation.get(conversationId) === current) {
      messageProcessingByConversation.delete(conversationId);
    }
  });
};

const isOrderedAfter = (messages, afterSeq) => {
  let previousSeq = Number(afterSeq) || 0;
  for (const message of messages) {
    const seq = Number(message.seq);
    if (!Number.isSafeInteger(seq) || seq <= previousSeq) return false;
    previousSeq = seq;
  }
  return true;
};

/** 在会话快照固定水位内分页补齐离线消息，水位之后的新消息留给 WebSocket 阶段追赶。 */
const catchUpOfflineConversation = async (conversation, session) => {
  const conversationId = conversation?.conversationId || "";
  const snapshotSeq = Number(conversation?.latestSeq) || 0;
  let cursor = Number(conversation?.lastContinuousSeq) || 0;
  if (!conversationId || cursor >= snapshotSeq) return;

  while (cursor < snapshotSeq) {
    const previousCursor = cursor;
    const page = await getOfflineMessages({
      conversationId,
      afterSeq: cursor,
      snapshotSeq,
      limit: 10,
    });
    if (!isSessionActive(session)) return;

    const messages = (page?.messages || []).map(normalizeMessage);
    if (messages.length && !isOrderedAfter(messages, cursor)) {
      throw new Error(`离线消息序号重复或乱序：${conversationId}`);
    }

    const nextCursor = Number(page?.nextCursor);
    if (!Number.isSafeInteger(nextCursor) || nextCursor < cursor || nextCursor > snapshotSeq) {
      throw new Error(`离线消息游标无效：${conversationId}`);
    }
    if (messages.length && Number(messages.at(-1)?.seq) > nextCursor) {
      throw new Error(`离线消息游标落后于消息：${conversationId}`);
    }
    if (page?.hasMore && !messages.length) {
      throw new Error(`离线分页没有消息但仍有后续页：${conversationId}`);
    }

    const persisted = await persistConfirmedMessages(messages, {
      session,
      lastContinuousSeqByConversation: { [conversationId]: nextCursor },
    });
    if (!isSessionActive(session)) return;

    cursor = Number(persisted?.lastContinuousSeqByConversation?.[conversationId]) || nextCursor;
    conversation.lastContinuousSeq = cursor;

    if (page?.hasMore && cursor <= previousCursor) {
      throw new Error(`离线分页游标未推进：${conversationId}`);
    }
    if (!page?.hasMore) break;
  }
};

/** 并发补齐各会话的固定快照；任一失败时不建立 WebSocket。 */
const catchUpAllOfflineConversations = async (session = captureSession()) => {
  const results = await Promise.allSettled(
    state.conversations.map((conversation) => catchUpOfflineConversation(conversation, session)),
  );
  if (!isSessionActive(session)) return;
  const failure = results.find((result) => result.status === "rejected");
  if (failure) throw failure.reason;
};

/**
 * 处理一条带完整内容和服务端 seq 的实时消息：连续则直接提交，存在缺口则从本地边界同步。
 */
const processRealtimeMessage = async (rawMessage) => {
  const session = captureSession();
  let message = normalizeMessage(rawMessage);
  const conversationId = message.conversationId || "";
  const conversation = findConversation(state.conversations, conversationId);
  if (
    !isSessionActive(session) ||
    !conversation ||
    !message.messageId ||
    Number(message.seq) <= 0
  ) {
    return;
  }

  const pending = message.clientMsgId
    ? findPendingMessage(message.clientMsgId, conversationId)
    : null;
  if (pending) message = normalizeMessage({ ...pending, ...message });

  return enqueueConversationMessage(conversationId, async () => {
    if (!isSessionActive(session)) return;

    const localBoundary = Number(conversation.lastContinuousSeq) || 0;
    const messageSeq = Number(message.seq);

    let messagesToPersist = [message];
    let nextBoundary = messageSeq;
    if (messageSeq > localBoundary + 1) {
      const syncResult = await syncMessages(conversationId, localBoundary);
      if (!isSessionActive(session)) return;

      const synchronized = (syncResult?.messages || [])
        .map(normalizeMessage)
        .filter((item) => item.messageId && Number(item.seq) > 0);
      const bySeq = new Map(synchronized.map((item) => [Number(item.seq), item]));
      bySeq.set(messageSeq, message);
      messagesToPersist = [...bySeq.values()].sort(
        (left, right) => Number(left.seq) - Number(right.seq),
      );

      if (!isOrderedAfter(messagesToPersist, localBoundary)) {
        throw new Error("消息同步结果存在重复或乱序");
      }
      nextBoundary = Number(messagesToPersist.at(-1)?.seq) || localBoundary;
    }

    const persisted = await persistConfirmedMessages(messagesToPersist, {
      session,
      lastContinuousSeqByConversation: { [conversationId]: nextBoundary },
    });
    if (!isSessionActive(session)) return;

    conversation.lastContinuousSeq =
      Number(persisted?.lastContinuousSeqByConversation?.[conversationId]) || nextBoundary;

    const active = state.activeConversationId === conversationId;
    if (active) {
      await mergeMessages(conversationId, messagesToPersist, session);
    }

    applyMessagesToConversation(
      conversation,
      messagesToPersist,
      state.activeConversationId,
      currentUserId(),
    );
    updateConversationList();
    await persistConversation(conversation, session);

    sendMessageRead(conversationId);
  });
};

/** WebSocket 完整消息是本地消息库的唯一实时更新入口。 */
const upsertIncomingMessage = (payload) => {
  processRealtimeMessage(payload).catch(() => {});
};

/** ACK 只更新发送状态；带 seq 的消息推送负责持久化确认消息。 */
const handleMessageAck = (ack) => {
  const session = captureSession();
  const pending = pendingMessages.get(ack?.clientMsgId);
  if (!pending || !isSessionActive(session)) return;

  const conversationId = ack.conversationId || pending.conversationId;
  const failed = ack.status === "failed";
  const updated = normalizeMessage({
    ...pending,
    messageId: ack.messageId || pending.messageId || "",
    conversationId,
    seq: pending.seq || 0,
    attachmentId: ack.attachmentId || pending.attachmentId || "",
    sendTime: Number(ack.sendTime) || pending.sendTime,
    status: Number(pending.status) || 1,
    accepted: !failed,
    error: failed ? ack.extra || "消息发送失败" : "",
  });

  state.messages[conversationId] = mergeMessageLists(state.messages[conversationId] || [], [
    updated,
  ]);
  const storedMessage = state.messages[conversationId].find(
    (item) => item.clientMsgId === updated.clientMsgId,
  );
  if (failed) {
    // 失败消息必须继续保留在待处理表中，点击重试时复用原 clientMsgId。
    pendingMessages.set(ack.clientMsgId, storedMessage || updated);
  } else {
    pendingMessages.delete(ack.clientMsgId);
    pendingSendRequests.delete(ack.clientMsgId);
    const uploadTask = pendingUploadTasks.get(ack.clientMsgId);
    if (uploadTask) uploadTask.fileId = "";
  }

  if (storedMessage) {
    attachmentResolver
      .resolve([storedMessage])
      .finally(() => {
        if (!failed) releaseUploadPreview(ack.clientMsgId, storedMessage);
      })
      .catch(() => {});
  } else if (!failed) {
    pendingUploadTasks.delete(ack.clientMsgId);
  }
};

/** 以最大值推进私聊对端最高已读水位；该水位决定最后一条本人消息的展示状态。 */
const handleMessageReadNotice = (receipt) => {
  const conversationId = receipt?.conversationId || "";
  const lastReadSeq = Number(receipt?.lastReadSeq) || 0;
  const conversation = findConversation(state.conversations, conversationId);
  if (!conversation || conversation.convType !== ConversationType.PRIVATE_CHAT || lastReadSeq <= 0)
    return;

  const previous = Number(conversation.readWatermark) || 0;
  if (lastReadSeq <= previous) return;
  conversation.readWatermark = lastReadSeq;
  persistConversation(conversation);
};

/** 从服务端刷新会话快照，并保留内存中的本地连续序号。 */
const refreshConversationSnapshot = async (session = captureSession()) => {
  const items = await getConversations();
  if (!isSessionActive(session)) return [];

  const localContinuousSeqByConversation = new Map(
    state.conversations.map((conversation) => [
      conversation.conversationId,
      Number(conversation.lastContinuousSeq) || 0,
    ]),
  );
  const readWatermarkByConversation = new Map(
    state.conversations.map((conversation) => [
      conversation.conversationId,
      Number(conversation.readWatermark) || 0,
    ]),
  );
  const conversations = sortConversations(
    (items || []).map((conversation) => ({
      ...conversation,
      lastContinuousSeq: localContinuousSeqByConversation.get(conversation.conversationId) || 0,
      readWatermark: readWatermarkByConversation.get(conversation.conversationId) || 0,
    })),
  );
  state.conversations = conversations;
  if (!findConversation(conversations, state.activeConversationId)) {
    state.activeConversationId = "";
  }
  await replaceConversations(session.userId, conversations).catch(() => {});
  return conversations;
};

/** 从本地扫描水位同步到服务端本次请求快照，并串行合并通知触发的补拉。 */
const syncConversationFromLocal = async (
  conversationId,
  { session = captureSession(), targetSeq = 0 } = {},
) => {
  if (!conversationId || !isSessionActive(session)) return;
  const conversation = findConversation(state.conversations, conversationId);
  if (!conversation) return;
  return enqueueConversationMessage(conversationId, async () => {
    const localBoundary = Number(conversation.lastContinuousSeq) || 0;
    if (targetSeq > 0 && targetSeq <= localBoundary) return;
    const result = await syncMessages(conversationId, localBoundary);
    if (!isSessionActive(session)) return;

    const messages = (result?.messages || []).map(normalizeMessage);
    if (messages.length && !isOrderedAfter(messages, localBoundary)) {
      throw new Error("消息同步结果存在重复或乱序");
    }

    const nextBoundary = Number(messages.at(-1)?.seq) || localBoundary;
    if (nextBoundary <= localBoundary) return;

    const persisted = await persistConfirmedMessages(messages, {
      session,
      lastContinuousSeqByConversation: { [conversationId]: nextBoundary },
    });
    conversation.lastContinuousSeq =
      Number(persisted?.lastContinuousSeqByConversation?.[conversationId]) || nextBoundary;

    const active = state.activeConversationId === conversationId;
    if (active) await mergeMessages(conversationId, messages, session);
    applyMessagesToConversation(
      conversation,
      messages,
      state.activeConversationId,
      currentUserId(),
    );
    updateConversationList();
    await persistConversation(conversation, session);
    sendMessageRead(conversationId);
  });
};

/** 大型房间轻量通知没有消息正文，按其 seq 从本地边界补齐。 */
const syncRoomMessageNotice = async (notice) => {
  const conversationId = notice?.conversationId || "";
  const targetSeq = Number(notice?.seq) || 0;
  if (!conversationId || targetSeq <= 0) return;
  return syncConversationFromLocal(conversationId, { targetSeq });
};

/** WebSocket 建立或重连后，逐会话补齐离线快照与实时连接之间的缺口。 */
const syncAllConversationGaps = async (session = captureSession()) => {
  const conversations = [...state.conversations];
  await Promise.all(
    conversations.map((conversation) =>
      syncConversationFromLocal(conversation.conversationId, { session }),
    ),
  );
};

const wsClient = createWsClient({
  onStateChange: (connection) => {
    state.connection = connection;
  },
  onOpen: () => {
    if (pendingRoomEntryConversationID === state.activeConversationId) {
      sendRoomEnter(pendingRoomEntryConversationID);
    }
    const session = captureSession();
    syncAllConversationGaps(session).catch((error) => {
      console.error("WebSocket 建立后补齐消息失败", error);
    });
  },
  onBeforeReconnect: async () => {
    const session = captureSession();
    if (!isSessionActive(session)) return;
    await refreshConversationSnapshot(session);
    if (isSessionActive(session)) await catchUpAllOfflineConversations(session);
  },
  onReconnectPreparationError: (error) => {
    console.error("WebSocket 重连前离线补齐失败", error);
  },
  onMessagePush: upsertIncomingMessage,
  onMessageSendResult: handleMessageAck,
  onMessageReadNotice: handleMessageReadNotice,
  onRoomMessageNotice: (notice) => {
    syncRoomMessageNotice(notice).catch(() => {});
  },
});

const sendRoomEnter = (conversationId) => {
  const conversation = findConversation(state.conversations, conversationId);
  if (
    !conversation ||
    conversation.convType !== ConversationType.ROOM ||
    state.activeConversationId !== conversationId ||
    !wsClient.isConnected()
  )
    return false;

  const sent = wsClient.sendRoomEnter({ roomId: conversation.targetId });
  if (sent && pendingRoomEntryConversationID === conversationId) {
    pendingRoomEntryConversationID = "";
  }
  return sent;
};

/** 发送当前会话的已读回执；实时消息和点击会话共用此方法。 */
const sendMessageRead = (conversationId = state.activeConversationId) => {
  const conversation = findConversation(state.conversations, conversationId);
  const lastContinuousSeq = Number(conversation?.lastContinuousSeq) || 0;
  if (
    !conversation ||
    state.activeConversationId !== conversationId ||
    lastContinuousSeq <= 0 ||
    !wsClient.isConnected()
  )
    return;

  const boundaryMessage = (state.messages[conversationId] || []).find(
    (message) => Number(message.seq) === lastContinuousSeq,
  );
  if (!boundaryMessage?.messageId || boundaryMessage.senderId === currentUserId()) return;

  const sent = wsClient.sendMessageRead({
    messageId: boundaryMessage.messageId,
  });
  if (sent) conversation.unread = 0;
};

const connectSocket = ({ initialCatchupComplete = false } = {}) => {
  if (!state.authenticated || wsClient.isConnected() || wsClient.isConnecting()) return;
  if (initialCatchupComplete) {
    wsClient.connect(true);
    return;
  }
  wsClient.reconnect();
};

const clearAuthState = () => {
  sessionGeneration += 1;
  startupCatchupGeneration = -1;
  pendingRoomEntryConversationID = "";
  pendingMessages.clear();
  pendingSendRequests.clear();
  for (const task of pendingUploadTasks.values()) {
    task.cancelRequested = true;
    if (task.running) {
      task.uploader.cancel();
    } else if (!task.settled) {
      task.settled = true;
      task.reject?.(new Error("登录状态已失效"));
    }
  }
  uploadQueue.length = 0;
  pendingUploadTasks.clear();
  wsClient.disconnect();
  state.authenticated = false;
  state.currentUser = null;
  state.connection = "disconnected";
  state.loading = false;
  state.historyLoading = false;
  state.conversations = [];
  state.messages = {};
  state.contacts = [];
  state.friendRequests = [];
  state.activeConversationId = "";
  state.historyHasMore = {};
  state.historyCursor = {};
  attachmentResolver.clear();
  historyRequests.clear();

  for (const storage of [localStorage, sessionStorage]) {
    storage.removeItem("im_user");
  }

  cacheClearPromise = Promise.all([clearMessages(), clearConversations()]).catch(() => {});
  return cacheClearPromise;
};

if (typeof window !== "undefined") {
  window.addEventListener("auth:expired", () => {
    clearAuthState();
  });
}

export const hasChatAuth = () => state.authenticated;

export const ensureChatAuth = async () => {
  if (state.authenticated) return true;
  if (!authBootstrapPromise) {
    const generation = sessionGeneration;
    authBootstrapPromise = refreshSession()
      .then((auth) => {
        if (generation !== sessionGeneration) return false;
        state.authenticated = true;
        state.currentUser = authUserProfile(auth);
        persistSession(auth, Boolean(localStorage.getItem("im_user")));
        return true;
      })
      .catch(async () => {
        await clearAuthState();
        return false;
      })
      .finally(() => {
        authBootstrapPromise = null;
      });
  }
  return authBootstrapPromise;
};

export const useChatStore = defineStore("chat", () => {
  const activeConversation = computed(() =>
    findConversation(state.conversations, state.activeConversationId),
  );
  const activeMessages = computed(() => state.messages[state.activeConversationId] || []);

  const authenticate = async ({ mode, account, password, reconfirmPassword, remember }) => {
    if (mode === "register") {
      return registerUser({ phone: account, password, reconfirmPassword });
    }

    const auth = await loginUser({ account, password });
    await cacheClearPromise;
    state.authenticated = true;
    state.currentUser = authUserProfile(auth);
    persistSession(auth, remember);
    return auth;
  };

  const updateProfile = async (payload) => {
    const profile = await updateUserProfile(payload);
    state.currentUser = authUserProfile({ ...state.currentUser, ...profile });
    persistSession(state.currentUser, Boolean(localStorage.getItem("im_user")));
    return state.currentUser;
  };

  const loadWorkspace = async () => {
    if (!state.authenticated) return;
    const session = captureSession();
    const pendingClear = cacheClearPromise;
    state.loading = true;

    try {
      await pendingClear;
      if (!isSessionActive(session)) return;

      const [cachedConversations, remoteResult, friends, requests] = await Promise.all([
        queryConversationsByUserId(session.userId).catch(() => []),
        getConversations()
          .then((value) => ({ ok: true, value: value || [] }))
          .catch((error) => ({ ok: false, error })),
        getFriends(),
        getFriendRequests(),
      ]);
      if (!isSessionActive(session)) return;
      if (!remoteResult.ok && !cachedConversations.length) throw remoteResult.error;
      let didInitialCatchup = false;

      const cachedContinuousSeqByConversation = new Map(
        cachedConversations.map((conversation) => [
          conversation.conversationId,
          Number(conversation.lastContinuousSeq) || 0,
        ]),
      );
      const cachedReadWatermarkByConversation = new Map(
        cachedConversations.map((conversation) => [
          conversation.conversationId,
          Number(conversation.readWatermark) || 0,
        ]),
      );
      const conversations = sortConversations(
        remoteResult.ok
          ? (remoteResult.value || []).map((conversation) => ({
              ...conversation,
              lastContinuousSeq:
                cachedContinuousSeqByConversation.get(conversation.conversationId) || 0,
              readWatermark:
                cachedReadWatermarkByConversation.get(conversation.conversationId) || 0,
            }))
          : cachedConversations,
      );
      state.conversations = conversations;
      if (remoteResult.ok) {
        await replaceConversations(session.userId, conversations).catch(() => {});
        if (startupCatchupGeneration !== session.generation) {
          await catchUpAllOfflineConversations(session);
          if (!isSessionActive(session)) return;
          startupCatchupGeneration = session.generation;
          didInitialCatchup = true;
        }
      }

      const conversationByTargetId = new Map(
        conversations.map((conversation) => [conversation.targetId, conversation]),
      );
      state.contacts = (friends || []).map((item) => ({
        id: item.friendUserId,
        name: item.displayName || item.friendUsername || "好友",
        username: item.friendUsername || "",
        avatar: item.friendAvatar || "",
        online: false,
        avatarColor: THEME_CONTACT_AVATAR,
        conversationId: conversationByTargetId.get(item.friendUserId)?.conversationId || "",
      }));
      state.friendRequests = (requests || [])
        .filter((item) => item.status === 1)
        .map((item) => ({
          id: item.requestId,
          applicantUserId: item.applicantUserId || "",
          name: item.applicantNickName || "新联系人",
          note: item.message,
          time: formatTime(item.applyTime),
          avatarColor: "#8069a8",
        }));
      if (!findConversation(state.conversations, state.activeConversationId)) {
        state.activeConversationId = "";
      }

      if (!remoteResult.ok) throw remoteResult.error;
      connectSocket({ initialCatchupComplete: didInitialCatchup });
    } finally {
      if (isSessionActive(session)) state.loading = false;
    }
  };

  const loadHistory = async (conversationId, cursor = 0) => {
    const session = captureSession();
    if (!conversationId || !isSessionActive(session)) return;

    const requestId = ++historyRequestSequence;
    historyRequests.set(conversationId, requestId);
    state.historyLoading = true;
    try {
      const data = await queryMessagesByCursor({
        conversationId,
        cursor,
        limit: HISTORY_PAGE_SIZE,
      });
      if (!isSessionActive(session) || state.activeConversationId !== conversationId) return;

      // 历史消息完全读取本地 IndexedDB，不在滚动分页时回源。
      await mergeMessages(conversationId, data.messages, session);
      state.historyCursor[conversationId] = data.nextCursor;
      state.historyHasMore[conversationId] = data.hasMore;
    } finally {
      if (historyRequests.get(conversationId) === requestId) {
        historyRequests.delete(conversationId);
      }
      if (!historyRequests.has(state.activeConversationId)) state.historyLoading = false;
    }
  };

  const selectConversation = async (conversationId) => {
    state.activeConversationId = conversationId;
    const conversation = findConversation(state.conversations, conversationId);
    if (conversation) conversation.unread = 0;
    if (conversation?.convType === ConversationType.ROOM) {
      pendingRoomEntryConversationID = conversationId;
      sendRoomEnter(conversationId);
    } else {
      pendingRoomEntryConversationID = "";
    }
    if (state.authenticated) await loadHistory(conversationId, 0);
  };

  const loadOlderMessages = async () => {
    const conversationId = state.activeConversationId;
    const cursor = state.historyCursor[conversationId];
    if (!conversationId || cursor === -1 || state.historyLoading) return;
    return loadHistory(conversationId, cursor);
  };

  const appendOutgoingMessage = (conversation, payload, overrides = {}) => {
    const message = {
      ...createOutgoingMessage(conversation, payload, state.currentUser),
      ...overrides,
    };
    if (!state.messages[conversation.conversationId]) {
      state.messages[conversation.conversationId] = [];
    }
    state.messages[conversation.conversationId] = mergeMessageLists(
      state.messages[conversation.conversationId],
      [message],
    );
    const storedMessage =
      findPendingMessage(payload.clientMsgId, conversation.conversationId) || message;
    pendingMessages.set(payload.clientMsgId, storedMessage);
    conversation.lastMessage = createConversationPreview(
      conversation,
      storedMessage,
      state.currentUser,
    );
    updateConversationList();
    persistConversation(conversation);
    return storedMessage;
  };

  const sendMessage = (content) => {
    const text = typeof content === "string" ? content.trim() : "";
    const conversation = activeConversation.value;
    if (!text || !conversation) return null;
    if (!wsClient.isConnected()) throw new Error("实时连接尚未建立，请稍后重试。");

    const clientMsgId = createClientMessageId();
    const displayData = {
      clientMsgId,
      convType: conversation.convType,
      cType: MessageType.TEXT,
      content: text,
    };
    appendOutgoingMessage(conversation, displayData, {
      error: "",
    });

    const request = createMessageRequest(conversation, {
      clientMsgId,
      cType: MessageType.TEXT,
      content: text,
    });
    return sendPendingMessage(clientMsgId, conversation, request);
  };

  const sendAttachment = async (file, cType) => {
    if (!file) return null;
    if (![MessageType.IMAGE, MessageType.VIDEO, MessageType.FILE].includes(Number(cType))) {
      throw new Error("不支持的媒体类型。");
    }
    if (Number(cType) === MessageType.IMAGE && file.type && !file.type.startsWith("image/")) {
      throw new Error("请选择图片文件。");
    }
    if (Number(cType) === MessageType.VIDEO && file.type && !file.type.startsWith("video/")) {
      throw new Error("请选择视频文件。");
    }
    const conversation = activeConversation.value;
    if (!conversation) return null;
    if (!state.authenticated) throw new Error("登录状态已失效，请重新登录。");
    if (!wsClient.isConnected()) throw new Error("实时连接尚未建立，请稍后重试。");

    const clientMsgId = createClientMessageId();
    const previewURL = createLocalPreviewURL(file, cType);
    const displayData = {
      clientMsgId,
      convType: conversation.convType,
      cType,
      fileName: file.name || "",
      fileSize: file.size || 0,
      mediaUrl: previewURL,
    };
    appendOutgoingMessage(conversation, displayData, {
      uploadStage: "queued",
      uploadProgress: 0,
      error: "",
    });

    const uploader = useFileUpload();
    const task = {
      clientMsgId,
      conversationId: conversation.conversationId,
      conversation,
      session: captureSession(),
      file,
      cType,
      fileId: "",
      previewURL,
      uploader,
      running: false,
      settled: false,
      cancelRequested: false,
    };
    pendingUploadTasks.set(clientMsgId, task);

    // 文件上传后只提交 fileId；文件元信息由后端根据文件记录组装。
    return enqueueUploadTask(task);
  };

  /** 点击失败消息的感叹号后，复用原 clientMsgId 重新发送。 */
  const retryMessage = async (message) => {
    const clientMsgId = message?.clientMsgId || "";
    const session = captureSession();
    const conversation = findConversation(state.conversations, message?.conversationId);
    const pending =
      pendingMessages.get(clientMsgId) || findPendingMessage(clientMsgId, message?.conversationId);

    if (!clientMsgId || !pending || !conversation || !isSessionActive(session)) {
      throw new Error("消息已失效，请重新选择会话后重试。");
    }
    if (!pending.error) return pending;
    if (!wsClient.isConnected()) {
      throw new Error("实时连接尚未建立，请稍后重试。");
    }

    const request = pendingSendRequests.get(clientMsgId);
    if (request) {
      return sendPendingMessage(clientMsgId, conversation, request, session);
    }

    const task = pendingUploadTasks.get(clientMsgId);
    const isText = Number(pending.cType) === MessageType.TEXT;
    if (isText) {
      throw new Error("待发送消息请求已失效，请重新发送。");
    }
    if (task?.fileId) {
      const mediaRequest = createMessageRequest(conversation, {
        clientMsgId,
        cType: task.cType,
        fileId: task.fileId,
      });
      return sendPendingMessage(clientMsgId, conversation, mediaRequest, session);
    }

    if (!task?.file) {
      throw new Error("原始文件已不可用，请重新选择文件。");
    }

    task.conversation = conversation;
    task.session = session;
    task.cancelRequested = false;
    task.settled = false;
    updatePendingMessage(clientMsgId, conversation.conversationId, {
      uploadStage: "queued",
      uploadProgress: 0,
      error: "",
    });
    return enqueueUploadTask(task);
  };

  const pauseUpload = (clientMsgId) => {
    const task = pendingUploadTasks.get(clientMsgId);
    if (task?.running) task.uploader.pause();
  };

  const resumeUpload = (clientMsgId) => {
    const task = pendingUploadTasks.get(clientMsgId);
    if (task?.running) task.uploader.resume();
  };

  const cancelUpload = (clientMsgId) => {
    const task = pendingUploadTasks.get(clientMsgId);
    if (!task) return;

    task.cancelRequested = true;
    if (task.running) {
      task.uploader.cancel();
      markPendingMessageFailed(clientMsgId, task.conversationId, new Error("上传已取消"));
      return;
    }

    if (!task.settled) {
      task.settled = true;
      markPendingMessageFailed(clientMsgId, task.conversationId, new Error("上传已取消"));
      task.reject?.(new Error("上传已取消"));
    }
  };

  const clearConversation = async (conversationId) => {
    if (!conversationId) return;
    state.messages[conversationId] = [];
    state.historyCursor[conversationId] = 0;
    state.historyHasMore[conversationId] = false;
    for (const [clientMsgId, message] of pendingMessages) {
      if (message.conversationId === conversationId) {
        pendingMessages.delete(clientMsgId);
        pendingSendRequests.delete(clientMsgId);
      }
    }
    await deleteMessagesByConversation(currentUserId(), conversationId);
    const conversation = findConversation(state.conversations, conversationId);
    if (conversation) conversation.lastContinuousSeq = 0;
  };

  const handleFriendRequest = async (request, accepted) => {
    await operateFriendRequest({
      requestId: request.id,
      action: accepted ? 1 : 2,
    });
    state.friendRequests = state.friendRequests.filter((item) => item.id !== request.id);
    if (accepted) await loadWorkspace();
  };

  const startConversation = async (contact) => {
    let conversation = state.conversations.find(
      (item) =>
        item.convType === ConversationType.PRIVATE_CHAT &&
        (item.targetId === contact.id ||
          (contact.conversationId && item.conversationId === contact.conversationId)),
    );
    if (!conversation) {
      const session = captureSession();
      await refreshConversationSnapshot(session);
      if (!isSessionActive(session)) return null;
      conversation = state.conversations.find(
        (item) => item.convType === ConversationType.PRIVATE_CHAT && item.targetId === contact.id,
      );
      if (!conversation) throw new Error("会话不存在，请刷新好友列表后重试。");
    }

    await selectConversation(conversation.conversationId);
    return conversation;
  };

  const createRoom = async (form) => {
    const room = await createRoomRequest(form);
    await loadWorkspace();
    return room;
  };

  const joinRoom = async (inviteCode) => {
    const session = captureSession();
    const room = await joinRoomRequest(inviteCode);
    if (!isSessionActive(session)) return room;

    const conversation = {
      ...findConversation(state.conversations, room.conversationId),
      conversationId: room.conversationId,
      convType: ConversationType.ROOM,
      targetId: room.roomId,
      displayName: room.roomName,
      avatar: room.avatar,
      unread: 0,
      latestSeq: Number(room.latestSeq) || 0,
      lastContinuousSeq: Number(room.latestSeq) || 0,
      room: {
        roomId: room.roomId,
        roomName: room.roomName,
        avatar: room.avatar,
        description: room.description,
        memberCount: room.memberCount,
      },
    };
    // 入房水位是同步起点，不代表入房前的消息已下载到本地。
    await insertConversations(session.userId, [conversation]);
    if (!isSessionActive(session)) return room;

    state.conversations = sortConversations([
      ...state.conversations.filter((item) => item.conversationId !== room.conversationId),
      conversation,
    ]);
    await syncConversationFromLocal(room.conversationId, { session });
    return room;
  };

  const retryConnection = () =>
    startupCatchupGeneration === sessionGeneration ? wsClient.reconnect() : loadWorkspace();

  const logout = async () => {
    try {
      if (state.authenticated) await logoutUser();
    } finally {
      await clearAuthState();
    }
  };

  return {
    state,
    activeConversation,
    activeMessages,
    authenticate,
    updateProfile,
    loadWorkspace,
    selectConversation,
    loadOlderMessages,
    sendMessage,
    sendAttachment,
    retryMessage,
    sendMessageRead,
    clearConversation,
    handleFriendRequest,
    addFriendRequest: createFriendRequest,
    startConversation,
    createRoom,
    joinRoom,
    retryConnection,
    pauseUpload,
    resumeUpload,
    cancelUpload,
    logout,
  };
});
