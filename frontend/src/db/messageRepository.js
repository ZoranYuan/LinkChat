import { MESSAGE_INDEXES, MESSAGE_STORE_NAME, createMessageRecord } from "./models/message.js";

import { CONVERSATION_STORE_NAME } from "./models/conversation.js";

import { openIndexedDb } from "./connection.js";

const MAX_LIMIT = 100;

/**
 * 原子写入确认消息和已确认的本地连续边界。
 * 连续性由 WebSocket /sync 调用方判断，这里不扫描消息表推测边界。
 */
export const insertMessages = async (
  messages,
  { userId, lastContinuousSeqByConversation = {} } = {},
) => {
  const boundaryEntries = Object.entries(lastContinuousSeqByConversation || {});
  if (!messages?.length && !boundaryEntries.length) {
    return {
      lastContinuousSeqByConversation: {},
    };
  }

  if (!userId) {
    throw new Error("insertMessages requires userId");
  }

  const records = (messages || []).map((message) => createMessageRecord(message)).filter(Boolean);

  if (!records.length && !boundaryEntries.length) {
    return {
      lastContinuousSeqByConversation: {},
    };
  }

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([MESSAGE_STORE_NAME, CONVERSATION_STORE_NAME], "readwrite");
    const messageStore = transaction.objectStore(MESSAGE_STORE_NAME);
    const conversationStore = transaction.objectStore(CONVERSATION_STORE_NAME);
    const updatedBoundaries = {};

    for (const record of records) {
      // conversationId + seq 是主键，重复推送覆盖同一条消息本体。
      messageStore.put(record);
    }

    for (const [conversationId, rawSeq] of boundaryEntries) {
      const nextSeq = Number(rawSeq);
      if (!conversationId || !Number.isSafeInteger(nextSeq) || nextSeq < 0) continue;

      const request = conversationStore.get([userId, conversationId]);
      request.onsuccess = () => {
        const conversation = request.result;
        if (!conversation) return;

        const currentSeq = Number(conversation.lastContinuousSeq) || 0;
        if (nextSeq > currentSeq) {
          conversation.lastContinuousSeq = nextSeq;
          conversationStore.put(conversation);
        }
        updatedBoundaries[conversationId] = Math.max(currentSeq, nextSeq);
      };
    }

    transaction.oncomplete = () => {
      db.close();
      resolve({ lastContinuousSeqByConversation: updatedBoundaries });
    };
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
    transaction.onabort = () => {
      db.close();
      reject(transaction.error || new Error("消息和连续序号写入事务被取消"));
    };
  });
};

/** 按会话和 cursor 倒序读取本地历史消息，并返回本地分页游标。 */
export const queryMessagesByCursor = async ({ conversationId, cursor = 0, limit = 30 }) => {
  if (!conversationId) {
    return {
      messages: [],
      hasMore: false,
      nextCursor: -1,
    };
  }

  if (Number(cursor) === -1) {
    return {
      messages: [],
      hasMore: false,
      nextCursor: -1,
    };
  }

  const currentCursor = Math.max(Number(cursor) || 0, 0);

  const pageSize = Math.min(Math.max(Number(limit) || 30, 1), MAX_LIMIT);

  const db = await openIndexedDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(MESSAGE_STORE_NAME, "readonly");

    const store = transaction.objectStore(MESSAGE_STORE_NAME);

    const index = store.index(MESSAGE_INDEXES.CONVERSATION_SEQ);

    const lowerKey = [conversationId, 0];

    const upperKey =
      currentCursor > 0
        ? [conversationId, currentCursor]
        : [conversationId, Number.MAX_SAFE_INTEGER];

    const range = IDBKeyRange.bound(lowerKey, upperKey, false, currentCursor > 0);

    const records = [];
    const request = index.openCursor(range, "prev");

    request.onsuccess = () => {
      const result = request.result;

      if (!result) {
        return;
      }

      records.push(result.value);

      // 多取一条，判断本地是否还有更早消息
      if (records.length <= pageSize) {
        result.continue();
      }
    };

    request.onerror = () => {
      reject(request.error);
    };

    transaction.oncomplete = () => {
      db.close();

      const hasMore = records.length > pageSize;

      const pageRecords = records.slice(0, pageSize).reverse();

      resolve({
        messages: pageRecords,
        hasMore,
        nextCursor: hasMore ? Number(pageRecords[0]?.seq) || -1 : -1,
      });
    };

    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
  });
};

/** 删除指定会话的本地消息，并在同一事务中重置同步边界。 */
export const deleteMessagesByConversation = async (userId, conversationId) => {
  if (!userId || !conversationId) return;

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([MESSAGE_STORE_NAME, CONVERSATION_STORE_NAME], "readwrite");
    const messageStore = transaction.objectStore(MESSAGE_STORE_NAME);
    const conversationStore = transaction.objectStore(CONVERSATION_STORE_NAME);
    const index = messageStore.index(MESSAGE_INDEXES.CONVERSATION);
    const request = index.openCursor(IDBKeyRange.only(conversationId));

    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return;
      cursor.delete();
      cursor.continue();
    };
    request.onerror = () => reject(request.error);
    const conversationRequest = conversationStore.get([userId, conversationId]);
    conversationRequest.onsuccess = () => {
      const conversation = conversationRequest.result;
      if (!conversation) return;
      conversation.lastContinuousSeq = 0;
      conversationStore.put(conversation);
    };
    conversationRequest.onerror = () => reject(conversationRequest.error);
    transaction.oncomplete = () => {
      db.close();
      resolve();
    };
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
    transaction.onabort = () => {
      db.close();
      reject(transaction.error || new Error("删除消息事务被取消"));
    };
  });
};

/** 清空本地消息表，通常在退出登录时调用。 */
export const clearMessages = async () => {
  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(MESSAGE_STORE_NAME, "readwrite");
    transaction.objectStore(MESSAGE_STORE_NAME).clear();
    transaction.oncomplete = () => {
      db.close();
      resolve();
    };
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
    transaction.onabort = () => {
      db.close();
      reject(transaction.error || new Error("清理消息事务被取消"));
    };
  });
};
