import {
  CONVERSATION_INDEXES,
  CONVERSATION_STORE_NAME,
  createConversationRecord,
} from "./models/conversation.js";
import { openIndexedDb } from "./connection.js";

/** 在一个 IndexedDB 事务中批量新增或更新用户会话。 */
export const insertConversations = async (userId, conversations) => {
  if (!userId || !conversations?.length) return;

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CONVERSATION_STORE_NAME, "readwrite");
    const store = transaction.objectStore(CONVERSATION_STORE_NAME);

    for (const conversation of conversations) {
      const record = createConversationRecord(userId, conversation);
      if (!record) continue;

      store.put(record);
    }

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
      reject(transaction.error || new Error("会话批量写入事务被取消"));
    };
  });
};

/** 用服务端会话快照替换指定用户的本地会话列表。 */
export const replaceConversations = async (userId, conversations) => {
  if (!userId) return;

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CONVERSATION_STORE_NAME, "readwrite");
    const store = transaction.objectStore(CONVERSATION_STORE_NAME);
    const index = store.index(CONVERSATION_INDEXES.USER_ID);
    const request = index.getAllKeys(IDBKeyRange.only(userId));

    request.onsuccess = () => {
      for (const key of request.result || []) store.delete(key);
      for (const conversation of conversations || []) {
        const record = createConversationRecord(userId, conversation);
        if (record) store.put(record);
      }
    };
    request.onerror = () => reject(request.error);
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
      reject(transaction.error || new Error("会话快照替换事务被取消"));
    };
  });
};

/** 按用户 ID 查询本地会话，并按最后一条消息时间排序。 */
export const queryConversationsByUserId = async (userId) => {
  if (!userId) return [];

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CONVERSATION_STORE_NAME, "readonly");
    const index = transaction
      .objectStore(CONVERSATION_STORE_NAME)
      .index(CONVERSATION_INDEXES.USER_ID);
    const request = index.getAll(IDBKeyRange.only(userId));

    request.onsuccess = () => {
      const result = request.result || [];
      result.sort(
        (left, right) =>
          Number(right.lastMessage?.sendTime || 0) - Number(left.lastMessage?.sendTime || 0),
      );
      resolve(result);
    };
    request.onerror = () => reject(request.error);
    transaction.oncomplete = () => db.close();
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
  });
};

/** 清空本地会话表，通常在退出登录时调用。 */
export const clearConversations = async () => {
  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(CONVERSATION_STORE_NAME, "readwrite");
    transaction.objectStore(CONVERSATION_STORE_NAME).clear();
    transaction.oncomplete = () => {
      db.close();
      resolve();
    };
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
  });
};
