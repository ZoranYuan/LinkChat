import { MESSAGE_INDEXES, MESSAGE_KEY_PATH, MESSAGE_STORE_NAME } from "./models/message.js";
import {
  CONVERSATION_INDEXES,
  CONVERSATION_KEY_PATH,
  CONVERSATION_STORE_NAME,
} from "./models/conversation.js";
import { PREFERENCES_KEY_PATH, PREFERENCES_STORE_NAME } from "./models/preferences.js";

const DB_NAME = "im-chat-cache";
const DB_VERSION = 3;

/** 创建消息表及会话、会话序号索引。 */
const createMessageStore = (db) => {
  const store = db.createObjectStore(MESSAGE_STORE_NAME, {
    keyPath: MESSAGE_KEY_PATH,
  });
  store.createIndex(MESSAGE_INDEXES.CONVERSATION, "conversationId", { unique: false });
  store.createIndex(MESSAGE_INDEXES.CONVERSATION_SEQ, ["conversationId", "seq"], { unique: false });
};

/** 创建本地会话表及用户索引。 */
const createConversationStore = (db) => {
  const store = db.createObjectStore(CONVERSATION_STORE_NAME, {
    keyPath: CONVERSATION_KEY_PATH,
  });
  store.createIndex(CONVERSATION_INDEXES.USER_ID, "userId", { unique: false });
};

/** 打开本地 IndexedDB，并在首次创建或升级时初始化数据表。 */
export const openIndexedDb = () =>
  new Promise((resolve, reject) => {
    if (typeof window === "undefined" || !("indexedDB" in window)) {
      reject(new Error("IndexedDB unavailable"));
      return;
    }

    const request = window.indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(MESSAGE_STORE_NAME)) {
        createMessageStore(db);
      }
      if (!db.objectStoreNames.contains(CONVERSATION_STORE_NAME)) {
        createConversationStore(db);
      }
      // 用户偏好：一个用户一条记录，按 userId 主键隔离
      if (!db.objectStoreNames.contains(PREFERENCES_STORE_NAME)) {
        db.createObjectStore(PREFERENCES_STORE_NAME, { keyPath: PREFERENCES_KEY_PATH });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
