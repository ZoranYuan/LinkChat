import { openIndexedDb } from "./connection.js";
import { PREFERENCES_STORE_NAME } from "./models/preferences.js";

/** 按用户 ID 读取本地偏好配置；无记录时返回 null。 */
export const loadPreferences = async (userId) => {
  if (!userId) return null;

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PREFERENCES_STORE_NAME, "readonly");
    const request = transaction.objectStore(PREFERENCES_STORE_NAME).get(userId);

    request.onsuccess = () => resolve(request.result || null);
    request.onerror = () => reject(request.error);
    transaction.oncomplete = () => db.close();
    transaction.onerror = () => {
      db.close();
      reject(transaction.error);
    };
  });
};

/** 写入（覆盖）指定用户的偏好配置。 */
export const savePreferences = async (userId, preferences) => {
  if (!userId) return;

  const db = await openIndexedDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PREFERENCES_STORE_NAME, "readwrite");
    transaction.objectStore(PREFERENCES_STORE_NAME).put(preferences);

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
      reject(transaction.error || new Error("偏好写入事务被取消"));
    };
  });
};
