/** 用户偏好表：一个用户一条记录，按 userId 主键隔离。 */
export const PREFERENCES_STORE_NAME = "preferences";
export const PREFERENCES_KEY_PATH = "userId";

/** 将偏好构造成 IndexedDB 记录，只保留可持久化字段。 */
export const createPreferencesRecord = (userId, preferences = {}) => {
  if (!userId) return null;
  return {
    userId,
    theme: preferences.theme === "dark" ? "dark" : "light",
    fontScale: Number(preferences.fontScale) || 1,
    updatedAt: Date.now(),
  };
};
