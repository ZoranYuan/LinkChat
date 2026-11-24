import { reactive } from "vue";
import { createPreferencesRecord } from "../db/models/preferences.js";
import { loadPreferences, savePreferences } from "../db/preferencesRepository.js";

/**
 * 用户偏好配置（权威源 = IndexedDB 的 preferences 表，按 userId 隔离）
 *
 * - 登录成功后调用 hydratePreferences(userId) 从 IndexedDB 加载该用户配置
 * - 配置变更时写回 IndexedDB（按 userId），并同步写一份 localStorage 快照
 * - localStorage 快照只用于首屏同步渲染（IndexedDB 是异步的，纯用它首屏会闪一下），
 *   加载完成后以 IndexedDB 为准覆盖
 * - 退出登录不清偏好（clearAuthState 只删 im_user 与消息/会话），下次登录按 userId 读回
 */
const SNAPSHOT_KEY = "im_user_preferences";
const LEGACY_THEME_KEY = "im_theme";
const LEGACY_FONT_KEY = "im_font_size";

export const FONT_SCALE_MIN = 0.8;
export const FONT_SCALE_MAX = 1.2;

function defaultTheme() {
  if (typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
    return "dark";
  }
  return "light";
}

const DEFAULTS = { theme: defaultTheme(), fontScale: 1 };

function sanitize(raw) {
  const out = { ...DEFAULTS };
  if (!raw || typeof raw !== "object") return out;
  if (raw.theme === "light" || raw.theme === "dark") out.theme = raw.theme;
  const n = Number(raw.fontScale);
  if (Number.isFinite(n)) out.fontScale = Math.min(FONT_SCALE_MAX, Math.max(FONT_SCALE_MIN, n));
  return out;
}

function read(key) {
  try {
    return typeof localStorage !== "undefined" ? localStorage.getItem(key) : null;
  } catch {
    return null;
  }
}

function writeSnapshot(value) {
  try {
    localStorage.setItem(SNAPSHOT_KEY, JSON.stringify(value));
  } catch {
    // 隐私模式 / 配额不足时忽略，IndexedDB 仍是权威源
  }
}

/** 读取老版本散落的 key（仅用于一次性迁移到 IndexedDB）。 */
function readLegacy() {
  const theme = read(LEGACY_THEME_KEY);
  const font = read(LEGACY_FONT_KEY);
  if (theme === null && font === null) return null;
  return sanitize({
    theme: theme || undefined,
    fontScale: font !== null ? Number(font) : undefined,
  });
}

/** 读首屏同步快照：新 key 优先，其次老 key（升级过渡）。 */
function readSnapshot() {
  const raw = read(SNAPSHOT_KEY);
  if (raw) {
    try {
      return sanitize(JSON.parse(raw));
    } catch {
      // 快照损坏则回退老 key
    }
  }
  // 无任何历史配置时回退默认值副本，确保 reactive(preferences) 永不为 null
  // 返回副本而非 DEFAULTS 本身，避免 reactive 修改污染默认值
  return readLegacy() || { ...DEFAULTS };
}

// 模块级单例：全局唯一的偏好状态，初值取自首屏快照（同步读取，避免闪烁）
export const preferences = reactive(readSnapshot());
// 当前已 hydrate 的用户，未登录时为空
let currentUserId = "";

/**
 * 登录成功后按用户加载配置（权威源 IndexedDB）。
 * - 该用户已有记录：加载并覆盖到 preferences
 * - 无记录：把首屏快照 / 老 key 的值作为该用户初始配置落库（一次性迁移）
 */
export const hydratePreferences = async (userId) => {
  if (!userId) return;
  currentUserId = userId;

  try {
    const stored = await loadPreferences(userId);
    if (stored) {
      Object.assign(preferences, sanitize(stored));
      writeSnapshot(preferences);
      return;
    }
    // 首次登录：用快照/老配置作为初始值落库
    Object.assign(preferences, readSnapshot() || DEFAULTS);
    await persistCurrent();
  } catch {
    // IndexedDB 不可用（隐私模式等）：保留快照值，本次会话仍可用
  }
};

async function persistCurrent() {
  if (!currentUserId) return;
  const record = createPreferencesRecord(currentUserId, preferences);
  if (!record) return;
  try {
    await savePreferences(currentUserId, record);
  } catch {
    // 写库失败不影响内存状态与快照
  }
}

export function updatePreferences(patch) {
  const next = sanitize({ ...preferences, ...patch });
  Object.assign(preferences, next);
  writeSnapshot(preferences);
  // 异步写回 IndexedDB（按当前用户），不阻塞 UI
  void persistCurrent();
}

export function useUserPreferences() {
  return { preferences, updatePreferences, hydratePreferences };
}
