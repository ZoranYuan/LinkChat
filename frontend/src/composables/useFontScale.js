import { computed, watch } from "vue";
import {
  preferences,
  updatePreferences,
  FONT_SCALE_MIN,
  FONT_SCALE_MAX,
} from "./useUserPreferences.js";

export { FONT_SCALE_MIN, FONT_SCALE_MAX };
export const FONT_SCALE_STEP = 0.05;

function apply(value) {
  const root = typeof document !== "undefined" ? document.documentElement : null;
  if (!root) return;
  // 直接写总缩放变量，全站 --im-fs-* 立即生效
  root.style.setProperty("--im-font-scale", String(value));
}

// 配置变化（含登录后 hydrate 覆盖）后自动同步 CSS 变量
watch(() => preferences.fontScale, (value) => apply(value));

export function useFontScale() {
  const fontScale = computed(() => preferences.fontScale);

  const init = () => {
    apply(fontScale.value);
  };

  const setScale = (value) => {
    if (!Number.isFinite(value)) return;
    updatePreferences({ fontScale: value });
    apply(fontScale.value);
  };

  return { fontScale, init, setScale };
}
