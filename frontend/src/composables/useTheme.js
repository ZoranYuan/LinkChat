import { computed, watch } from "vue";
import { preferences, updatePreferences } from "./useUserPreferences.js";

const DARK = "dark";
const LIGHT = "light";

function apply(value) {
  const root = typeof document !== "undefined" ? document.documentElement : null;
  if (!root) return;
  if (value === DARK) {
    root.setAttribute("data-theme", DARK);
  } else {
    root.removeAttribute("data-theme");
  }
}

// 配置变化（含登录后 hydrate 覆盖）后自动同步到 DOM，避免手动 apply 遗漏
watch(() => preferences.theme, (value) => apply(value));

export function useTheme() {
  // 对外仍是 ref 语义（computed 也是 ref），AppRail 里的 theme === 'dark' 照常可用
  const theme = computed(() => preferences.theme);

  const init = () => {
    apply(theme.value);
  };

  const setTheme = (value) => {
    if (value !== DARK && value !== LIGHT) return;
    updatePreferences({ theme: value });
    apply(theme.value);
  };

  const toggle = () => setTheme(theme.value === DARK ? LIGHT : DARK);

  const isDark = () => theme.value === DARK;

  return { theme, init, setTheme, toggle, isDark };
}
