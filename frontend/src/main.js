import { createPinia } from "pinia";
import { createApp } from "vue";
import App from "./App.vue";
import router from "./router";
import { useTheme } from "./composables/useTheme.js";
import { useFontScale } from "./composables/useFontScale.js";
// Element Plus（仅滑块所需样式，按需引入，避免全量体积）
import "element-plus/theme-chalk/base.css";
import "element-plus/theme-chalk/el-slider.css";
import "element-plus/theme-chalk/el-tooltip.css";
import "./styles/main.css";
// 让 Element Plus 组件跟随主题主色
import "./styles/element-theme.css";

// 启动时应用已保存的主题与字号，避免首屏闪烁
useTheme().init();
useFontScale().init();

createApp(App).use(createPinia()).use(router).mount("#app");
