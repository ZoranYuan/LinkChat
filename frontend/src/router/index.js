import { createRouter, createWebHistory } from "vue-router";
import { ensureChatAuth, hasChatAuth } from "../modules/chat/chatStore.js";
import ChatView from "../views/ChatView.vue";
import LoginView from "../views/LoginView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/login", name: "login", component: LoginView },
    { path: "/chat", name: "chat", component: ChatView, meta: { requiresAuth: true } },
    { path: "/", redirect: "/chat" },
    { path: "/:pathMatch(.*)*", redirect: "/chat" },
  ],
});

router.beforeEach(async (to) => {
  if (to.path === "/login") return true;

  const authenticated = hasChatAuth() || (await ensureChatAuth());
  if (to.meta.requiresAuth && !authenticated)
    return { name: "login", query: { redirect: to.fullPath } };
  return true;
});

export default router;
