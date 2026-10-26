import { createRouter, createWebHistory } from "vue-router";
import { ensureChatAuth, hasChatAuth } from "../modules/chat/chatStore.js";
import ChatView from "../views/ChatView.vue";
import LoginView from "../views/LoginView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/login", name: "login", component: LoginView, meta: { guestOnly: true } },
    { path: "/chat", name: "chat", component: ChatView, meta: { requiresAuth: true } },
    { path: "/", redirect: "/chat" },
    { path: "/:pathMatch(.*)*", redirect: "/chat" },
  ],
});

router.beforeEach(async (to) => {
  const authenticated = hasChatAuth() || (await ensureChatAuth());
  if (to.meta.requiresAuth && !authenticated)
    return { name: "login", query: { redirect: to.fullPath } };
  if (to.meta.guestOnly && authenticated) return "/chat";
  return true;
});

export default router;
