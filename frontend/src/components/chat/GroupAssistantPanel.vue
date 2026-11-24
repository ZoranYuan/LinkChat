<script setup>
import { X } from "@lucide/vue";
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { streamGroupAgent } from "../../services/http/api.js";

const props = defineProps({ conversation: { type: Object, default: null } });
defineEmits(["close"]);

const groupId = computed(() => props.conversation?.group?.groupId || props.conversation?.targetId || "");
const messages = ref([]);
const question = ref("");
const feed = ref(null);
const isStreaming = ref(false);
let activeController = null;

const toolLabels = {
  get_unread_messages: "读取未读消息",
  search_messages: "检索群聊消息",
  sync_group_messages: "同步群聊消息",
};

const scrollToBottom = () => {
  nextTick(() => {
    if (feed.value) feed.value.scrollTop = feed.value.scrollHeight;
  });
};

const stopRequest = () => {
  activeController?.abort();
  activeController = null;
};

watch(groupId, () => {
  stopRequest();
  messages.value = [];
  isStreaming.value = false;
});

onBeforeUnmount(stopRequest);

const runTask = async (body, prompt) => {
  if (!groupId.value || isStreaming.value) return;

  const assistantMessage = {
    role: "assistant",
    content: "",
    status: "streaming",
    activity: "正在连接群聊助手…",
    error: "",
    tools: [],
  };
  if (prompt) messages.value.push({ role: "user", content: prompt });
  messages.value.push(assistantMessage);
  scrollToBottom();

  const controller = new AbortController();
  activeController = controller;
  isStreaming.value = true;
  let terminalEventReceived = false;

  try {
    await streamGroupAgent(groupId.value, body, ({ event, data }) => {
      switch (event) {
        case "connected":
          assistantMessage.activity = "助手已连接";
          break;
        case "token":
          assistantMessage.content += String(data.content ?? "");
          assistantMessage.activity = "";
          scrollToBottom();
          break;
        case "tool_start": {
          const name = String(data.name ?? "");
          assistantMessage.tools.push({ name, status: "running" });
          assistantMessage.activity = `${toolLabels[name] || name || "工具"}中…`;
          scrollToBottom();
          break;
        }
        case "tool_end": {
          const name = String(data.name ?? "");
          const tool = [...assistantMessage.tools].reverse().find(
            (item) => item.name === name && item.status === "running",
          );
          if (tool) tool.status = "done";
          assistantMessage.activity = "正在整理回答…";
          break;
        }
        case "compress":
          assistantMessage.activity = "正在整理较早的对话…";
          break;
        case "completed":
          terminalEventReceived = true;
          assistantMessage.status = "completed";
          assistantMessage.activity = "";
          break;
        case "error":
          terminalEventReceived = true;
          assistantMessage.status = "failed";
          assistantMessage.error = String(data.message ?? "助手处理失败");
          assistantMessage.activity = "";
          break;
      }
    }, controller.signal);

    if (!terminalEventReceived) {
      assistantMessage.status = "interrupted";
      assistantMessage.activity = "";
      assistantMessage.error = "连接意外结束，本轮回答未完成。";
    }
  } catch (error) {
    if (controller.signal.aborted) {
      assistantMessage.status = "interrupted";
      assistantMessage.activity = "";
      assistantMessage.error = "本轮已停止。";
    } else {
      assistantMessage.status = "failed";
      assistantMessage.activity = "";
      assistantMessage.error = error.message || "群聊助手连接失败";
    }
  } finally {
    if (activeController === controller) activeController = null;
    isStreaming.value = false;
    scrollToBottom();
  }
};

const submitQuestion = () => {
  const text = question.value.trim();
  if (!text || isStreaming.value) return;
  question.value = "";
  runTask({ task: "chat", question: text }, text);
};

const submitSummary = () => runTask({ task: "summary" }, "总结群聊未读消息");

const retryMessage = (message) => {
  if (message.role !== "assistant" || message.status === "completed" || isStreaming.value) return;
  const previous = messages.value[messages.value.indexOf(message) - 1];
  if (previous?.role === "user" && previous.content === "总结群聊未读消息") {
    submitSummary();
    return;
  }
  const prompt = previous?.role === "user" ? previous.content : "";
  if (prompt) runTask({ task: "chat", question: prompt }, "");
};
</script>

<template>
  <aside
    v-if="conversation"
    class="group-assistant-panel flex min-w-0 flex-col overflow-hidden bg-[var(--im-surface)] text-sm max-[767px]:fixed max-[767px]:inset-0 max-[767px]:z-[46]"
  >
    <header class="flex min-h-[68px] items-center gap-3 border-b border-[var(--im-border)] px-4">
      <span
        class="grid size-9 place-items-center rounded-xl bg-gradient-to-br from-[var(--im-primary-tint)] to-[var(--im-primary-hover)] text-[var(--im-fs-11)] font-extrabold text-white shadow-[var(--im-shadow-md)]"
        >AI</span
      >
      <div class="min-w-0 flex-1">
        <strong class="block text-[var(--im-fs-13)] text-[var(--im-text-on-surface)]">群聊助手</strong>
        <span class="block truncate text-[var(--im-fs-10)] text-[var(--im-text-muted)]">{{ conversation.displayName }}</span>
      </div>
      <button
        class="rounded-lg bg-[var(--im-primary-soft)] px-2.5 py-2 text-[var(--im-fs-10)] font-semibold text-[var(--im-primary-hover)] hover:bg-[var(--im-primary-border)] disabled:opacity-50"
        type="button"
        :disabled="isStreaming || !groupId"
        @click="submitSummary"
      >
        总结未读
      </button>
      <button
        class="grid size-9 place-items-center rounded-lg bg-transparent text-[var(--im-text-muted)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)]"
        type="button"
        title="关闭群聊助手"
        @click="$emit('close')"
      >
        <X :size="18" />
      </button>
    </header>

    <div ref="feed" class="min-h-0 flex-1 overflow-y-auto bg-[linear-gradient(180deg,var(--im-surface-subtle)_0%,var(--im-surface-muted)_100%)] p-4">
      <div v-if="messages.length === 0" class="mx-auto mt-8 flex max-w-[280px] flex-col items-center text-center">
        <span class="mb-4 grid size-14 place-items-center rounded-[18px] bg-[var(--im-primary-soft)] text-lg font-extrabold text-[var(--im-primary-hover)]">AI</span>
        <h3 class="m-0 text-[var(--im-fs-15)] font-bold text-[var(--im-text)]">群聊助手</h3>
        <p class="mb-5 mt-2 text-[var(--im-fs-11)] leading-5 text-[var(--im-text-muted)]">
          可以总结群聊未读消息，也可以直接询问群聊里讨论过的内容。
        </p>
        <button
          class="rounded-full bg-[var(--im-primary-soft)] px-3 py-2 text-[var(--im-fs-11)] font-semibold text-[var(--im-primary-hover)] hover:bg-[var(--im-primary-border)] disabled:opacity-50"
          type="button"
          :disabled="isStreaming || !groupId"
          @click="submitSummary"
        >
          总结群聊未读消息
        </button>
      </div>

      <div v-else class="flex flex-col gap-4">
        <article v-for="(message, index) in messages" :key="index" class="flex flex-col gap-2">
          <div
            v-if="message.role === 'user'"
            class="ml-auto max-w-[88%] whitespace-pre-wrap break-words rounded-2xl rounded-br-md bg-[var(--im-primary)] px-3.5 py-2.5 text-[var(--im-fs-12)] leading-5 text-white"
          >
            {{ message.content }}
          </div>
          <div v-else class="max-w-[94%] rounded-2xl rounded-bl-md border border-[var(--im-border)] bg-[var(--im-surface)] px-3.5 py-3 shadow-[var(--im-shadow-sm)]">
            <div v-if="message.tools.length" class="mb-2 flex flex-wrap gap-1.5">
              <span
                v-for="(tool, toolIndex) in message.tools"
                :key="`${tool.name}-${toolIndex}`"
                class="rounded-full bg-[var(--im-page)] px-2 py-1 text-[var(--im-fs-9)] text-[var(--im-text-secondary)]"
              >
                {{ toolLabels[tool.name] || tool.name }}{{ tool.status === "running" ? "中…" : "· 已结束" }}
              </span>
            </div>
            <p v-if="message.content" class="m-0 whitespace-pre-wrap break-words text-[var(--im-fs-12)] leading-6 text-[var(--im-text)]">
              {{ message.content }}
            </p>
            <p v-else-if="message.status === 'streaming'" class="m-0 text-[var(--im-fs-11)] leading-5 text-[var(--im-text-muted)]">
              {{ message.activity || "正在生成回答…" }}
            </p>
            <p v-if="message.status === 'streaming' && message.content && message.activity" class="mb-0 mt-2 text-[var(--im-fs-10)] text-[var(--im-text-muted)]">
              {{ message.activity }}
            </p>
            <p v-if="message.error" class="mb-0 mt-2 text-[var(--im-fs-10)] leading-5 text-[var(--im-danger)]">
              {{ message.error }}
            </p>
            <button
              v-if="['failed', 'interrupted'].includes(message.status)"
              class="mt-2 bg-transparent p-0 text-[var(--im-fs-10)] font-semibold text-[var(--im-primary-hover)] hover:underline"
              type="button"
              :disabled="isStreaming"
              @click="retryMessage(message)"
            >
              再试一次
            </button>
          </div>
        </article>
      </div>
    </div>

    <form class="border-t border-[var(--im-border)] bg-[var(--im-surface)] p-3" @submit.prevent="submitQuestion">
      <div class="rounded-xl border border-[var(--im-border)] bg-[var(--im-primary-softer)] p-2 focus-within:border-[var(--im-primary-tint)]">
        <textarea
          v-model="question"
          class="max-h-28 min-h-10 w-full resize-none border-0 bg-transparent px-1 py-1.5 text-[var(--im-fs-12)] leading-5 text-[var(--im-text)] outline-none placeholder:text-[var(--im-text-faint)]"
          rows="2"
          maxlength="2000"
          placeholder="问问群聊助手…"
          :disabled="isStreaming || !groupId"
          @keydown.enter.exact.prevent="submitQuestion"
        ></textarea>
        <div class="flex items-center justify-between gap-3 px-1 pt-1">
          <span class="text-[var(--im-fs-9)] text-[var(--im-text-faint)]">助手只会读取你有权限访问的群聊消息</span>
          <button
            v-if="isStreaming"
            class="rounded-lg bg-[var(--im-page)] px-3 py-1.5 text-[var(--im-fs-10)] font-semibold text-[var(--im-text-secondary)] hover:bg-[var(--im-border)]"
            type="button"
            @click="stopRequest"
          >
            停止
          </button>
          <button
            v-else
            class="rounded-lg bg-[var(--im-primary)] px-3 py-1.5 text-[var(--im-fs-10)] font-semibold text-white hover:bg-[var(--im-primary-hover)] disabled:cursor-not-allowed disabled:opacity-40"
            type="submit"
            :disabled="!question.trim() || !groupId"
          >
            发送
          </button>
        </div>
      </div>
    </form>
  </aside>
</template>
