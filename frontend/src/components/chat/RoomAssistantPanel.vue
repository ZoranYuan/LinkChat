<script setup>
import { X } from "@lucide/vue";
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { streamRoomAgent } from "../../services/http/api.js";

const props = defineProps({ conversation: { type: Object, default: null } });
defineEmits(["close"]);

const roomId = computed(() => props.conversation?.room?.roomId || props.conversation?.targetId || "");
const messages = ref([]);
const question = ref("");
const feed = ref(null);
const isStreaming = ref(false);
let activeController = null;

const toolLabels = {
  get_unread_messages: "读取未读消息",
  search_messages: "检索房间消息",
  sync_room_messages: "同步房间消息",
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

watch(roomId, () => {
  stopRequest();
  messages.value = [];
  isStreaming.value = false;
});

onBeforeUnmount(stopRequest);

const runTask = async (body, prompt) => {
  if (!roomId.value || isStreaming.value) return;

  const assistantMessage = {
    role: "assistant",
    content: "",
    status: "streaming",
    activity: "正在连接房间助手…",
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
    await streamRoomAgent(roomId.value, body, ({ event, data }) => {
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
      assistantMessage.error = error.message || "房间助手连接失败";
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

const submitSummary = () => runTask({ task: "summary" }, "总结房间未读消息");

const retryMessage = (message) => {
  if (message.role !== "assistant" || message.status === "completed" || isStreaming.value) return;
  const previous = messages.value[messages.value.indexOf(message) - 1];
  if (previous?.role === "user" && previous.content === "总结房间未读消息") {
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
    class="room-assistant-panel flex min-w-0 flex-col overflow-hidden bg-white text-sm max-[767px]:fixed max-[767px]:inset-0 max-[767px]:z-[46]"
  >
    <header class="flex min-h-[68px] items-center gap-3 border-b border-[#ebe9f7] px-4">
      <span
        class="grid size-9 place-items-center rounded-xl bg-gradient-to-br from-[#8063ff] to-[#5234dc] text-[11px] font-extrabold text-white shadow-[0_8px_18px_rgba(91,53,245,0.22)]"
        >AI</span
      >
      <div class="min-w-0 flex-1">
        <strong class="block text-[13px] text-[#202033]">房间助手</strong>
        <span class="block truncate text-[10px] text-[#9893a6]">{{ conversation.displayName }}</span>
      </div>
      <button
        class="rounded-lg bg-[#f2efff] px-2.5 py-2 text-[10px] font-semibold text-[#6847e8] hover:bg-[#e9e3ff] disabled:opacity-50"
        type="button"
        :disabled="isStreaming || !roomId"
        @click="submitSummary"
      >
        总结未读
      </button>
      <button
        class="grid size-9 place-items-center rounded-lg bg-transparent text-[#9893a6] hover:bg-[#f8f7ff] hover:text-[#5b35f5]"
        type="button"
        title="关闭房间助手"
        @click="$emit('close')"
      >
        <X :size="18" />
      </button>
    </header>

    <div ref="feed" class="min-h-0 flex-1 overflow-y-auto bg-[linear-gradient(180deg,#fcfbff_0%,#f8f7fc_100%)] p-4">
      <div v-if="messages.length === 0" class="mx-auto mt-8 flex max-w-[280px] flex-col items-center text-center">
        <span class="mb-4 grid size-14 place-items-center rounded-[18px] bg-[#eee9ff] text-lg font-extrabold text-[#6847e8]">AI</span>
        <h3 class="m-0 text-[15px] font-bold text-[#28243a]">房间助手</h3>
        <p class="mb-5 mt-2 text-[11px] leading-5 text-[#89849a]">
          可以总结房间未读消息，也可以直接询问房间里讨论过的内容。
        </p>
        <button
          class="rounded-full bg-[#eee9ff] px-3 py-2 text-[11px] font-semibold text-[#6847e8] hover:bg-[#e5deff] disabled:opacity-50"
          type="button"
          :disabled="isStreaming || !roomId"
          @click="submitSummary"
        >
          总结房间未读消息
        </button>
      </div>

      <div v-else class="flex flex-col gap-4">
        <article v-for="(message, index) in messages" :key="index" class="flex flex-col gap-2">
          <div
            v-if="message.role === 'user'"
            class="ml-auto max-w-[88%] whitespace-pre-wrap break-words rounded-2xl rounded-br-md bg-[#6243e8] px-3.5 py-2.5 text-[12px] leading-5 text-white"
          >
            {{ message.content }}
          </div>
          <div v-else class="max-w-[94%] rounded-2xl rounded-bl-md border border-[#ebe8f3] bg-white px-3.5 py-3 shadow-[0_4px_14px_rgba(58,43,110,0.04)]">
            <div v-if="message.tools.length" class="mb-2 flex flex-wrap gap-1.5">
              <span
                v-for="(tool, toolIndex) in message.tools"
                :key="`${tool.name}-${toolIndex}`"
                class="rounded-full bg-[#f5f3fa] px-2 py-1 text-[9px] text-[#77718c]"
              >
                {{ toolLabels[tool.name] || tool.name }}{{ tool.status === "running" ? "中…" : "· 已结束" }}
              </span>
            </div>
            <p v-if="message.content" class="m-0 whitespace-pre-wrap break-words text-[12px] leading-6 text-[#383448]">
              {{ message.content }}
            </p>
            <p v-else-if="message.status === 'streaming'" class="m-0 text-[11px] leading-5 text-[#9893a6]">
              {{ message.activity || "正在生成回答…" }}
            </p>
            <p v-if="message.status === 'streaming' && message.content && message.activity" class="mb-0 mt-2 text-[10px] text-[#9893a6]">
              {{ message.activity }}
            </p>
            <p v-if="message.error" class="mb-0 mt-2 text-[10px] leading-5 text-[#c34d65]">
              {{ message.error }}
            </p>
            <button
              v-if="['failed', 'interrupted'].includes(message.status)"
              class="mt-2 bg-transparent p-0 text-[10px] font-semibold text-[#6847e8] hover:underline"
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

    <form class="border-t border-[#ebe9f7] bg-white p-3" @submit.prevent="submitQuestion">
      <div class="rounded-xl border border-[#e8e5f0] bg-[#fbfaff] p-2 focus-within:border-[#b8aaff]">
        <textarea
          v-model="question"
          class="max-h-28 min-h-10 w-full resize-none border-0 bg-transparent px-1 py-1.5 text-[12px] leading-5 text-[#28243a] outline-none placeholder:text-[#aaa6b5]"
          rows="2"
          maxlength="2000"
          placeholder="问问房间助手…"
          :disabled="isStreaming || !roomId"
          @keydown.enter.exact.prevent="submitQuestion"
        ></textarea>
        <div class="flex items-center justify-between gap-3 px-1 pt-1">
          <span class="text-[9px] text-[#aaa6b5]">助手只会读取你有权限访问的房间消息</span>
          <button
            v-if="isStreaming"
            class="rounded-lg bg-[#f5f3fa] px-3 py-1.5 text-[10px] font-semibold text-[#6e687e] hover:bg-[#ebe8f3]"
            type="button"
            @click="stopRequest"
          >
            停止
          </button>
          <button
            v-else
            class="rounded-lg bg-[#6243e8] px-3 py-1.5 text-[10px] font-semibold text-white hover:bg-[#5234dc] disabled:cursor-not-allowed disabled:opacity-40"
            type="submit"
            :disabled="!question.trim() || !roomId"
          >
            发送
          </button>
        </div>
      </div>
    </form>
  </aside>
</template>
