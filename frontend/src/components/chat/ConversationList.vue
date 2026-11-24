<script setup>
import { LoaderCircle, Plus, Search, Users } from "@lucide/vue";
import { computed, ref } from "vue";
import { ConversationType } from "../../constants/conversation.js";
import { messageTypeLabel } from "../../constants/message.js";

const props = defineProps({
  conversations: { type: Array, default: () => [] },
  activeId: { type: String, default: "" },
  loading: { type: Boolean, default: false },
  connection: { type: String, default: "disconnected" },
});

defineEmits(["select", "create-group", "retry"]);

const query = ref("");
const conversationDisplayName = (conversation) => conversation.displayName || "未命名会话";
const conversationPreview = (conversation) => {
  const lastMessage = conversation.lastMessage;
  if (!lastMessage) return "暂无消息";

  return messageTypeLabel(lastMessage.cType) || lastMessage.content || "暂无消息";
};
const conversationTime = (conversation) => {
  if (!conversation.lastMessage?.sendTime) return "";
  return new Date(Number(conversation.lastMessage.sendTime)).toLocaleTimeString("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
};
const filtered = computed(() => {
  const keyword = query.value.trim().toLowerCase();
  return props.conversations.filter(
    (item) =>
      !keyword ||
      conversationDisplayName(item).toLowerCase().includes(keyword) ||
      conversationPreview(item).toLowerCase().includes(keyword),
  );
});
</script>

<template>
  <section class="flex h-full min-w-0 flex-col bg-[var(--im-surface)] text-sm">
    <header class="flex items-center justify-between px-5 pb-4 pt-6">
      <div>
        <p class="mb-1 text-[var(--im-fs-10)] font-bold tracking-[0.16em] text-[var(--im-primary)]">LinkChat</p>
        <h1 class="m-0 text-[var(--im-fs-20)] font-bold tracking-[-0.03em] text-[var(--im-text)]">消息</h1>
      </div>
      <div class="flex items-center gap-1.5">
        <button
          class="grid size-9 place-items-center rounded-[9px] border border-transparent bg-transparent text-[var(--im-text-muted)] transition-[background,color,border-color] duration-150 hover:border-[var(--im-border)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)]"
          type="button"
          title="新建群聊"
          @click="$emit('create-group')"
        >
          <Plus :size="19" />
        </button>
      </div>
    </header>

    <label
      class="mx-4 flex min-h-10 items-center gap-2 rounded-[10px] border border-transparent bg-[var(--im-surface-muted)] px-3 text-[var(--im-text-faint)] transition-[border-color,box-shadow] duration-150 focus-within:border-[var(--im-primary-border)] focus-within:ring-4 focus-within:ring-[var(--im-shadow-focus)]"
    >
      <Search :size="17" />
      <input
        v-model="query"
        type="search"
        placeholder="搜索会话或消息"
        class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[var(--im-text)] outline-0 placeholder:text-[var(--im-text-faint)]"
      />
      <kbd class="rounded border border-[var(--im-border)] bg-[var(--im-surface)] px-1.5 py-0.5 text-[var(--im-fs-10)] text-[var(--im-text-faint)]"
        >⌘ K</kbd
      >
    </label>

    <div
      v-if="connection === 'connecting' || connection === 'disconnected'"
      class="mx-4 mt-2 flex min-h-9 items-center justify-between gap-3 rounded-lg border border-[var(--im-danger-soft)] bg-[var(--im-danger-soft)] px-3 text-[var(--im-fs-11)] text-[var(--im-danger)]"
      role="status"
    >
      <span>{{ connection === "connecting" ? "正在连接服务器…" : "无法连接服务器" }}</span>
      <button
        v-if="connection === 'disconnected'"
        class="shrink-0 bg-transparent font-semibold text-[var(--im-danger)] hover:text-[var(--im-text)]"
        type="button"
        @click="$emit('retry')"
      >重试</button>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto px-4 pb-4 pt-4">
      <div
        v-if="loading"
        class="flex min-h-32 flex-col items-center justify-center gap-2 text-xs text-[var(--im-text-muted)]"
      >
        <LoaderCircle class="animate-spin" :size="20" /><span>正在同步会话</span>
      </div>
      <div
        v-else-if="filtered.length === 0"
        class="flex min-h-32 flex-col items-center justify-center gap-1 text-center text-xs text-[var(--im-text-muted)]"
      >
        <span>没有匹配的会话</span
        ><small class="text-[var(--im-fs-11)] text-[var(--im-text-faint)]">尝试更换搜索关键词</small>
      </div>
      <button
        v-for="conversation in filtered"
        v-else
        :key="conversation.conversationId"
        :class="
          conversation.conversationId === activeId
            ? 'border-[var(--im-primary-border)] bg-[var(--im-primary-soft)] shadow-[var(--im-shadow-md)]'
            : 'border-transparent hover:bg-[var(--im-surface-subtle)]'
        "
        class="group flex w-full items-center gap-3 rounded-xl border px-3 py-3 text-left transition-[background,border-color,box-shadow,transform] duration-150 hover:-translate-y-px"
        type="button"
        @click="$emit('select', conversation.conversationId)"
      >
        <span
          :class="
            Number(conversation.convType) === ConversationType.GROUP
              ? 'bg-[var(--im-primary)]'
              : 'bg-[var(--im-primary-tint)]'
          "
          class="relative grid size-11 flex-none place-items-center overflow-visible rounded-xl text-sm font-bold text-white"
        >
          <img
            v-if="conversation.avatar"
            class="size-full rounded-xl object-cover"
            :src="conversation.avatar"
            :alt="conversationDisplayName(conversation)"
          />
          <Users
            v-else-if="Number(conversation.convType) === ConversationType.GROUP"
            :size="19"
          />
          <span v-else>{{ conversationDisplayName(conversation).slice(0, 1) }}</span>
        </span>
        <span class="min-w-0 flex-1">
          <span class="flex items-center justify-between gap-2">
            <strong class="truncate text-[var(--im-fs-12)] font-semibold text-[var(--im-text)]">{{
              conversationDisplayName(conversation)
            }}</strong>
            <time class="flex-none text-[var(--im-fs-10)] text-[var(--im-text-faint)]">{{
              conversationTime(conversation)
            }}</time>
          </span>
          <span class="mt-1 flex items-center justify-between gap-2">
            <span
              :class="
                conversation.conversationId === activeId ? 'text-[var(--im-primary-tint)]' : 'text-[var(--im-text-muted)]'
              "
              class="truncate text-[var(--im-fs-10)]"
              >{{ conversationPreview(conversation) }}</span
            >
            <span class="flex flex-none items-center gap-1 text-[var(--im-text-faint)]">
              <b
                v-if="conversation.unread"
                class="grid min-w-5 place-items-center rounded-full bg-[var(--im-primary)] px-1 text-[var(--im-fs-10)] font-semibold text-white"
                >{{ conversation.unread > 99 ? "99+" : conversation.unread }}</b
              >
            </span>
          </span>
        </span>
      </button>
    </div>
  </section>
</template>
