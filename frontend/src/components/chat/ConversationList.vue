<script setup>
import { LoaderCircle, Plus, Search, Users } from "@lucide/vue";
import { computed, ref } from "vue";
import { ConversationType } from "../../constants/conversation.js";
import { messageTypeLabel } from "../../constants/message.js";

const props = defineProps({
  conversations: { type: Array, default: () => [] },
  activeId: { type: String, default: "" },
  loading: { type: Boolean, default: false },
});

defineEmits(["select", "create-room"]);

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
  <section class="flex h-full min-w-0 flex-col bg-white text-sm">
    <header class="flex items-center justify-between px-5 pb-4 pt-6">
      <div>
        <p class="mb-1 text-[10px] font-bold tracking-[0.16em] text-[#5b35f5]">IMCHAT</p>
        <h1 class="m-0 text-[20px] font-bold tracking-[-0.03em] text-[#17122c]">消息</h1>
      </div>
      <div class="flex items-center gap-1.5">
        <button
          class="grid size-9 place-items-center rounded-[9px] border border-transparent bg-transparent text-[#8d91a3] transition-[background,color,border-color] duration-150 hover:border-[#ebe9f7] hover:bg-[#f8f7ff] hover:text-[#5b35f5]"
          type="button"
          title="新建房间"
          @click="$emit('create-room')"
        >
          <Plus :size="19" />
        </button>
      </div>
    </header>

    <label
      class="mx-4 flex min-h-10 items-center gap-2 rounded-[10px] border border-transparent bg-[#f8f8fb] px-3 text-[#a3a0b0] transition-[border-color,box-shadow] duration-150 focus-within:border-[#cfc5ff] focus-within:ring-4 focus-within:ring-[rgba(91,53,245,0.08)]"
    >
      <Search :size="17" />
      <input
        v-model="query"
        type="search"
        placeholder="搜索会话或消息"
        class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[#17122c] outline-0 placeholder:text-[#aaa6b5]"
      />
      <kbd class="rounded border border-[#e7e4ef] bg-white px-1.5 py-0.5 text-[10px] text-[#aaa6b5]"
        >⌘ K</kbd
      >
    </label>

    <div class="min-h-0 flex-1 overflow-y-auto px-4 pb-4 pt-4">
      <div
        v-if="loading"
        class="flex min-h-32 flex-col items-center justify-center gap-2 text-xs text-[#9994a7]"
      >
        <LoaderCircle class="animate-spin" :size="20" /><span>正在同步会话</span>
      </div>
      <div
        v-else-if="filtered.length === 0"
        class="flex min-h-32 flex-col items-center justify-center gap-1 text-center text-xs text-[#9994a7]"
      >
        <span>没有匹配的会话</span
        ><small class="text-[11px] text-[#b1adbb]">尝试更换搜索关键词</small>
      </div>
      <button
        v-for="conversation in filtered"
        v-else
        :key="conversation.conversationId"
        :class="
          conversation.conversationId === activeId
            ? 'border-[#cfc5ff] bg-[#f4f1ff] shadow-[0_8px_20px_rgba(91,53,245,0.07)]'
            : 'border-transparent hover:bg-[#faf9fd]'
        "
        class="group flex w-full items-center gap-3 rounded-xl border px-3 py-3 text-left transition-[background,border-color,box-shadow] duration-150"
        type="button"
        @click="$emit('select', conversation.conversationId)"
      >
        <span
          :class="
            Number(conversation.convType) === ConversationType.ROOM
              ? 'bg-[#6d4aff]'
              : 'bg-[#8b72d6]'
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
            v-else-if="Number(conversation.convType) === ConversationType.ROOM"
            :size="19"
          />
          <span v-else>{{ conversationDisplayName(conversation).slice(0, 1) }}</span>
        </span>
        <span class="min-w-0 flex-1">
          <span class="flex items-center justify-between gap-2">
            <strong class="truncate text-[12px] font-semibold text-[#29233e]">{{
              conversationDisplayName(conversation)
            }}</strong>
            <time class="flex-none text-[10px] text-[#aaa6b5]">{{
              conversationTime(conversation)
            }}</time>
          </span>
          <span class="mt-1 flex items-center justify-between gap-2">
            <span
              :class="
                conversation.conversationId === activeId ? 'text-[#6a55bc]' : 'text-[#9994a7]'
              "
              class="truncate text-[10px]"
              >{{ conversationPreview(conversation) }}</span
            >
            <span class="flex flex-none items-center gap-1 text-[#aaa6b5]">
              <b
                v-if="conversation.unread"
                class="grid min-w-5 place-items-center rounded-full bg-[#5b35f5] px-1 text-[10px] font-semibold text-white"
                >{{ conversation.unread > 99 ? "99+" : conversation.unread }}</b
              >
            </span>
          </span>
        </span>
      </button>
    </div>
  </section>
</template>
