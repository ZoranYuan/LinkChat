<script setup>
import { Trash2, Users, X } from "@lucide/vue";
import { ConversationType } from "../../constants/conversation.js";

defineProps({ conversation: { type: Object, default: null } });
defineEmits(["close", "clear"]);
</script>

<template>
  <aside
    v-if="conversation"
    class="details-panel col-[5] flex min-w-0 w-full flex-col overflow-hidden bg-[var(--im-surface)] text-sm max-[767px]:fixed max-[767px]:inset-0 max-[767px]:z-[45]"
  >
    <header
      class="flex min-h-[68px] items-center justify-between border-b border-[var(--im-border)] px-4 text-[var(--im-fs-13)] font-semibold"
    >
      <strong>会话详情</strong
      ><button
        class="grid size-9 place-items-center rounded-lg bg-transparent text-[var(--im-text-muted)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)]"
        type="button"
        title="关闭"
        @click="$emit('close')"
      >
        <X :size="19" />
      </button>
    </header>
    <div class="min-h-0 overflow-auto px-[18px] py-[22px]">
      <section class="flex flex-col items-center border-b border-[var(--im-border)] pb-[22px] text-center">
        <span
          :class="
            Number(conversation.convType) === ConversationType.GROUP
              ? 'bg-[var(--im-primary)]'
              : 'bg-[var(--im-primary-tint)]'
          "
          class="grid size-16 place-items-center overflow-hidden rounded-[13px] text-2xl font-bold text-white"
          ><img
            v-if="conversation.avatar"
            class="size-full object-cover"
            :src="conversation.avatar"
            :alt="conversation.displayName"
          /><Users
            v-else-if="Number(conversation.convType) === ConversationType.GROUP"
            :size="26"
          /><b v-else>{{ (conversation.displayName || "未命名会话").slice(0, 1) }}</b></span
        >
        <h3 class="mb-1 mt-[13px] text-base font-bold">
          {{ conversation.displayName || "未命名会话" }}
        </h3>
        <p class="m-0 text-[var(--im-fs-10)] text-[var(--im-text-secondary)]">
          {{
            Number(conversation.convType) === ConversationType.GROUP
              ? `${conversation.group?.memberCount || 0} 位成员`
              : "私聊"
          }}
        </p>
      </section>

      <section
        v-if="Number(conversation.convType) === ConversationType.GROUP"
        class="border-b border-[var(--im-border)] py-[18px]"
      >
        <div class="mb-3 flex items-center justify-between">
          <strong class="text-xs">群聊信息</strong
          ><span class="text-[var(--im-fs-10)] text-[var(--im-text-faint)]"
            >{{ conversation.group?.memberCount || 0 }} 人</span
          >
        </div>
        <p class="m-0 text-[var(--im-fs-11)] leading-[1.7] text-[var(--im-text-secondary)]">
          {{ conversation.group?.description || "用于团队日常沟通与项目协作。" }}
        </p>
      </section>

      <button
        class="mt-4 flex min-h-11 w-full items-center gap-2 rounded-lg bg-[var(--im-danger-soft)] px-3 text-[var(--im-fs-11)] text-[var(--im-danger)] hover:bg-[var(--im-danger-soft)]"
        type="button"
        @click="$emit('clear')"
      >
        <Trash2 :size="18" />清空聊天记录
      </button>
    </div>
  </aside>
</template>
