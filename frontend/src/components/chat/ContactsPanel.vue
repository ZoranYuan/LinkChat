<script setup>
import { Check, MessageCircle, Search, UserPlus, X } from "@lucide/vue";
import { computed, reactive, ref } from "vue";

const props = defineProps({
  contacts: { type: Array, default: () => [] },
  requests: { type: Array, default: () => [] },
  onAddFriend: { type: Function, default: null },
});

const emit = defineEmits(["open-chat", "handle-request"]);
const query = ref("");
const showAdd = ref(false);
const adding = ref(false);
const addForm = reactive({ keyword: "", message: "你好，我想加你为好友" });

const filteredContacts = computed(() => {
  const keyword = query.value.trim().toLowerCase();
  return props.contacts.filter(
    (item) =>
      !keyword ||
      item.name.toLowerCase().includes(keyword) ||
      item.username?.toLowerCase().includes(keyword),
  );
});

const submitRequest = async () => {
  if (!addForm.keyword.trim()) return;
  adding.value = true;
  try {
    if (typeof props.onAddFriend !== "function") return;
    const succeeded = await props.onAddFriend({ ...addForm });
    if (succeeded === false) return;
    showAdd.value = false;
    addForm.keyword = "";
  } finally {
    adding.value = false;
  }
};
</script>

<template>
  <section class="flex h-full min-w-0 flex-col bg-[var(--im-surface)] text-sm">
    <header class="flex items-center justify-between px-5 pb-4 pt-6">
      <div>
        <p class="mb-1 text-[var(--im-fs-10)] font-bold tracking-[0.16em] text-[var(--im-primary)]">CONTACTS</p>
        <h1 class="m-0 text-[var(--im-fs-22)] font-bold tracking-[-0.03em] text-[var(--im-text)]">联系人</h1>
      </div>
      <button
        class="grid size-9 place-items-center rounded-[9px] border border-transparent bg-transparent text-[var(--im-text-muted)] transition-[background,color,border-color] duration-150 hover:border-[var(--im-border)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)]"
        type="button"
        title="添加好友"
        @click="showAdd = !showAdd"
      >
        <UserPlus :size="19" />
      </button>
    </header>

    <label
      class="mx-4 flex min-h-10 items-center gap-2 rounded-[10px] border border-transparent bg-[var(--im-surface-muted)] px-3 text-[var(--im-text-faint)] transition-[border-color,box-shadow] duration-150 focus-within:border-[var(--im-primary-border)] focus-within:ring-4 focus-within:ring-[var(--im-shadow-focus)]"
    >
      <Search :size="17" /><input
        v-model="query"
        type="search"
        placeholder="搜索联系人"
        class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[var(--im-text)] outline-0 placeholder:text-[var(--im-text-faint)]"
      />
    </label>

    <form
      v-if="showAdd"
      class="mx-4 mt-4 grid gap-3 rounded-xl border border-[var(--im-primary-border)] bg-[var(--im-primary-softer)] p-4"
      @submit.prevent="submitRequest"
    >
      <div class="flex items-center justify-between">
        <strong class="text-[var(--im-fs-13)] text-[var(--im-text)]">添加好友</strong
        ><button
          class="grid size-7 place-items-center rounded-lg bg-transparent text-[var(--im-text-muted)] hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)]"
          type="button"
          title="关闭"
          @click="showAdd = false"
        >
          <X :size="17" />
        </button>
      </div>
      <label class="grid gap-1.5"
        ><span class="text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]">手机号或用户名</span
        ><input
          v-model.trim="addForm.keyword"
          required
          placeholder="输入对方的手机号或用户名"
          class="min-h-9 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface)] px-2.5 text-xs outline-0 transition focus:border-[var(--im-primary-tint)] focus:ring-4 focus:ring-[var(--im-shadow-focus)]"
      /></label>
      <label class="grid gap-1.5"
        ><span class="text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]">申请留言</span
        ><input
          v-model.trim="addForm.message"
          required
          maxlength="60"
          class="min-h-9 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface)] px-2.5 text-xs outline-0 transition focus:border-[var(--im-primary-tint)] focus:ring-4 focus:ring-[var(--im-shadow-focus)]"
      /></label>
      <button
        class="min-h-9 rounded-lg bg-[var(--im-primary)] px-3 text-xs font-semibold text-white transition hover:bg-[var(--im-primary-hover)] disabled:cursor-not-allowed disabled:opacity-50"
        type="submit"
        :disabled="adding"
      >
        {{ adding ? "发送中..." : "发送申请" }}
      </button>
    </form>

    <div class="min-h-0 flex-1 overflow-y-auto px-4 pb-4 pt-4">
      <section v-if="requests.length" class="mb-6">
        <div
          class="mb-2 flex items-center justify-between px-1 text-xs font-semibold text-[var(--im-text-secondary)]"
        >
          <span>新的好友</span
          ><b
            class="grid min-w-5 place-items-center rounded-full bg-[var(--im-primary-soft)] px-1 text-[var(--im-fs-10)] text-[var(--im-primary)]"
            >{{ requests.length }}</b
          >
        </div>
        <article
          v-for="request in requests"
          :key="request.id"
          class="flex items-center gap-2.5 rounded-xl border border-[var(--im-border-soft)] px-3 py-3"
        >
          <span
            class="grid size-10 flex-none place-items-center rounded-xl text-sm font-bold text-white"
            :style="{ backgroundColor: request.avatarColor }"
            >{{ request.name.slice(0, 1) }}</span
          >
          <span class="min-w-0 flex-1"
            ><strong class="block truncate text-[var(--im-fs-13)] text-[var(--im-text-strong)]">{{ request.name }}</strong
            ><small class="mt-1 block truncate text-[var(--im-fs-11)] text-[var(--im-text-faint)]">{{
              request.note
            }}</small></span
          >
          <span class="flex gap-1"
            ><button
              class="grid size-7 place-items-center rounded-lg bg-[var(--im-success-soft)] text-[var(--im-success)] hover:bg-[var(--im-success-soft)]"
              type="button"
              title="同意"
              @click="$emit('handle-request', request, true)"
            >
              <Check :size="16" /></button
            ><button
              class="grid size-7 place-items-center rounded-lg bg-[var(--im-danger-soft)] text-[var(--im-danger)] hover:bg-[var(--im-danger-soft)]"
              type="button"
              title="拒绝"
              @click="$emit('handle-request', request, false)"
            >
              <X :size="16" /></button
          ></span>
        </article>
      </section>

      <section>
        <div
          class="mb-2 flex items-center justify-between px-1 text-xs font-semibold text-[var(--im-text-secondary)]"
        >
          <span>全部联系人</span
          ><span class="text-[var(--im-fs-11)] font-normal text-[var(--im-text-faint)]">{{ filteredContacts.length }}</span>
        </div>
        <button
          v-for="contact in filteredContacts"
          :key="contact.id"
          class="group flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left transition-[background,transform] duration-150 hover:-translate-y-px hover:bg-[var(--im-primary-softer)]"
          type="button"
          @click="$emit('open-chat', contact)"
        >
          <span
            class="relative grid size-10 flex-none place-items-center overflow-visible rounded-xl text-sm font-bold text-white"
            :style="{ backgroundColor: contact.avatarColor }"
            ><img
              v-if="contact.avatar"
              class="size-full rounded-xl object-cover"
              :src="contact.avatar"
              :alt="contact.name" /><span v-else>{{ contact.name.slice(0, 1) }}</span
            ><i
              v-if="contact.online"
              class="absolute -bottom-0.5 -right-0.5 size-2.5 rounded-full border-2 border-white bg-[var(--im-success)]"
            ></i
          ></span>
          <span class="min-w-0 flex-1"
            ><strong class="block truncate text-[var(--im-fs-13)] text-[var(--im-text-strong)]">{{ contact.name }}</strong
            ><small class="mt-1 block truncate text-[var(--im-fs-11)] text-[var(--im-text-faint)]"
              >{{ contact.role
              }}<template v-if="contact.username"> · {{ contact.username }}</template></small
            ></span
          >
          <MessageCircle
            class="text-[var(--im-text-faint)] transition-colors group-hover:text-[var(--im-primary)]"
            :size="17"
          />
        </button>
        <div
          v-if="!filteredContacts.length"
          class="flex min-h-24 items-center justify-center text-xs text-[var(--im-text-faint)]"
        >
          没有找到联系人
        </div>
      </section>
    </div>
  </section>
</template>
