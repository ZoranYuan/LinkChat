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
  <section class="flex h-full min-w-0 flex-col bg-white text-sm">
    <header class="flex items-center justify-between px-5 pb-4 pt-6">
      <div>
        <p class="mb-1 text-[10px] font-bold tracking-[0.16em] text-[#5b35f5]">CONTACTS</p>
        <h1 class="m-0 text-[22px] font-bold tracking-[-0.03em] text-[#17122c]">联系人</h1>
      </div>
      <button
        class="grid size-9 place-items-center rounded-[9px] border border-transparent bg-transparent text-[#8d91a3] transition-[background,color,border-color] duration-150 hover:border-[#ebe9f7] hover:bg-[#f8f7ff] hover:text-[#5b35f5]"
        type="button"
        title="添加好友"
        @click="showAdd = !showAdd"
      >
        <UserPlus :size="19" />
      </button>
    </header>

    <label
      class="mx-4 flex min-h-10 items-center gap-2 rounded-[10px] border border-transparent bg-[#f8f8fb] px-3 text-[#a3a0b0] transition-[border-color,box-shadow] duration-150 focus-within:border-[#cfc5ff] focus-within:ring-4 focus-within:ring-[rgba(91,53,245,0.08)]"
    >
      <Search :size="17" /><input
        v-model="query"
        type="search"
        placeholder="搜索联系人"
        class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[#17122c] outline-0 placeholder:text-[#aaa6b5]"
      />
    </label>

    <form
      v-if="showAdd"
      class="mx-4 mt-4 grid gap-3 rounded-xl border border-[#e7e2f8] bg-[#faf9ff] p-4"
      @submit.prevent="submitRequest"
    >
      <div class="flex items-center justify-between">
        <strong class="text-[13px] text-[#332b50]">添加好友</strong
        ><button
          class="grid size-7 place-items-center rounded-lg bg-transparent text-[#9893a6] hover:bg-[#f0edff] hover:text-[#5b35f5]"
          type="button"
          title="关闭"
          @click="showAdd = false"
        >
          <X :size="17" />
        </button>
      </div>
      <label class="grid gap-1.5"
        ><span class="text-[11px] font-semibold text-[#625c72]">手机号或用户名</span
        ><input
          v-model.trim="addForm.keyword"
          required
          placeholder="输入对方的手机号或用户名"
          class="min-h-9 rounded-lg border border-[#e5e1f0] bg-white px-2.5 text-xs outline-0 transition focus:border-[#a493ff] focus:ring-4 focus:ring-[rgba(91,53,245,0.08)]"
      /></label>
      <label class="grid gap-1.5"
        ><span class="text-[11px] font-semibold text-[#625c72]">申请留言</span
        ><input
          v-model.trim="addForm.message"
          required
          maxlength="60"
          class="min-h-9 rounded-lg border border-[#e5e1f0] bg-white px-2.5 text-xs outline-0 transition focus:border-[#a493ff] focus:ring-4 focus:ring-[rgba(91,53,245,0.08)]"
      /></label>
      <button
        class="min-h-9 rounded-lg bg-[#5b35f5] px-3 text-xs font-semibold text-white transition hover:bg-[#4724d8] disabled:cursor-not-allowed disabled:opacity-50"
        type="submit"
        :disabled="adding"
      >
        {{ adding ? "发送中..." : "发送申请" }}
      </button>
    </form>

    <div class="min-h-0 flex-1 overflow-y-auto px-4 pb-4 pt-4">
      <section v-if="requests.length" class="mb-6">
        <div
          class="mb-2 flex items-center justify-between px-1 text-xs font-semibold text-[#6c6679]"
        >
          <span>新的好友</span
          ><b
            class="grid min-w-5 place-items-center rounded-full bg-[#f0edff] px-1 text-[10px] text-[#5b35f5]"
            >{{ requests.length }}</b
          >
        </div>
        <article
          v-for="request in requests"
          :key="request.id"
          class="flex items-center gap-2.5 rounded-xl border border-[#eeeaf7] px-3 py-3"
        >
          <span
            class="grid size-10 flex-none place-items-center rounded-xl text-sm font-bold text-white"
            :style="{ backgroundColor: request.avatarColor }"
            >{{ request.name.slice(0, 1) }}</span
          >
          <span class="min-w-0 flex-1"
            ><strong class="block truncate text-[13px] text-[#332d45]">{{ request.name }}</strong
            ><small class="mt-1 block truncate text-[11px] text-[#aaa6b5]">{{
              request.note
            }}</small></span
          >
          <span class="flex gap-1"
            ><button
              class="grid size-7 place-items-center rounded-lg bg-[#edf8f2] text-[#42a977] hover:bg-[#dff3e8]"
              type="button"
              title="同意"
              @click="$emit('handle-request', request, true)"
            >
              <Check :size="16" /></button
            ><button
              class="grid size-7 place-items-center rounded-lg bg-[#fff1f3] text-[#d35c70] hover:bg-[#ffe5e9]"
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
          class="mb-2 flex items-center justify-between px-1 text-xs font-semibold text-[#6c6679]"
        >
          <span>全部联系人</span
          ><span class="text-[11px] font-normal text-[#aaa6b5]">{{ filteredContacts.length }}</span>
        </div>
        <button
          v-for="contact in filteredContacts"
          :key="contact.id"
          class="group flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left transition-colors duration-150 hover:bg-[#faf9ff]"
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
              class="absolute -bottom-0.5 -right-0.5 size-2.5 rounded-full border-2 border-white bg-[#62c894]"
            ></i
          ></span>
          <span class="min-w-0 flex-1"
            ><strong class="block truncate text-[13px] text-[#332d45]">{{ contact.name }}</strong
            ><small class="mt-1 block truncate text-[11px] text-[#aaa6b5]"
              >{{ contact.role
              }}<template v-if="contact.username"> · {{ contact.username }}</template></small
            ></span
          >
          <MessageCircle
            class="text-[#b2adbb] transition-colors group-hover:text-[#5b35f5]"
            :size="17"
          />
        </button>
        <div
          v-if="!filteredContacts.length"
          class="flex min-h-24 items-center justify-center text-xs text-[#aaa6b5]"
        >
          没有找到联系人
        </div>
      </section>
    </div>
  </section>
</template>
