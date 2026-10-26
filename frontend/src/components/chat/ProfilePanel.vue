<script setup>
import { Check, LoaderCircle, LogOut, Pencil, X } from "@lucide/vue";
import { computed, reactive, ref, watch } from "vue";

const props = defineProps({
  currentUser: { type: Object, required: true },
  saveProfile: { type: Function, required: true },
});
const emit = defineEmits(["logout"]);

const editing = ref(false);
const saving = ref(false);
const errorMessage = ref("");
const form = reactive({ username: "", nickName: "", avatar: "" });

const syncForm = (user) => {
  form.username = user?.username || "";
  form.nickName = user?.nickName || "";
  form.avatar = user?.avatar || "";
};

watch(
  () => props.currentUser,
  (user) => {
    if (!editing.value) syncForm(user);
  },
  { immediate: true },
);

const isDirty = computed(
  () =>
    form.username !== (props.currentUser.username || "") ||
    form.nickName !== (props.currentUser.nickName || "") ||
    form.avatar !== (props.currentUser.avatar || ""),
);

const startEditing = () => {
  syncForm(props.currentUser);
  errorMessage.value = "";
  editing.value = true;
};

const cancelEditing = () => {
  syncForm(props.currentUser);
  errorMessage.value = "";
  editing.value = false;
};

const submit = async () => {
  if (!isDirty.value || saving.value) return;
  saving.value = true;
  errorMessage.value = "";
  try {
    await props.saveProfile({
      username: form.username.trim(),
      nickName: form.nickName.trim(),
      avatar: form.avatar.trim(),
    });
    editing.value = false;
  } catch (error) {
    errorMessage.value = error?.message || "资料保存失败";
  } finally {
    saving.value = false;
  }
};
</script>

<template>
  <section class="flex h-full min-w-0 flex-col bg-white text-sm">
    <header class="px-5 pb-4 pt-6">
      <p class="mb-1 text-[10px] font-bold tracking-[0.16em] text-[#5b35f5]">ACCOUNT</p>
      <h1 class="m-0 text-[22px] font-bold tracking-[-0.03em] text-[#17122c]">我的</h1>
    </header>
    <div class="min-h-0 flex-1 overflow-y-auto px-5 pb-5">
      <div
        class="relative grid justify-items-center rounded-2xl border border-[#eeeaf7] bg-gradient-to-b from-[#faf9ff] to-white px-5 py-7 text-center"
      >
        <button
          v-if="!editing"
          class="absolute right-3 top-3 grid size-8 place-items-center rounded-lg bg-white text-[#77718c] shadow-sm transition-colors hover:text-[#5b35f5]"
          type="button"
          title="编辑资料"
          @click="startEditing"
        >
          <Pencil :size="15" />
        </button>
        <span
          class="grid size-20 place-items-center overflow-hidden rounded-3xl text-2xl font-bold text-white shadow-[0_12px_28px_rgba(91,53,245,0.18)]"
          :style="{ backgroundColor: currentUser.avatarColor || '#5b35f5' }"
        >
          <img
            v-if="currentUser.avatar"
            class="size-full object-cover"
            :src="currentUser.avatar"
            :alt="currentUser.nickName || currentUser.username"
          />
          <span v-else>{{
            (currentUser.nickName || currentUser.name || currentUser.username || "我").slice(0, 1)
          }}</span>
        </span>
        <h2 class="mt-4 text-lg font-bold text-[#27213c]">
          {{ currentUser.nickName || currentUser.name || currentUser.username || "IM 用户" }}
        </h2>
        <p class="mt-1 text-xs text-[#aaa6b5]">
          @{{ currentUser.username || currentUser.userId || "user" }}
        </p>
        <span
          class="mt-3 inline-flex items-center gap-1.5 rounded-full bg-[#edf8f2] px-2.5 py-1 text-[11px] font-semibold text-[#42a977]"
          ><i class="size-1.5 rounded-full bg-[#62c894]"></i>在线</span
        >
      </div>

      <form
        v-if="editing"
        class="mt-4 grid gap-3 rounded-xl border border-[#eeeaf7] bg-white p-4"
        @submit.prevent="submit"
      >
        <label class="grid gap-1.5 text-[11px] font-semibold text-[#6d6780]"
          >用户名
          <input
            v-model="form.username"
            maxlength="16"
            autocomplete="username"
            class="h-10 rounded-lg border border-[#e7e2f1] bg-[#fcfbff] px-3 text-sm text-[#342b4d] outline-none transition-colors focus:border-[#a493ff]"
          />
        </label>
        <label class="grid gap-1.5 text-[11px] font-semibold text-[#6d6780]"
          >昵称
          <input
            v-model="form.nickName"
            maxlength="64"
            autocomplete="nickname"
            class="h-10 rounded-lg border border-[#e7e2f1] bg-[#fcfbff] px-3 text-sm text-[#342b4d] outline-none transition-colors focus:border-[#a493ff]"
          />
        </label>
        <label class="grid gap-1.5 text-[11px] font-semibold text-[#6d6780]"
          >头像地址
          <input
            v-model="form.avatar"
            maxlength="100"
            inputmode="url"
            class="h-10 rounded-lg border border-[#e7e2f1] bg-[#fcfbff] px-3 text-sm text-[#342b4d] outline-none transition-colors focus:border-[#a493ff]"
          />
        </label>
        <p v-if="errorMessage" class="m-0 text-[11px] text-[#c34d65]">{{ errorMessage }}</p>
        <div class="flex justify-end gap-2">
          <button
            class="grid size-9 place-items-center rounded-lg border border-[#e7e2f1] bg-white text-[#77718c] hover:bg-[#f7f4ff]"
            type="button"
            title="取消编辑"
            :disabled="saving"
            @click="cancelEditing"
          >
            <X :size="16" />
          </button>
          <button
            class="grid size-9 place-items-center rounded-lg bg-[#5b35f5] text-white hover:bg-[#4d2ee0] disabled:cursor-not-allowed disabled:opacity-50"
            type="submit"
            title="保存资料"
            :disabled="!isDirty || saving"
          >
            <LoaderCircle v-if="saving" class="animate-spin" :size="16" />
            <Check v-else :size="16" />
          </button>
        </div>
      </form>

      <button
        class="mt-5 flex min-h-11 w-full items-center justify-center gap-2 rounded-xl border border-[#f2d6dc] bg-[#fff7f8] text-xs font-semibold text-[#c34d65] transition-colors hover:bg-[#fff0f3]"
        type="button"
        @click="$emit('logout')"
      >
        <LogOut :size="18" />退出登录
      </button>
    </div>
  </section>
</template>
