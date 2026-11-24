<script setup>
import { Check, LoaderCircle, LogOut, Pencil, X } from "@lucide/vue";
import { computed, reactive, ref, watch } from "vue";
import { ElSlider } from "element-plus";
import {
  useFontScale,
  FONT_SCALE_MIN,
  FONT_SCALE_MAX,
  FONT_SCALE_STEP,
} from "../../composables/useFontScale.js";

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

const { fontScale, setScale } = useFontScale();
const sliderValue = computed({
  get: () => fontScale.value,
  set: (v) => setScale(Number(v)),
});
const fontMarks = {
  [FONT_SCALE_MIN]: "小",
  1: "标准",
  [FONT_SCALE_MAX]: "大",
};

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
  <section class="flex h-full min-w-0 flex-col bg-[var(--im-surface)] text-sm">
    <header class="px-5 pb-4 pt-6">
      <p class="mb-1 text-[var(--im-fs-10)] font-bold tracking-[0.16em] text-[var(--im-primary)]">ACCOUNT</p>
      <h1 class="m-0 text-[var(--im-fs-22)] font-bold tracking-[-0.03em] text-[var(--im-text)]">我的</h1>
    </header>
    <div class="min-h-0 flex-1 overflow-y-auto px-5 pb-5">
      <div
        class="relative grid justify-items-center rounded-2xl border border-[var(--im-border-soft)] bg-gradient-to-b from-[var(--im-primary-softer)] to-white px-5 py-7 text-center"
      >
        <button
          v-if="!editing"
          class="absolute right-3 top-3 grid size-8 place-items-center rounded-lg bg-[var(--im-surface)] text-[var(--im-text-secondary)] shadow-sm transition-colors hover:text-[var(--im-primary)]"
          type="button"
          title="编辑资料"
          @click="startEditing"
        >
          <Pencil :size="15" />
        </button>
        <span
          class="grid size-20 place-items-center overflow-hidden rounded-3xl text-2xl font-bold text-white shadow-[var(--im-shadow-md)]"
          :style="{ backgroundColor: currentUser.avatarColor || 'var(--im-primary)' }"
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
        <h2 class="mt-4 text-lg font-bold text-[var(--im-text)]">
          {{ currentUser.nickName || currentUser.name || currentUser.username || "IM 用户" }}
        </h2>
        <p class="mt-1 text-xs text-[var(--im-text-faint)]">
          @{{ currentUser.username || currentUser.userId || "user" }}
        </p>
        <span
          class="mt-3 inline-flex items-center gap-1.5 rounded-full bg-[var(--im-success-soft)] px-2.5 py-1 text-[var(--im-fs-11)] font-semibold text-[var(--im-success)]"
          ><i class="size-1.5 rounded-full bg-[var(--im-success)]"></i>在线</span
        >
      </div>

      <section
        class="mt-4 rounded-xl border border-[var(--im-border-soft)] bg-[var(--im-surface)] p-4"
        aria-label="显示偏好"
      >
        <div class="mb-4 flex items-center gap-2">
          <span class="grid size-7 place-items-center rounded-lg bg-[var(--im-primary-soft)] text-[var(--im-primary)]">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7V4h16v3M9 20h6M12 4v16" /></svg>
          </span>
          <h3 class="m-0 text-[var(--im-fs-13)] font-semibold text-[var(--im-text-strong)]">显示偏好</h3>
        </div>
        <div class="grid grid-cols-[auto_1fr] items-center gap-4">
          <span class="whitespace-nowrap text-[var(--im-fs-12)] text-[var(--im-text-secondary)]">字体大小</span>
          <div class="min-w-0 pt-5">
            <ElSlider
              v-model="sliderValue"
              class="font-size-slider"
              :min="FONT_SCALE_MIN"
              :max="FONT_SCALE_MAX"
              :step="FONT_SCALE_STEP"
              :marks="fontMarks"
              :show-tooltip="false"
            />
          </div>
        </div>
      </section>

      <form
        v-if="editing"
        class="mt-4 grid gap-3 rounded-xl border border-[var(--im-border-soft)] bg-[var(--im-surface)] p-4"
        @submit.prevent="submit"
      >
        <label class="grid gap-1.5 text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]"
          >用户名
          <input
            v-model="form.username"
            maxlength="16"
            autocomplete="username"
            class="h-10 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface-subtle)] px-3 text-sm text-[var(--im-text-strong)] outline-none transition-colors focus:border-[var(--im-primary-tint)]"
          />
        </label>
        <label class="grid gap-1.5 text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]"
          >昵称
          <input
            v-model="form.nickName"
            maxlength="64"
            autocomplete="nickname"
            class="h-10 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface-subtle)] px-3 text-sm text-[var(--im-text-strong)] outline-none transition-colors focus:border-[var(--im-primary-tint)]"
          />
        </label>
        <label class="grid gap-1.5 text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]"
          >头像地址
          <input
            v-model="form.avatar"
            maxlength="100"
            inputmode="url"
            class="h-10 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface-subtle)] px-3 text-sm text-[var(--im-text-strong)] outline-none transition-colors focus:border-[var(--im-primary-tint)]"
          />
        </label>
        <p v-if="errorMessage" class="m-0 text-[var(--im-fs-11)] text-[var(--im-danger)]">{{ errorMessage }}</p>
        <div class="flex justify-end gap-2">
          <button
            class="grid size-9 place-items-center rounded-lg border border-[var(--im-border)] bg-[var(--im-surface)] text-[var(--im-text-secondary)] hover:bg-[var(--im-primary-softer)]"
            type="button"
            title="取消编辑"
            :disabled="saving"
            @click="cancelEditing"
          >
            <X :size="16" />
          </button>
          <button
            class="grid size-9 place-items-center rounded-lg bg-[var(--im-primary)] text-white hover:bg-[var(--im-primary-hover)] disabled:cursor-not-allowed disabled:opacity-50"
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
        class="mt-5 flex min-h-11 w-full items-center justify-center gap-2 rounded-xl border border-[var(--im-danger)] bg-[var(--im-danger-soft)] text-xs font-semibold text-[var(--im-danger)] transition-colors hover:bg-[var(--im-danger-soft)]"
        type="button"
        @click="$emit('logout')"
      >
        <LogOut :size="18" />退出登录
      </button>
    </div>
  </section>
</template>
