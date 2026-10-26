<script setup>
import { CircleAlert } from "@lucide/vue";

const props = defineProps({
  open: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    default: "确认操作",
  },
  description: {
    type: String,
    default: "请确认是否继续执行当前操作。",
  },
  confirmLabel: {
    type: String,
    default: "确认",
  },
  cancelLabel: {
    type: String,
    default: "取消",
  },
  loading: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["update:open", "confirm", "cancel"]);

const closeDialog = () => {
  emit("update:open", false);
  emit("cancel");
};
</script>

<template>
  <teleport to="body">
    <transition
      enter-active-class="transition duration-150 ease-out"
      enter-from-class="opacity-0"
      enter-to-class="opacity-100"
      leave-active-class="transition duration-100 ease-in"
      leave-from-class="opacity-100"
      leave-to-class="opacity-0"
    >
      <div
        v-if="props.open"
        class="fixed inset-0 z-[60] flex items-center justify-center bg-slate-900/45 px-4 text-sm"
        @click.self="closeDialog"
      >
        <div
          class="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-[0_24px_70px_rgba(15,23,42,0.18)]"
        >
          <div class="flex items-start gap-3">
            <div class="rounded-full bg-rose-50 p-3 text-rose-600">
              <CircleAlert :size="20" />
            </div>
            <div class="min-w-0 flex-1">
              <p class="text-base font-semibold">{{ title }}</p>
              <p class="mt-2 text-sm text-slate-500">{{ description }}</p>
            </div>
          </div>
          <div class="mt-6 flex justify-end gap-3">
            <button
              class="min-h-10 rounded-lg border border-slate-200 bg-white px-4 text-sm text-slate-600 transition-colors hover:bg-slate-50"
              type="button"
              @click="closeDialog"
            >
              {{ cancelLabel }}
            </button>
            <button
              class="min-h-10 min-w-24 rounded-lg bg-rose-600 px-4 text-sm font-semibold text-white transition-colors hover:bg-rose-700 disabled:cursor-not-allowed disabled:opacity-50"
              type="button"
              :disabled="loading"
              @click="$emit('confirm')"
            >
              {{ loading ? "处理中..." : confirmLabel }}
            </button>
          </div>
        </div>
      </div>
    </transition>
  </teleport>
</template>
