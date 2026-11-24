<script setup>
import { computed } from "vue";
import { LogOut, MessageCircle, Moon, Settings, Sun, Users } from "@lucide/vue";
import { useTheme } from "../../composables/useTheme.js";

const props = defineProps({
  activeSection: { type: String, required: true },
  currentUser: { type: Object, required: true },
});

defineEmits(["select", "logout"]);

const { theme, toggle } = useTheme();

// 导航激活项索引，驱动滑动指示条位置（每项 46px + 4px gap）
const activeIndex = computed(() => ({ conversations: 0, contacts: 1, profile: 2 })[props.activeSection] ?? 0);
</script>

<template>
  <aside
    class="hidden h-full w-41 flex-none flex-col items-center rounded-2xl border border-[var(--im-border-strong)] bg-[var(--im-surface)] px-3 py-4.5 text-sm text-[var(--im-text-muted)] shadow-[var(--im-shadow-sm)] min-[768px]:flex"
  >
    <div class="flex w-full items-center gap-2.5 px-2">
      <span
        class="grid size-9 flex-none place-items-center rounded-[11px] bg-linear-to-br from-[var(--im-primary)] to-[var(--im-primary-hover)] text-white shadow-[var(--im-shadow-md)]"
      >
        <MessageCircle :size="20" />
      </span>
      <strong class="text-sm font-extrabold tracking-[-0.03em] text-[var(--im-text-strong)]">LinkChat</strong>
    </div>

    <nav class="relative mt-9 flex w-full flex-1 flex-col gap-1" aria-label="主要导航">
      <span
        class="pointer-events-none absolute left-0 top-0 h-11.5 w-full rounded-[10px] bg-linear-to-br from-[var(--im-primary)] to-[var(--im-primary-hover)] shadow-[var(--im-shadow-md)] transition-transform duration-300 ease-out"
        :style="{ transform: `translateY(${activeIndex * 50}px)` }"
        aria-hidden="true"
      />
      <button
        :class="
          activeSection === 'conversations'
            ? 'text-white'
            : 'text-[var(--im-text-muted)] hover:text-[var(--im-primary)]'
        "
        class="relative z-10 flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[var(--im-fs-11)] transition-[color,transform] duration-150 active:scale-[0.98]"
        type="button"
        title="消息"
        @click="$emit('select', 'conversations')"
      >
        <MessageCircle :size="21" /><span>会话</span>
      </button>
      <button
        :class="
          activeSection === 'contacts'
            ? 'text-white'
            : 'text-[var(--im-text-muted)] hover:text-[var(--im-primary)]'
        "
        class="relative z-10 flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[var(--im-fs-11)] transition-[color,transform] duration-150 active:scale-[0.98]"
        type="button"
        title="联系人"
        @click="$emit('select', 'contacts')"
      >
        <Users :size="21" /><span>通讯录</span>
      </button>
      <button
        :class="
          activeSection === 'profile'
            ? 'text-white'
            : 'text-[var(--im-text-muted)] hover:text-[var(--im-primary)]'
        "
        class="relative z-10 flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[var(--im-fs-11)] transition-[color,transform] duration-150 active:scale-[0.98]"
        type="button"
        title="我的"
        @click="$emit('select', 'profile')"
      >
        <Settings :size="21" /><span>设置</span>
      </button>
    </nav>

    <div class="mt-auto flex w-full flex-col gap-1">
      <button
        class="flex min-h-13 w-full items-center gap-2.5 rounded-[10px] px-2.5 text-left transition-colors duration-150 hover:bg-[var(--im-primary-softer)]"
        type="button"
        title="个人资料"
        @click="$emit('select', 'profile')"
      >
        <span
          class="relative grid size-9 flex-none place-items-center overflow-hidden rounded-full bg-[var(--im-primary-soft)] text-sm font-bold text-[var(--im-primary)]"
        >
          <img
            v-if="currentUser.avatar"
            class="size-full object-cover"
            :src="currentUser.avatar"
            :alt="currentUser.nickName || currentUser.name"
          />
          <span v-else>{{
            (currentUser.nickName || currentUser.name || currentUser.username || "我").slice(0, 1)
          }}</span>
        </span>
        <span class="min-w-0"
          ><strong class="block truncate text-[var(--im-fs-11)] font-semibold text-[var(--im-text-strong)]">{{
            currentUser.nickName || currentUser.name || currentUser.username || "我的账号"
          }}</strong>
        </span>
      </button>
      <button
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-3 text-[var(--im-fs-11)] text-[var(--im-text-muted)] transition-colors duration-150 hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)]"
        type="button"
        :title="theme === 'dark' ? '切换到浅色' : '切换到深色'"
        @click="toggle"
      >
        <Sun v-if="theme === 'dark'" :size="15" />
        <Moon v-else :size="15" />
        <span>{{ theme === 'dark' ? '浅色' : '深色' }}</span>
      </button>
      <button
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-3 text-[var(--im-fs-11)] text-[var(--im-text-muted)] transition-colors duration-150 hover:bg-[var(--im-danger-soft)] hover:text-[var(--im-danger)]"
        type="button"
        title="退出登录"
        @click="$emit('logout')"
      >
        <LogOut :size="15" /><span>退出</span>
      </button>
    </div>
  </aside>
</template>
