<script setup>
import { LogOut, MessageCircle, Settings, Users } from "@lucide/vue";

defineProps({
  activeSection: { type: String, required: true },
  currentUser: { type: Object, required: true },
  connection: { type: String, default: "disconnected" },
});

defineEmits(["select", "logout", "retry"]);
</script>

<template>
  <aside
    class="hidden h-full w-41 flex-none flex-col items-center rounded-2xl border border-[#e5e0f3] bg-white px-3 py-4.5 text-sm text-[#98a2b3] shadow-[0_14px_35px_rgba(58,43,110,0.08)] min-[768px]:flex"
  >
    <div class="flex w-full items-center gap-2.5 px-2">
      <span
        class="grid size-9 flex-none place-items-center rounded-[11px] bg-linear-to-br from-[#7355ff] to-[#4724d8] text-white shadow-[0_8px_18px_rgba(91,53,245,0.2)]"
      >
        <MessageCircle :size="20" />
      </span>
      <strong class="text-sm font-extrabold tracking-[-0.03em] text-[#241b43]">IMChat</strong>
    </div>

    <nav class="mt-9 flex w-full flex-1 flex-col gap-1" aria-label="主要导航">
      <button
        :class="
          activeSection === 'conversations'
            ? 'bg-linear-to-br from-[#6d4aff] to-[#4a29e5] text-white shadow-[0_12px_24px_rgba(91,53,245,0.22)]'
            : 'text-[#8d91a3] hover:bg-[#f1efff] hover:text-[#5b35f5]'
        "
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[11px] transition-[background,color,box-shadow] duration-150"
        type="button"
        title="消息"
        @click="$emit('select', 'conversations')"
      >
        <MessageCircle :size="21" /><span>会话</span>
      </button>
      <button
        :class="
          activeSection === 'contacts'
            ? 'bg-linear-to-br from-[#6d4aff] to-[#4a29e5] text-white shadow-[0_12px_24px_rgba(91,53,245,0.22)]'
            : 'text-[#8d91a3] hover:bg-[#f1efff] hover:text-[#5b35f5]'
        "
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[11px] transition-[background,color,box-shadow] duration-150"
        type="button"
        title="联系人"
        @click="$emit('select', 'contacts')"
      >
        <Users :size="21" /><span>通讯录</span>
      </button>
      <button
        :class="
          activeSection === 'profile'
            ? 'bg-linear-to-br from-[#6d4aff] to-[#4a29e5] text-white shadow-[0_12px_24px_rgba(91,53,245,0.22)]'
            : 'text-[#8d91a3] hover:bg-[#f1efff] hover:text-[#5b35f5]'
        "
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[11px] transition-[background,color,box-shadow] duration-150"
        type="button"
        title="我的"
        @click="$emit('select', 'profile')"
      >
        <Settings :size="21" /><span>设置</span>
      </button>
    </nav>

    <div class="mt-auto flex w-full flex-col gap-1">
      <button
        class="flex min-h-13 w-full items-center gap-2.5 rounded-[10px] px-2.5 text-left transition-colors duration-150 hover:bg-[#f8f6ff]"
        type="button"
        title="个人资料"
        @click="$emit('select', 'profile')"
      >
        <span
          class="relative grid size-9 flex-none place-items-center overflow-hidden rounded-full bg-[#eee9ff] text-sm font-bold text-[#5b35f5]"
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
          ><strong class="block truncate text-[11px] font-semibold text-[#342b4d]">{{
            currentUser.nickName || currentUser.name || currentUser.username || "我的账号"
          }}</strong>
        </span>
      </button>
      <button
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-4 text-[11px] text-[#8d91a3] transition-colors duration-150 hover:bg-[#f1efff] hover:text-[#5b35f5]"
        type="button"
        :title="connection === 'connected' ? '实时连接正常' : '点击重新连接'"
        @click="$emit('retry')"
      >
        <i
          :class="[
            'size-2.5 rounded-full',
            connection === 'connected'
              ? 'bg-[#62c894] shadow-[0_0_0_4px_rgba(98,200,148,0.14)]'
              : connection === 'connecting'
                ? 'bg-[#d29a42]'
                : 'bg-[#dc6570]',
          ]"
        ></i>
        <span>{{ connection === "connected" ? "在线" : "连接" }}</span>
      </button>
      <button
        class="flex min-h-11.5 w-full flex-row items-center justify-start gap-3 rounded-[10px] px-3 text-[11px] text-[#8d91a3] transition-colors duration-150 hover:bg-[#fff0f3] hover:text-[#dc6570]"
        type="button"
        title="退出登录"
        @click="$emit('logout')"
      >
        <LogOut :size="15" /><span>退出</span>
      </button>
    </div>
  </aside>
</template>
