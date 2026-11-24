<script setup>
import { Eye, EyeOff, Globe2, LockKeyhole, MessageCircle, Phone, QrCode } from "@lucide/vue";
import { computed, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useChatStore } from "../modules/chat/chatStore.js";
import { messageTips } from "../utils/messageTips.js";

const route = useRoute();
const router = useRouter();
const { authenticate } = useChatStore();

const mode = ref("login");
const showPassword = ref(false);
const submitting = ref(false);
const form = reactive({ account: "", password: "", reconfirmPassword: "", remember: true });
const errors = reactive({ account: "", password: "", reconfirmPassword: "", submit: "" });

const title = computed(() => (mode.value === "login" ? "欢迎回来" : "创建你的账号"));
const validate = () => {
  errors.account =
    mode.value === "register"
      ? /^1\d{10}$/.test(form.account)
        ? ""
        : "注册请输入 11 位手机号"
      : form.account.trim()
        ? ""
        : "请输入手机号或用户名";
  errors.password = form.password.length >= 6 ? "" : "密码至少需要 6 位";
  errors.reconfirmPassword =
    mode.value === "register" && form.reconfirmPassword !== form.password
      ? "两次输入的密码不一致"
      : "";
  errors.submit = "";
  return !errors.account && !errors.password && !errors.reconfirmPassword;
};

const submit = async () => {
  if (!validate()) return;
  submitting.value = true;
  try {
    await authenticate({ mode: mode.value, ...form });
    if (mode.value === "register") {
      messageTips.success("注册成功，请登录");
      mode.value = "login";
      form.password = "";
      form.reconfirmPassword = "";
      return;
    }
    router.replace(typeof route.query.redirect === "string" ? route.query.redirect : "/chat");
  } catch (error) {
    errors.submit = error.message;
    messageTips.error({
      title: mode.value === "login" ? "登录失败" : "注册失败",
      message: error.message,
    });
  } finally {
    submitting.value = false;
  }
};

watch(mode, () => {
  errors.account = "";
  errors.password = "";
  errors.reconfirmPassword = "";
  errors.submit = "";
});
</script>

<template>
  <main
    class="relative isolate grid min-h-dvh place-items-center overflow-hidden bg-[var(--im-surface-muted)] px-5 py-8 text-sm text-[var(--im-text)]"
  >
    <span
      class="pointer-events-none absolute inset-0 -z-10 opacity-30 bg-[radial-gradient(var(--im-shadow-focus)_1px,transparent_1px)] bg-size-[24px_24px] mask-[linear-gradient(to_bottom,rgba(0,0,0,0.5),transparent_70%)]"
      aria-hidden="true"
    ></span>
    <span
      class="auth-orb-left pointer-events-none absolute -left-25 top-[12%] -z-10 size-65 rounded-full bg-[rgba(136,111,255,0.13)] blur-[1px]"
      aria-hidden="true"
    ></span>
    <span
      class="auth-orb-right pointer-events-none absolute -right-22.5 bottom-[5%] -z-10 size-57.5 rounded-full bg-[rgba(191,173,255,0.2)] blur-[1px]"
      aria-hidden="true"
    ></span>

    <section
      class="w-full max-w-105 rounded-3xl border border-[var(--im-border)] bg-[var(--im-surface)]/92 px-10 pb-7.5 pt-9.5 shadow-[0_24px_70px_var(--im-shadow-sm)] backdrop-blur-[18px] max-[520px]:px-5.5 max-[520px]:pb-6.25 max-[520px]:pt-7.5"
    >
      <header class="grid justify-items-center text-center">
        <span
          class="grid size-13.5 place-items-center rounded-2xl bg-linear-to-br from-[var(--im-primary)] to-[var(--im-primary-hover)] text-white shadow-[var(--im-shadow-md)]"
        >
          <MessageCircle :size="25" />
        </span>
        <span class="mt-2.75 text-base font-bold tracking-[-0.02em]">LinkChat</span>
        <span class="mt-7.5 text-[var(--im-fs-10)] font-extrabold tracking-[0.16em] text-[var(--im-primary)]"
          >ACCOUNT ACCESS</span
        >
        <h1 class="mt-2.5 text-[var(--im-fs-29)] font-bold tracking-[-0.04em]">{{ title }}</h1>
        <p class="mt-2 text-[var(--im-fs-13)] text-[var(--im-text-secondary)]">
          {{ mode === "login" ? "登录后继续你的对话" : "使用手机号创建一个新账号" }}
        </p>
      </header>

      <div
        class="mt-7 grid grid-cols-2 gap-1 rounded-xl bg-[var(--im-page)] p-1"
        role="tablist"
        aria-label="账号操作"
      >
        <button
          :class="
            mode === 'login'
              ? 'bg-[var(--im-surface)]! text-[var(--im-primary)]! shadow-[0_5px_13px_var(--im-shadow-sm)]!'
              : 'text-[var(--im-text-muted)]'
          "
          class="min-h-9.5 rounded-lg bg-transparent text-[var(--im-fs-13)] font-semibold transition-[background,box-shadow,color] duration-150 ease-in-out"
          type="button"
          role="tab"
          :aria-selected="mode === 'login'"
          @click="mode = 'login'"
        >
          登录
        </button>
        <button
          :class="
            mode === 'register'
              ? 'bg-[var(--im-surface)]! text-[var(--im-primary)]! shadow-[0_5px_13px_var(--im-shadow-sm)]!'
              : 'text-[var(--im-text-muted)]'
          "
          class="min-h-9.5 rounded-lg bg-transparent text-[var(--im-fs-13)] font-semibold transition-[background,box-shadow,color] duration-150 ease-in-out"
          type="button"
          role="tab"
          :aria-selected="mode === 'register'"
          @click="mode = 'register'"
        >
          注册
        </button>
      </div>

      <form class="mt-6.25 grid gap-4.25" @submit.prevent="submit">
        <label class="grid gap-2 text-sm">
          <span class="text-xs font-semibold text-[var(--im-text-secondary)]">{{
            mode === "register" ? "手机号" : "手机号或用户名"
          }}</span>
          <div
            :class="{ 'border-[var(--im-danger)]!': errors.account }"
            class="flex min-h-12.5 items-center gap-2.5 rounded-[11px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] px-3.25 text-[var(--im-text-faint)] transition-[border-color,box-shadow] duration-150 focus-within:border-[var(--im-primary-tint)] focus-within:ring-4 focus-within:ring-[var(--im-shadow-focus)]"
          >
            <Phone :size="18" />
            <input
              v-model.trim="form.account"
              :inputmode="mode === 'register' ? 'tel' : 'text'"
              :autocomplete="mode === 'register' ? 'tel' : 'username'"
              :maxlength="mode === 'register' ? 11 : 64"
              :placeholder="mode === 'register' ? '请输入手机号' : '请输入手机号或用户名'"
              :aria-invalid="Boolean(errors.account)"
              class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[var(--im-text)] outline-0 placeholder:text-[var(--im-text-faint)]"
            />
          </div>
          <small v-if="errors.account" class="text-[var(--im-fs-11)] text-[var(--im-danger)]">{{
            errors.account
          }}</small>
        </label>

        <label class="grid gap-2 text-sm">
          <span class="text-xs font-semibold text-[var(--im-text-secondary)]">密码</span>
          <div
            :class="{ 'border-[var(--im-danger)]!': errors.password }"
            class="flex min-h-12.5 items-center gap-2.5 rounded-[11px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] px-3.25 text-[var(--im-text-faint)] transition-[border-color,box-shadow] duration-150 focus-within:border-[var(--im-primary-tint)] focus-within:ring-4 focus-within:ring-[var(--im-shadow-focus)]"
          >
            <LockKeyhole :size="18" />
            <input
              v-model="form.password"
              :type="showPassword ? 'text' : 'password'"
              :autocomplete="mode === 'login' ? 'current-password' : 'new-password'"
              placeholder="请输入密码"
              :aria-invalid="Boolean(errors.password)"
              class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[var(--im-text)] outline-0 placeholder:text-[var(--im-text-faint)]"
            />
            <button
              class="grid size-7.25 flex-none place-items-center rounded-lg bg-transparent text-[var(--im-text-faint)] transition-colors hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)]"
              type="button"
              :title="showPassword ? '隐藏密码' : '显示密码'"
              @click="showPassword = !showPassword"
            >
              <EyeOff v-if="showPassword" :size="18" />
              <Eye v-else :size="18" />
            </button>
          </div>
          <small v-if="errors.password" class="text-[var(--im-fs-11)] text-[var(--im-danger)]">{{
            errors.password
          }}</small>
        </label>

        <label v-if="mode === 'register'" class="grid gap-2 text-sm">
          <span class="text-xs font-semibold text-[var(--im-text-secondary)]">确认密码</span>
          <div
            :class="{ 'border-[var(--im-danger)]!': errors.reconfirmPassword }"
            class="flex min-h-12.5 items-center gap-2.5 rounded-[11px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] px-3.25 text-[var(--im-text-faint)] transition-[border-color,box-shadow] duration-150 focus-within:border-[var(--im-primary-tint)] focus-within:ring-4 focus-within:ring-[var(--im-shadow-focus)]"
          >
            <LockKeyhole :size="18" />
            <input
              v-model="form.reconfirmPassword"
              :type="showPassword ? 'text' : 'password'"
              autocomplete="new-password"
              placeholder="请再次输入密码"
              :aria-invalid="Boolean(errors.reconfirmPassword)"
              class="min-w-0 flex-1 border-0 bg-transparent text-sm text-[var(--im-text)] outline-0 placeholder:text-[var(--im-text-faint)]"
            />
          </div>
          <small v-if="errors.reconfirmPassword" class="text-[var(--im-fs-11)] text-[var(--im-danger)]">{{
            errors.reconfirmPassword
          }}</small>
        </label>

        <div
          v-if="mode === 'login'"
          class="flex items-center justify-between text-xs text-[var(--im-text-secondary)]"
        >
          <label class="flex items-center gap-1.75"
            ><input v-model="form.remember" type="checkbox" class="size-3.75 accent-[var(--im-primary)]" />
            <span>记住我</span></label
          >
          <button
            type="button"
            class="bg-transparent font-semibold text-[var(--im-primary)]"
            @click="messageTips.info('暂未提供找回密码接口')"
          >
            忘记密码?
          </button>
        </div>

        <button
          class="auth-simple-submit relative z-0 min-h-12.5 rounded-[11px] bg-transparent text-[var(--im-fs-13)] font-bold text-white isolate"
          type="submit"
          :disabled="submitting"
        >
          {{ submitting ? "正在提交..." : mode === "login" ? "登录" : "注册" }}
        </button>
      </form>

      <div
        class="mt-6.75 flex items-center gap-2.75 text-[var(--im-fs-11)] text-[var(--im-text-faint)] before:h-px before:flex-1 before:bg-[var(--im-border-strong)] before:content-[''] after:h-px after:flex-1 after:bg-[var(--im-border-strong)] after:content-['']"
      >
        <span>其他登录方式</span>
      </div>
      <div class="mt-3.75 flex justify-center gap-2.5">
        <button
          class="grid size-10 place-items-center rounded-[10px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] text-[var(--im-text-muted)] transition-[border-color,color,transform] duration-150 hover:-translate-y-0.5 hover:border-[var(--im-primary-border)] hover:text-[var(--im-primary)]"
          type="button"
          title="微信登录"
          @click="messageTips.info('暂未提供第三方登录接口')"
        >
          <MessageCircle :size="18" />
        </button>
        <button
          class="grid size-10 place-items-center rounded-[10px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] text-[var(--im-text-muted)] transition-[border-color,color,transform] duration-150 hover:-translate-y-0.5 hover:border-[var(--im-primary-border)] hover:text-[var(--im-primary)]"
          type="button"
          title="GitHub 登录"
          @click="messageTips.info('暂未提供第三方登录接口')"
        >
          <Globe2 :size="18" />
        </button>
        <button
          class="grid size-10 place-items-center rounded-[10px] border border-[var(--im-border-strong)] bg-[var(--im-surface)] text-[var(--im-text-muted)] transition-[border-color,color,transform] duration-150 hover:-translate-y-0.5 hover:border-[var(--im-primary-border)] hover:text-[var(--im-primary)]"
          type="button"
          title="扫码登录"
          @click="messageTips.info('暂未提供扫码登录接口')"
        >
          <QrCode :size="18" />
        </button>
      </div>

      <p class="mt-6 text-center text-xs text-[var(--im-text-muted)]">
        {{ mode === "login" ? "还没有账号？" : "已有账号？" }}
        <button
          type="button"
          class="bg-transparent font-semibold text-[var(--im-primary)]"
          @click="mode = mode === 'login' ? 'register' : 'login'"
        >
          {{ mode === "login" ? "立即注册" : "返回登录" }}
        </button>
      </p>
    </section>
  </main>
</template>

<style scoped>
@keyframes auth-orb-drift {
  0%,
  100% {
    transform: translate3d(0, 0, 0);
  }

  50% {
    transform: translate3d(12px, -18px, 0);
  }
}

.auth-orb-left {
  animation: auth-orb-drift 8s ease-in-out infinite;
}

.auth-orb-right {
  animation: auth-orb-drift 10s ease-in-out -2s infinite reverse;
}

.auth-simple-submit::before {
  position: absolute;
  z-index: -1;
  inset: 0;
  border-radius: inherit;
  background: linear-gradient(135deg, var(--im-primary), var(--im-primary-hover));
  box-shadow: 0 13px 25px var(--im-shadow-focus);
  content: "";
  pointer-events: none;
  transition:
    transform 180ms ease,
    box-shadow 180ms ease;
}

.auth-simple-submit:hover:not(:disabled)::before {
  box-shadow: 0 18px 32px var(--im-shadow-focus);
  transform: scale(1.1);
}

.auth-simple-submit:active:not(:disabled)::before {
  transform: scale(0.99);
}
</style>
