<script setup>
import {
  ArrowLeft,
  Download,
  FileText,
  Folder,
  Image as ImageIcon,
  Info,
  LoaderCircle,
  MoreHorizontal,
  Pause,
  Play,
  Send,
  Share2,
  Users,
  Video,
  X,
} from "@lucide/vue";
import { ElImage } from "element-plus";
import "element-plus/es/components/image/style/css";
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { ConversationType } from "../../constants/conversation.js";
import { MessageType, messageViewType } from "../../constants/message.js";

const props = defineProps({
  conversation: { type: Object, default: null },
  messages: { type: Array, default: () => [] },
  currentUser: { type: Object, required: true },
  connection: { type: String, default: "disconnected" },
  loading: { type: Boolean, default: false },
  hasMore: { type: Boolean, default: false },
  mobile: { type: Boolean, default: false },
  visible: { type: Boolean, default: true },
  shareLoading: { type: Boolean, default: false },
});

const emit = defineEmits([
  "back",
  "details",
  "assistant",
  "share-group",
  "send",
  "attachment",
  "load-older",
  "retry-message",
  "pause-upload",
  "resume-upload",
  "cancel-upload",
]);
const draft = ref("");
const messageList = ref(null);
const messageContent = ref(null);
const shouldStickToBottom = ref(true);
const fileInput = ref(null);
const composerHeight = ref(200);
const isResizingComposer = ref(false);
const resizeStartY = ref(0);
const resizeStartHeight = ref(160);
const MIN_COMPOSER_HEIGHT = 180;
const MAX_COMPOSER_HEIGHT = 300;
const BOTTOM_THRESHOLD = 20;
let messageResizeObserver = null;

const scrollToBottom = () => {
  if (!messageList.value) return;

  messageList.value.scrollTo({
    top: messageList.value.scrollHeight,
    behavior: "auto",
  });
};

const scheduleScrollToBottom = (force = false) => {
  if (!force && !shouldStickToBottom.value) return;

  nextTick(() => {
    if (!force && !shouldStickToBottom.value) return;

    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(scrollToBottom);
    } else {
      scrollToBottom();
    }
  });
};

const handleMessageScroll = () => {
  const element = messageList.value;
  if (!element) return;

  const distance = element.scrollHeight - element.scrollTop - element.clientHeight;
  shouldStickToBottom.value = distance <= BOTTOM_THRESHOLD;
};

const observeMessageContent = () => {
  if (messageResizeObserver && messageContent.value) {
    messageResizeObserver.disconnect();
    messageResizeObserver.observe(messageContent.value);
  }
};

const submit = () => {
  const text = draft.value.trim();
  if (!text) return;
  emit("send", text);
  draft.value = "";
  // 不在这里滚动：消息尚未进入 props.messages，滚的是旧位置；
  // 统一由 watch(messages.length) 在消息渲染后滚一次，避免双重滚动跳动。
};

const handleEnter = (event) => {
  if (event.isComposing || event.shiftKey) return;
  event.preventDefault();
  submit();
};

const isMine = (message) => message.senderId === props.currentUser.userId;
const shouldShowMessageStatus = (message) => {
  if (!isMine(message)) return false;
  if (message.sendStatus === "failed") return false;
  if (message.error) return true;
  return props.messages.at(-1) === message;
};
const isLastMineMessageRead = (message) => {
  if (!isMine(message) || Number(props.conversation?.convType) !== ConversationType.PRIVATE_CHAT)
    return false;

  const seq = Number(message.seq) || 0;
  const readWatermark = Number(props.conversation?.readWatermark) || 0;
  return seq > 0 && seq <= readWatermark;
};
const isGroup = (conversation) => Number(conversation?.convType) === ConversationType.GROUP;
const conversationDisplayName = (conversation) => conversation?.displayName || "未命名会话";
const memberCount = (conversation) => conversation?.group?.memberCount || 0;
const displayType = (message) => messageViewType(message.cType);

// 名字优先级：消息自带字段（后端补齐后为 username/nickname） > 会话对方资料 > 短 ID。
// 后端 MessageResponse 目前只返回 senderId；私聊可用 peerUser 补全，群聊暂时回退短 ID。
const senderNames = (message) => {
  const peer = props.conversation?.peerUser;
  const isPeer = !!peer && String(peer.userId) === String(message.senderId);
  return {
    username: message.senderUsername || (isPeer ? peer.userName || "" : ""),
    nickname:
      message.senderNickname ||
      (isPeer ? peer.nickName || "" : "") ||
      message.senderUsername ||
      "",
  };
};
const senderName = (message) => {
  const { username, nickname } = senderNames(message);
  return nickname || username || String(message.senderId || "成员").slice(0, 8);
};
// 群聊里展示为「用户名/昵称」，两者相同则只显示一个。
const senderLabel = (message) => {
  const { username, nickname } = senderNames(message);
  if (username && nickname && String(username) !== String(nickname)) {
    return `${username}/${nickname}`;
  }
  return senderName(message);
};
const messageTime = (message) => {
  if (!message?.sendTime) return "";
  const date = new Date(Number(message.sendTime));
  const now = new Date();
  if (date.toDateString() === now.toDateString()) {
    return date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false });
  }
  return `${date.getMonth() + 1}/${date.getDate()}`;
};
// key 必须在消息整个生命周期内稳定：本地发送时只有 clientMsgId，服务端 ack 后
// Object.assign 会补上 messageId。若优先用 messageId，key 会在 ack 瞬间改变，
// TransitionGroup 会当成「删旧元素+插新元素」触发 leave/enter，表现为消息闪烁。
// clientMsgId 本地生成、ack 后保留、mergeMessageLists 也保留，全程稳定，故优先。
const messageKey = (message) =>
  message.clientMsgId || message.messageId || `${message.conversationId}:${message.seq}`;
const uploadStageText = (message) => {
  switch (message.uploadStage) {
    case "hashing":
      return "计算文件指纹";
    case "initializing":
      return "初始化上传";
    case "uploading":
      return "上传中";
    case "completing":
      return "合并文件";
    case "paused":
      return "已暂停";
    case "queued":
      return "等待上传";
    default:
      return "上传中";
  }
};
const isUploadPending = (message) =>
  !message.error &&
  ["queued", "hashing", "initializing", "uploading", "completing", "paused"].includes(
    message.uploadStage,
  );
const messageStatusText = (message) => {
  if (isUploadPending(message)) {
    return `${uploadStageText(message)} ${Number(message.uploadProgress) || 0}%`;
  }
  if (message.error) return "发送失败";
  if (message.sendStatus === "completed") return "已发送";

  if (!message.messageId) return "发送中";
  if (Number(message.seq) <= 0) return message.accepted ? "已发送" : "发送中";
  return isLastMineMessageRead(message) ? "已读" : "已发送";
};
const shortFileName = (name) => {
  const value = String(name || "");
  const chars = Array.from(value);
  return chars.length > 5 ? `${chars.slice(0, 5).join("")}...` : value;
};
const fileTypeText = (message) => {
  const contentType = String(message.mimeType || "").toLowerCase();
  if (contentType === "application/pdf") return "PDF";
  if (contentType.includes("word") || contentType.includes("document")) return "Word";
  if (contentType.includes("excel") || contentType.includes("spreadsheet")) return "Excel";

  const fileName = String(message.fileName || "");
  const extension = fileName.includes(".") ? fileName.split(".").pop() : "";
  return extension ? extension.toUpperCase() : "文件";
};
const fileSizeText = (size) => {
  const bytes = Number(size) || 0;
  if (!bytes) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
};
const resolveAttachmentType = (file) => {
  const contentType = String(file?.type || "").toLowerCase();
  if (contentType.startsWith("image/")) return MessageType.IMAGE;
  if (contentType.startsWith("video/")) return MessageType.VIDEO;
  return MessageType.FILE;
};
const selectAttachment = (event) => {
  const file = event.target.files?.[0];
  if (file) emit("attachment", file, resolveAttachmentType(file));
  event.target.value = "";
};

const clampComposerHeight = (height) =>
  Math.min(MAX_COMPOSER_HEIGHT, Math.max(MIN_COMPOSER_HEIGHT, height));

const startComposerResize = (event) => {
  event.preventDefault();
  resizeStartY.value = event.clientY;
  resizeStartHeight.value = composerHeight.value;
  isResizingComposer.value = true;
  event.currentTarget.setPointerCapture?.(event.pointerId);
};

const resizeComposer = (event) => {
  if (!isResizingComposer.value) return;
  composerHeight.value = clampComposerHeight(
    resizeStartHeight.value - (event.clientY - resizeStartY.value),
  );
};

const stopComposerResize = (event) => {
  if (!isResizingComposer.value) return;
  isResizingComposer.value = false;
  if (event.currentTarget.hasPointerCapture?.(event.pointerId)) {
    event.currentTarget.releasePointerCapture(event.pointerId);
  }
};

onMounted(() => {
  if (typeof ResizeObserver === "undefined") return;

  messageResizeObserver = new ResizeObserver(() => {
    if (!shouldStickToBottom.value) return;

    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(scrollToBottom);
    } else {
      scrollToBottom();
    }
  });

  observeMessageContent();
});

onBeforeUnmount(() => {
  messageResizeObserver?.disconnect();
  messageResizeObserver = null;
});

watch(
  () => props.conversation?.conversationId,
  async () => {
    shouldStickToBottom.value = true;
    await nextTick();
    observeMessageContent();
    scheduleScrollToBottom(true);
  },
);

watch(
  () => props.messages.length,
  () => scheduleScrollToBottom(),
);
</script>

<template>
  <section
    :class="[
      'chat-panel min-w-2xs flex-col overflow-hidden bg-[var(--im-primary-softer)] text-sm',
      mobile
        ? visible
          ? 'fixed inset-0 z-40 flex h-full w-full'
          : 'hidden'
        : 'relative flex h-full',
    ]"
  >
    <template v-if="conversation">
      <header
        class="flex min-h-16 flex-none items-center gap-3 border-b border-[var(--im-border)] bg-[var(--im-surface)] px-[22px] max-[767px]:px-4"
      >
        <button
          v-if="mobile"
          class="grid size-9 place-items-center rounded-lg bg-transparent text-[var(--im-text-secondary)] hover:bg-[var(--im-surface-muted)]"
          type="button"
          title="返回"
          @click="$emit('back')"
        >
          <ArrowLeft :size="21" />
        </button>
        <span
          :class="isGroup(conversation) ? 'bg-[var(--im-primary)]' : 'bg-[var(--im-primary-tint)]'"
          class="grid size-[42px] flex-none place-items-center overflow-hidden rounded-full text-sm font-bold text-white shadow-[var(--im-shadow-md)]"
        >
          <img
            v-if="conversation.avatar"
            class="size-full object-cover"
            :src="conversation.avatar"
            :alt="conversationDisplayName(conversation)"
          />
          <Users v-else-if="isGroup(conversation)" :size="18" />
          <span v-else>{{ conversationDisplayName(conversation).slice(0, 1) }}</span>
        </span>
        <div class="min-w-0 flex-1">
          <h2 class="truncate text-[var(--im-fs-15)] font-extrabold text-[var(--im-text-strong)]">
            {{ conversationDisplayName(conversation) }}
          </h2>
          <p class="mt-0.5 flex items-center gap-1.5 text-[var(--im-fs-11)] text-[var(--im-text-secondary)]">
            {{ isGroup(conversation) ? `${memberCount(conversation)} 位成员` : "私聊" }}
          </p>
        </div>
        <div class="flex items-center gap-1">
          <button
            v-if="isGroup(conversation)"
            class="flex min-h-9 items-center gap-1.5 rounded-lg border border-[var(--im-primary-border)] bg-[var(--im-primary-softer)] px-3 text-[var(--im-fs-11)] font-semibold text-[var(--im-primary-hover)] hover:bg-[var(--im-primary-soft)]"
            type="button"
            title="打开群聊 AI 助手"
            @click="$emit('assistant')"
          >
            <span class="grid size-5 place-items-center rounded-md bg-[var(--im-primary)] text-[var(--im-fs-9)] font-extrabold text-white">AI</span>
            <span class="max-[420px]:hidden">群聊助手</span>
          </button>
          <button
            v-if="isGroup(conversation)"
            class="flex min-h-9 items-center gap-1.5 rounded-lg border border-[var(--im-border)] bg-[var(--im-surface)] px-3 text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)] transition-colors hover:border-[var(--im-primary-border)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)] max-[420px]:px-2.5"
            type="button"
            title="分享群聊"
            :disabled="shareLoading"
            @click="$emit('share-group')"
          >
            <Share2 :size="15" />
            <span class="max-[420px]:hidden">{{ shareLoading ? "生成中" : "分享" }}</span>
          </button>
          <button
            class="grid size-9 place-items-center rounded-lg bg-transparent text-[var(--im-text-secondary)] hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)]"
            type="button"
            title="会话详情"
            @click="$emit('details')"
          >
            <Info v-if="!mobile" :size="19" />
            <MoreHorizontal v-else :size="20" />
          </button>
        </div>
      </header>

      <div
        ref="messageList"
        class="flex min-h-0 flex-1 flex-col items-stretch overflow-y-auto scroll-smooth bg-[linear-gradient(180deg,var(--im-primary-softer)_0%,var(--im-surface-muted)_100%)] px-[clamp(14px,2.2vw,28px)] py-[30px] max-[767px]:px-3"
        @scroll="handleMessageScroll"
      >
        <div ref="messageContent" class="flex min-h-full flex-col items-stretch">
          <button
            v-if="hasMore"
            class="mx-auto mb-5 flex min-h-8 items-center gap-1.5 rounded-full border border-[var(--im-border)] bg-[var(--im-surface)] px-3 text-[var(--im-fs-11)] text-[var(--im-text-secondary)] shadow-sm hover:bg-[var(--im-primary-softer)]"
            type="button"
            :disabled="loading"
            @click="$emit('load-older')"
          >
            <LoaderCircle v-if="loading" class="animate-spin" :size="15" />{{
              loading ? "加载中" : "查看更早消息"
            }}
          </button>
          <div
            v-if="loading && !messages.length"
            class="flex min-h-40 flex-col items-center justify-center gap-2 text-xs text-[var(--im-text-secondary)]"
          >
            <LoaderCircle class="animate-spin" :size="22" /><span>正在加载消息</span>
          </div>
          <div
            v-else-if="!messages.length"
            class="flex min-h-40 flex-col items-center justify-center gap-1 text-center text-sm text-[var(--im-text-secondary)]"
          >
            <span>还没有消息</span
            ><small class="text-xs text-[var(--im-text-faint)]">发送一条消息开始聊天</small>
          </div>

          <div class="flex min-h-0 flex-col gap-[30px]">
          <article
            v-for="message in messages"
            :key="messageKey(message)"
            :class="
              displayType(message) === 'system'
                ? 'justify-center'
                : isMine(message)
                  ? 'flex-row-reverse'
                  : 'flex-row'
            "
            class="mx-auto flex w-full max-w-195 items-start gap-3 min-[1500px]:max-w-210"
          >
            <template v-if="displayType(message) === 'system'"
              ><span class="rounded-full bg-[var(--im-surface-muted)] px-3 py-1 text-[var(--im-fs-11)] text-[var(--im-text-secondary)]">{{
                message.content
              }}</span></template
            >
            <template v-else>
              <span
                class="grid size-8.5 flex-none place-items-center overflow-hidden rounded-full text-xs font-bold text-white shadow-[var(--im-shadow-sm)]"
                :style="{ backgroundColor: isMine(message) ? 'var(--im-primary)' : 'var(--im-primary-tint)' }"
                >{{ senderName(message).slice(0, 1) }}</span
              >
              <div
                :class="isMine(message) ? 'items-end' : 'items-start'"
                class="flex max-w-[min(78%,560px)] flex-col gap-[7px] max-[767px]:max-w-[84%]"
              >
                <span
                  v-if="isGroup(conversation) && !isMine(message)"
                  class="text-xs text-[var(--im-text-muted)]"
                  >{{ senderLabel(message) }}</span
                >
                <div
                  :class="isMine(message) ? 'flex-row-reverse' : 'flex-row'"
                  class="flex items-end gap-2"
                >
                  <div class="flex min-w-0 flex-col gap-1">
                <component
                  :is="message.mediaUrl ? 'a' : 'div'"
                  v-if="displayType(message) === 'file'"
                  class="group flex w-full max-w-[min(320px,70vw)] items-center gap-3 rounded-xl bg-[var(--im-surface)] px-3.5 py-3 text-left shadow-[var(--im-shadow-md)] transition-colors"
                  :class="message.mediaUrl ? 'cursor-pointer hover:bg-[var(--im-primary-softer)]' : 'cursor-default'"
                  :href="message.mediaUrl || undefined"
                  :target="message.mediaUrl ? '_blank' : undefined"
                  :rel="message.mediaUrl ? 'noreferrer' : undefined"
                >
                  <span
                    class="grid size-11 flex-none place-items-center rounded-lg bg-[var(--im-primary-soft)] text-[var(--im-primary)]"
                  >
                    <FileText :size="24" />
                  </span>
                  <span class="grid min-w-0 flex-1 gap-1">
                    <strong class="truncate text-[var(--im-fs-13)] text-[var(--im-text-strong)]">{{
                      shortFileName(message.fileName || message.content)
                    }}</strong>
                    <small class="truncate text-xs text-[var(--im-text-muted)]">
                      {{ fileSizeText(message.fileSize) || "文件" }} · {{ fileTypeText(message) }}
                    </small>
                  </span>
                  <Download
                    v-if="message.mediaUrl"
                    class="flex-none text-[var(--im-primary-tint)] transition-colors group-hover:text-[var(--im-primary)]"
                    :size="18"
                  />
                </component>
                <div
                  v-else-if="displayType(message) === 'image' || displayType(message) === 'sticker'"
                  class="relative max-w-full overflow-hidden rounded-xl"
                >
                  <ElImage
                    v-if="message.mediaUrl"
                    class="block max-h-[280px] max-w-[min(280px,70vw)] rounded-xl"
                    :src="message.thumbUrl || message.mediaUrl"
                    :preview-src-list="[message.mediaUrl]"
                    :alt="
                      message.fileName ||
                      (displayType(message) === 'sticker' ? '表情包' : '聊天图片')
                    "
                    fit="contain"
                    preview-teleported
                  />
                  <div
                    v-else
                    class="grid size-32 place-items-center rounded-xl bg-[var(--im-primary-soft)] text-[var(--im-primary-tint)]"
                  >
                    <ImageIcon :size="30" />
                  </div>
                  <div
                    v-if="isUploadPending(message)"
                    class="absolute inset-0 grid place-items-center bg-slate-900/35 text-xs font-semibold text-white"
                  >
                    <span
                      class="flex items-center gap-1.5 rounded-full bg-slate-900/55 px-2.5 py-1.5"
                    >
                      <LoaderCircle class="animate-spin" :size="14" />{{
                        Number(message.uploadProgress) || 0
                      }}%
                    </span>
                  </div>
                </div>
                <div
                  v-else-if="displayType(message) === 'video'"
                  class="relative max-w-[min(280px,70vw)] overflow-hidden rounded-xl bg-[var(--im-surface)] shadow-[var(--im-shadow-md)]"
                >
                  <video
                    v-if="message.mediaUrl"
                    class="block h-auto max-h-[280px] max-w-[min(280px,70vw)] object-contain"
                    :src="message.mediaUrl"
                    controls
                    preload="metadata"
                  ></video>
                  <div v-else class="grid h-32 w-48 place-items-center text-[var(--im-primary-tint)]">
                    <Video :size="30" />
                  </div>
                  <span class="block px-3 py-2 text-xs text-[var(--im-text-secondary)]">{{
                    shortFileName(message.fileName)
                  }}</span>
                  <div
                    v-if="isUploadPending(message)"
                    class="absolute inset-0 grid place-items-center bg-slate-900/35 text-xs font-semibold text-white"
                  >
                    <span
                      class="flex items-center gap-1.5 rounded-full bg-slate-900/55 px-2.5 py-1.5"
                    >
                      <LoaderCircle class="animate-spin" :size="14" />{{
                        Number(message.uploadProgress) || 0
                      }}%
                    </span>
                  </div>
                </div>
                <div
                  v-else
                  :class="
                    isMine(message)
                      ? 'rounded-[14px_6px_14px_14px] bg-[var(--im-primary-soft)] text-[var(--im-text-strong)]'
                      : 'rounded-[6px_14px_14px_14px] bg-[var(--im-surface)] text-[var(--im-text)]'
                  "
                  class="w-fit max-w-full px-4 py-3 text-[var(--im-fs-13)] leading-[1.7] shadow-[var(--im-shadow-md)]"
                >
                  {{ message.content }}
                </div>
                <div class="flex items-center gap-1 text-[var(--im-fs-11)] text-[var(--im-text-faint)]">
                  <time>{{ messageTime(message) }}</time>
                  <small v-if="shouldShowMessageStatus(message)">{{
                    messageStatusText(message)
                  }}</small>
                </div>
                  </div>
                  <button
                    v-if="isMine(message) && (message.sendStatus === 'failed' || message.error)"
                    class="grid size-4 flex-none place-items-center rounded-full bg-[var(--im-danger)] text-[var(--im-fs-10)] font-bold text-white hover:bg-[var(--im-danger)]"
                    type="button"
                    aria-label="重试发送"
                    @click="$emit('retry-message', message)"
                  >!
                  </button>
                </div>
                <div
                  v-if="isMine(message) && isUploadPending(message)"
                  class="flex items-center gap-1 text-[var(--im-fs-11)] text-[var(--im-primary-tint)]"
                >
                  <button
                    v-if="message.uploadStage === 'uploading'"
                    class="hover:text-[var(--im-primary)]"
                    type="button"
                    title="暂停上传"
                    @click="$emit('pause-upload', message.clientMsgId)"
                  >
                    <Pause :size="13" />
                  </button>
                  <button
                    v-if="message.uploadStage === 'paused'"
                    class="hover:text-[var(--im-primary)]"
                    type="button"
                    title="继续上传"
                    @click="$emit('resume-upload', message.clientMsgId)"
                  >
                    <Play :size="13" />
                  </button>
                  <button
                    class="hover:text-[var(--im-danger)]"
                    type="button"
                    title="取消上传"
                    @click="$emit('cancel-upload', message.clientMsgId)"
                  >
                    <X :size="13" />
                  </button>
                </div>
              </div>
            </template>
          </article>
          </div>
        </div>
      </div>

      <footer
        class="flex flex-none flex-col gap-2 border-t border-[var(--im-border)] bg-[var(--im-primary-softer)] px-8 pb-4 pt-2.5 max-[767px]:px-3 max-[767px]:pb-3"
      >
        <div class="mx-auto w-full max-w-260">
          <div
            :class="isResizingComposer ? 'select-none' : ''"
            :style="{ height: `${composerHeight}px` }"
            class="relative flex w-full min-h-0 transition-none"
          >
            <button
              class="group absolute inset-x-0 top-0 z-10 h-2 cursor-ns-resize touch-none bg-transparent"
              type="button"
              aria-label="调整输入框高度"
              @pointerdown="startComposerResize"
              @pointermove="resizeComposer"
              @pointerup="stopComposerResize"
              @pointercancel="stopComposerResize"
            >
              <span
                class="mx-auto block h-1 w-10 rounded-full bg-[var(--im-primary-border)] transition-colors group-hover:bg-[var(--im-primary-border)] group-active:bg-[var(--im-primary-tint)]"
              ></span>
            </button>
            <div
              class="flex h-full w-full min-h-0 min-w-0 flex-col overflow-hidden rounded-2xl border border-[var(--im-primary-border)] bg-[var(--im-surface)] shadow-[var(--im-shadow-md)]"
            >
              <div
                class="flex h-11 flex-none items-center gap-1 border-b border-[var(--im-border-soft)] px-3 pt-1 text-[var(--im-text-secondary)]"
              >
                <button
                  class="grid size-7 place-items-center rounded-lg bg-transparent transition-colors hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)] disabled:cursor-not-allowed disabled:opacity-40"
                  type="button"
                  title="发送文件、图片或视频"
                  @click="fileInput?.click()"
                >
                  <Folder :size="15" />
                </button>
              </div>
              <textarea
                v-model="draft"
                rows="1"
                placeholder="Enter 发送 · Shift + Enter 换行"
                class="min-h-0 min-w-0 text-sm placeholder:text-sm flex-1 resize-none overflow-y-auto border-0 bg-transparent px-4 py-3 leading-6 outline-0 placeholder:text-[var(--im-text-faint)]"
                @keydown.enter="handleEnter"
              >
              </textarea>
              <div
                class="flex h-14 flex-none items-center justify-end border-t border-[var(--im-primary-soft)] px-3"
              >
                <button
                  class="flex h-10 min-w-23 items-center justify-center gap-2 rounded-[10px] px-4 bg-linear-to-br from-[var(--im-primary)] to-[var(--im-primary-hover)] text-[var(--im-fs-13)] font-bold text-white shadow-[var(--im-shadow-md)] transition-[box-shadow,transform] duration-150 hover:-translate-y-0.5 hover:shadow-[var(--im-shadow-md)] disabled:cursor-not-allowed disabled:opacity-50"
                  type="button"
                  title="发送消息"
                  :disabled="!draft.trim() || connection !== 'connected'"
                  @click="submit"
                >
                  <span>发送</span>
                  <Send :size="16" />
                </button>
              </div>
            </div>
          </div>
        </div>
        <input ref="fileInput" type="file" hidden @change="selectAttachment" />
      </footer>
    </template>

    <div
      v-else
      class="flex h-full flex-col items-center justify-center gap-3 bg-[var(--im-primary-softer)] text-center text-[var(--im-text-secondary)]"
    >
      <span class="grid size-14 place-items-center rounded-2xl bg-[var(--im-primary-soft)] text-[var(--im-primary)]">
        <Send :size="28" />
      </span>
      <h2 class="m-0 text-base font-bold text-[var(--im-text-secondary)]">选择一个会话</h2>
      <p class="m-0 text-xs">从左侧会话列表中选择联系人或群聊</p>
    </div>
  </section>
</template>
