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
});

const emit = defineEmits([
  "back",
  "details",
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
  scheduleScrollToBottom(true);
};

const handleEnter = (event) => {
  if (event.isComposing || event.shiftKey) return;
  event.preventDefault();
  submit();
};

const isMine = (message) => message.senderId === props.currentUser.userId;
const shouldShowMessageStatus = (message) => {
  if (!isMine(message)) return false;
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
const isRoom = (conversation) => Number(conversation?.convType) === ConversationType.ROOM;
const conversationDisplayName = (conversation) => conversation?.displayName || "未命名会话";
const memberCount = (conversation) => conversation?.room?.memberCount || 0;
const displayType = (message) => messageViewType(message.cType);
const senderName = (message) => message.senderUsername || message.senderId || "成员";
const messageTime = (message) => {
  if (!message?.sendTime) return "";
  const date = new Date(Number(message.sendTime));
  const now = new Date();
  if (date.toDateString() === now.toDateString()) {
    return date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false });
  }
  return `${date.getMonth() + 1}/${date.getDate()}`;
};
const messageKey = (message) =>
  message.messageId || message.clientMsgId || `${message.conversationId}:${message.seq}`;
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
      'chat-panel min-w-2xs flex-col overflow-hidden bg-[#faf9ff] text-sm',
      mobile
        ? visible
          ? 'fixed inset-0 z-40 flex h-full w-full'
          : 'hidden'
        : 'relative flex h-full',
    ]"
  >
    <template v-if="conversation">
      <header
        class="flex min-h-16 flex-none items-center gap-3 border-b border-[#edf1f7] bg-white px-[22px] max-[767px]:px-4"
      >
        <button
          v-if="mobile"
          class="grid size-9 place-items-center rounded-lg bg-transparent text-[#667085] hover:bg-[#f3f6fb]"
          type="button"
          title="返回"
          @click="$emit('back')"
        >
          <ArrowLeft :size="21" />
        </button>
        <span
          :class="isRoom(conversation) ? 'bg-[#6d4aff]' : 'bg-[#8b72d6]'"
          class="grid size-[42px] flex-none place-items-center overflow-hidden rounded-full text-sm font-bold text-white shadow-[0_10px_22px_rgba(91,53,245,0.2)]"
        >
          <img
            v-if="conversation.avatar"
            class="size-full object-cover"
            :src="conversation.avatar"
            :alt="conversationDisplayName(conversation)"
          />
          <Users v-else-if="isRoom(conversation)" :size="18" />
          <span v-else>{{ conversationDisplayName(conversation).slice(0, 1) }}</span>
        </span>
        <div class="min-w-0 flex-1">
          <h2 class="truncate text-[15px] font-extrabold text-[#111827]">
            {{ conversationDisplayName(conversation) }}
          </h2>
          <p class="mt-0.5 flex items-center gap-1.5 text-[11px] text-[#8a98ac]">
            {{ isRoom(conversation) ? `${memberCount(conversation)} 位成员` : "私聊" }}
          </p>
        </div>
        <div class="flex items-center gap-1">
          <button
            class="grid size-9 place-items-center rounded-lg bg-transparent text-[#667085] hover:bg-[#f3efff] hover:text-[#5b35f5]"
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
        class="flex min-h-0 flex-1 flex-col items-stretch overflow-y-auto scroll-smooth bg-[linear-gradient(180deg,#fbfaff_0%,#f7f8fc_100%)] px-[clamp(14px,2.2vw,28px)] pb-3 pt-[22px] max-[767px]:px-3"
        @scroll="handleMessageScroll"
      >
        <div ref="messageContent" class="flex min-h-full flex-col items-stretch">
          <button
            v-if="hasMore"
            class="mx-auto mb-5 flex min-h-8 items-center gap-1.5 rounded-full border border-[#e4e9f2] bg-white px-3 text-[11px] text-[#7d8999] shadow-sm hover:bg-[#f5f8ff]"
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
            class="flex min-h-40 flex-col items-center justify-center gap-2 text-xs text-[#8a98ac]"
          >
            <LoaderCircle class="animate-spin" :size="22" /><span>正在加载消息</span>
          </div>
          <div
            v-else-if="!messages.length"
            class="flex min-h-40 flex-col items-center justify-center gap-1 text-center text-sm text-[#8a98ac]"
          >
            <span>还没有消息</span
            ><small class="text-xs text-[#a8b1be]">发送一条消息开始聊天</small>
          </div>

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
            class="mx-auto mb-4.5 flex w-full max-w-195 items-start gap-3 min-[1500px]:max-w-210"
          >
            <template v-if="displayType(message) === 'system'"
              ><span class="rounded-full bg-[#eef2f8] px-3 py-1 text-[11px] text-[#8a98ac]">{{
                message.content
              }}</span></template
            >
            <template v-else>
              <span
                class="grid size-8.5 flex-none place-items-center overflow-hidden rounded-full text-xs font-bold text-white shadow-[0_6px_16px_rgba(37,52,86,0.08)]"
                :style="{ backgroundColor: isMine(message) ? '#5b35f5' : '#8b72d6' }"
                >{{ senderName(message).slice(0, 1) }}</span
              >
              <div
                :class="isMine(message) ? 'items-end' : 'items-start'"
                class="flex max-w-[min(78%,560px)] flex-col gap-[7px] max-[767px]:max-w-[84%]"
              >
                <span
                  v-if="isRoom(conversation) && !isMine(message)"
                  class="text-xs text-[#8b98aa]"
                  >{{ senderName(message) }}</span
                >
                <component
                  :is="message.mediaUrl ? 'a' : 'div'"
                  v-if="displayType(message) === 'file'"
                  class="group flex w-full max-w-[min(320px,70vw)] items-center gap-3 rounded-xl bg-white px-3.5 py-3 text-left shadow-[0_10px_28px_rgba(91,53,245,0.08)] transition-colors"
                  :class="message.mediaUrl ? 'cursor-pointer hover:bg-[#f8f6ff]' : 'cursor-default'"
                  :href="message.mediaUrl || undefined"
                  :target="message.mediaUrl ? '_blank' : undefined"
                  :rel="message.mediaUrl ? 'noreferrer' : undefined"
                >
                  <span
                    class="grid size-11 flex-none place-items-center rounded-lg bg-[#eee9ff] text-[#5b35f5]"
                  >
                    <FileText :size="24" />
                  </span>
                  <span class="grid min-w-0 flex-1 gap-1">
                    <strong class="truncate text-[13px] text-[#1f2937]">{{
                      shortFileName(message.fileName || message.content)
                    }}</strong>
                    <small class="truncate text-xs text-[#8a98aa]">
                      {{ fileSizeText(message.fileSize) || "文件" }} · {{ fileTypeText(message) }}
                    </small>
                  </span>
                  <Download
                    v-if="message.mediaUrl"
                    class="flex-none text-[#8b72d6] transition-colors group-hover:text-[#5b35f5]"
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
                    class="grid size-32 place-items-center rounded-xl bg-[#eee9ff] text-[#8b72d6]"
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
                  class="relative max-w-[min(280px,70vw)] overflow-hidden rounded-xl bg-white shadow-[0_10px_28px_rgba(91,53,245,0.08)]"
                >
                  <video
                    v-if="message.mediaUrl"
                    class="block h-auto max-h-[280px] max-w-[min(280px,70vw)] object-contain"
                    :src="message.mediaUrl"
                    controls
                    preload="metadata"
                  ></video>
                  <div v-else class="grid h-32 w-48 place-items-center text-[#8b72d6]">
                    <Video :size="30" />
                  </div>
                  <span class="block px-3 py-2 text-xs text-[#667085]">{{
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
                      ? 'rounded-[14px_6px_14px_14px] bg-[#e4dcff] text-[#3c2c72]'
                      : 'rounded-[6px_14px_14px_14px] bg-white text-[#293548]'
                  "
                  class="w-fit max-w-full px-4 py-3 text-[13px] leading-[1.7] shadow-[0_10px_28px_rgba(91,53,245,0.08)]"
                >
                  {{ message.content }}
                </div>
                <div class="flex items-center gap-1 text-[11px] text-[#9aa6b7]">
                  <time>{{ messageTime(message) }}</time>
                  <small v-if="shouldShowMessageStatus(message)">{{
                    messageStatusText(message)
                  }}</small>
                  <button
                    v-if="isMine(message) && message.error"
                    class="grid size-4 place-items-center rounded-full bg-[#d94f61] text-[10px] font-bold text-white hover:bg-[#bf394e]"
                    type="button"
                    title="重新发送"
                    @click="$emit('retry-message', message)"
                  >
                    !
                  </button>
                </div>
                <div
                  v-if="isMine(message) && isUploadPending(message)"
                  class="flex items-center gap-1 text-[11px] text-[#8b72d6]"
                >
                  <button
                    v-if="message.uploadStage === 'uploading'"
                    class="hover:text-[#5b35f5]"
                    type="button"
                    title="暂停上传"
                    @click="$emit('pause-upload', message.clientMsgId)"
                  >
                    <Pause :size="13" />
                  </button>
                  <button
                    v-if="message.uploadStage === 'paused'"
                    class="hover:text-[#5b35f5]"
                    type="button"
                    title="继续上传"
                    @click="$emit('resume-upload', message.clientMsgId)"
                  >
                    <Play :size="13" />
                  </button>
                  <button
                    class="hover:text-[#dc6570]"
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

      <footer
        class="flex flex-none flex-col gap-2 border-t border-[#ebe7f5] bg-[#fbfaff] px-8 pb-4 pt-2.5 max-[767px]:px-3 max-[767px]:pb-3"
      >
        <div
          v-if="connection !== 'connected'"
          class="mx-auto w-full max-w-260 text-[11px] text-[#c08526]"
        >
          <i class="mr-1 inline-block size-1.5 rounded-full bg-[#d29a42]"></i
          >{{
            connection === "connecting"
              ? "正在连接实时消息服务..."
              : "实时连接已断开，暂时无法发送消息"
          }}
        </div>
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
                class="mx-auto block h-1 w-10 rounded-full bg-[#e5defa] transition-colors group-hover:bg-[#cfc5ff] group-active:bg-[#a493ff]"
              ></span>
            </button>
            <div
              class="flex h-full w-full min-h-0 min-w-0 flex-col overflow-hidden rounded-2xl border border-[#e6e0f4] bg-white shadow-[0_8px_26px_rgba(91,53,245,0.06)]"
            >
              <div
                class="flex h-11 flex-none items-center gap-1 border-b border-[#f0ecf8] px-3 pt-1 text-[#77718c]"
              >
                <button
                  class="grid size-7 place-items-center rounded-lg bg-transparent transition-colors hover:bg-[#f3efff] hover:text-[#5b35f5] disabled:cursor-not-allowed disabled:opacity-40"
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
                class="min-h-0 min-w-0 text-sm placeholder:text-sm flex-1 resize-none overflow-y-auto border-0 bg-transparent px-4 py-3 leading-6 outline-0 placeholder:text-[#b0aabd]"
                @keydown.enter="handleEnter"
              >
              </textarea>
              <div
                class="flex h-14 flex-none items-center justify-end border-t border-[#f4f1f9] px-3"
              >
                <button
                  class="flex h-10 min-w-23 items-center justify-center gap-2 rounded-[10px] px-4 bg-linear-to-br from-[#7355ff] to-[#4b2bd7] text-[13px] font-bold text-white shadow-[0_10px_20px_rgba(91,53,245,0.2)] transition-[box-shadow,transform] duration-150 hover:-translate-y-0.5 hover:shadow-[0_14px_26px_rgba(91,53,245,0.28)] disabled:cursor-not-allowed disabled:opacity-50"
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
      class="flex h-full flex-col items-center justify-center gap-3 bg-[#faf9ff] text-center text-[#8a98ac]"
    >
      <span class="grid size-14 place-items-center rounded-2xl bg-[#eee9ff] text-[#5b35f5]">
        <Send :size="28" />
      </span>
      <h2 class="m-0 text-base font-bold text-[#475467]">选择一个会话</h2>
      <p class="m-0 text-xs">从左侧会话列表中选择联系人或房间</p>
    </div>
  </section>
</template>
