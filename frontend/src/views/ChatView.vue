<script setup>
import { MessageCircle, Plus, UserRound, Users, X } from "@lucide/vue";
import { storeToRefs } from "pinia";
import { onMounted, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { findUserByPhoneOrUsername } from "../services/http/api.js";
import AppRail from "../components/chat/AppRail.vue";
import ChatPanel from "../components/chat/ChatPanel.vue";
import ContactsPanel from "../components/chat/ContactsPanel.vue";
import ConversationDetails from "../components/chat/ConversationDetails.vue";
import ConversationList from "../components/chat/ConversationList.vue";
import ProfilePanel from "../components/chat/ProfilePanel.vue";
import ConfirmDialog from "../components/common/ConfirmDialog.vue";
import { useResponsive } from "../composables/useResponsive.js";
import { useChatStore } from "../modules/chat/chatStore.js";
import { messageTips } from "../utils/messageTips.js";

const router = useRouter();
const { isMobile } = useResponsive();
const chatStore = useChatStore();
const { activeConversation, activeMessages } = storeToRefs(chatStore);
const {
  state,
  loadWorkspace,
  updateProfile,
  selectConversation,
  loadOlderMessages,
  sendMessage,
  sendAttachment,
  retryMessage,
  sendReadAck,
  clearConversation,
  handleFriendRequest,
  addFriendRequest,
  startConversation,
  createRoom,
  joinRoom,
  retryConnection,
  pauseUpload,
  resumeUpload,
  cancelUpload,
  logout,
} = chatStore;

const activeSection = ref("conversations");
const mobileChatOpen = ref(false);
const detailsOpen = ref(false);
const roomDialogOpen = ref(false);
const roomMode = ref("create");
const roomSubmitting = ref(false);
const clearConfirmOpen = ref(false);

const loadError = ref("");
const roomForm = reactive({ roomName: "", description: "", avatar: "", inviteCode: "" });

const initialize = async () => {
  loadError.value = "";
  try {
    await loadWorkspace();
  } catch (error) {
    loadError.value = error.message;
    messageTips.error({ title: "数据加载失败", message: error.message });
  }
};

const chooseSection = (section) => {
  activeSection.value = section;
  mobileChatOpen.value = false;
  detailsOpen.value = false;
};

const chooseConversation = async (id) => {
  mobileChatOpen.value = true;
  try {
    await selectConversation(id);
    detailsOpen.value = false;
    sendReadAck();
  } catch (error) {
    messageTips.error({ title: "消息加载失败", message: error.message });
  }
};

const handleSend = (text) => {
  try {
    sendMessage(text);
  } catch (error) {
    messageTips.warning(error.message);
  }
};

const handleAttachment = async (file, cType) => {
  try {
    await sendAttachment(file, cType);
  } catch (error) {
    messageTips.error({ title: "发送失败", message: error.message });
  }
};

const handleRetryMessage = async (message) => {
  try {
    await retryMessage(message);
  } catch (error) {
    messageTips.error({ title: "重试失败", message: error.message });
  }
};

const handleContact = async (contact) => {
  try {
    const conversation = await startConversation(contact);
    activeSection.value = "conversations";
    mobileChatOpen.value = true;
    return conversation;
  } catch (error) {
    messageTips.info(error.message);
  }
};

const handleRequest = async (request, accepted) => {
  try {
    await handleFriendRequest(request, accepted);
    messageTips.success(accepted ? "已同意好友申请" : "已拒绝好友申请");
  } catch (error) {
    messageTips.error(error.message);
  }
};

const handleAddFriend = async (form) => {
  try {
    const user = await findUserByPhoneOrUsername(form.keyword);
    if (!user?.userId) throw new Error("未找到该用户");
    await addFriendRequest({
      peerUserId: user.userId,
      message: form.message,
    });
    messageTips.success("好友申请已发送");
    return true;
  } catch (error) {
    messageTips.error(error.message);
    return false;
  }
};

const submitRoom = async () => {
  roomSubmitting.value = true;
  try {
    if (roomMode.value === "create") {
      if (!roomForm.roomName.trim()) throw new Error("请输入房间名称。");
      await createRoom(roomForm);
      messageTips.success("房间创建成功");
    } else {
      if (!roomForm.inviteCode.trim()) throw new Error("请输入邀请码。");
      await joinRoom(roomForm.inviteCode);
      messageTips.success("已加入房间");
    }
    roomDialogOpen.value = false;
  } catch (error) {
    messageTips.error(error.message);
  } finally {
    roomSubmitting.value = false;
  }
};

const confirmClear = async () => {
  try {
    await clearConversation(activeConversation.value.conversationId);
    clearConfirmOpen.value = false;
    detailsOpen.value = false;
    messageTips.success("本地聊天记录已清空");
  } catch (error) {
    messageTips.error({ title: "清空失败", message: error.message });
  }
};

const handleLogout = async () => {
  await logout();
  messageTips.info("已退出登录");
  router.replace("/login");
};

const openDetails = () => {
  detailsOpen.value = true;
};

const closeDetails = () => {
  detailsOpen.value = false;
};

watch(isMobile, (mobile) => {
  if (mobile) {
    detailsOpen.value = false;
  }
});

onMounted(initialize);
</script>

<template>
  <main
    class="flex h-dvh min-h-0 w-full gap-3 overflow-hidden bg-[radial-gradient(circle_at_top_left,rgba(126,93,255,0.08),transparent_34%),#f3f1fb] p-3 text-sm max-[767px]:block max-[767px]:p-0">
    <AppRail :active-section="activeSection" :current-user="state.currentUser" :connection="state.connection"
      @select="chooseSection" @retry="retryConnection" @logout="handleLogout" />

    <section
      class="im-workspace relative min-h-0 min-w-0 w-full h-full flex-1 overflow-hidden rounded-2xl border border-[#e5e0f3] bg-white shadow-[0_18px_60px_rgba(58,43,110,0.1)]">
      <aside :class="[
        'directory-shell relative min-w-0 overflow-hidden border-r border-[#ebe9f7] bg-[#fdfcff]',
        { 'mobile-hidden': mobileChatOpen },
      ]">
        <ConversationList v-if="activeSection === 'conversations'" :conversations="state.conversations"
          :active-id="state.activeConversationId" :loading="state.loading" @select="chooseConversation"
          @create-room="roomDialogOpen = true" />
        <ContactsPanel v-else-if="activeSection === 'contacts'" :contacts="state.contacts"
          :requests="state.friendRequests" @open-chat="handleContact" @handle-request="handleRequest"
          :on-add-friend="handleAddFriend" />
        <ProfilePanel v-else :current-user="state.currentUser" :save-profile="updateProfile" @logout="handleLogout" />
        <div v-if="loadError"
          class="absolute inset-x-3 top-3 z-10 grid gap-1 rounded-xl border border-[#f2cdd5] bg-[#fff5f7] p-3 text-[11px] text-[#c34d65]">
          <strong>暂时无法同步数据</strong><span>{{ loadError }}</span><button
            class="w-fit bg-transparent font-semibold text-[#5b35f5]" type="button" @click="initialize">
            重新加载
          </button>
        </div>
      </aside>

      <div class="conversation-stage">
        <ChatPanel :visible="!isMobile || mobileChatOpen" :conversation="activeConversation" :messages="activeMessages"
          :current-user="state.currentUser" :connection="state.connection" :loading="state.historyLoading"
          :has-more="state.historyHasMore[state.activeConversationId]" :mobile="isMobile" @back="mobileChatOpen = false"
          @details="openDetails" @send="handleSend" @attachment="handleAttachment" @retry-message="handleRetryMessage"
          @pause-upload="pauseUpload" @resume-upload="resumeUpload" @cancel-upload="cancelUpload"
          @load-older="loadOlderMessages" />

        <div v-if="detailsOpen && isMobile" class="fixed inset-0 z-44 bg-slate-900/30" @click="closeDetails"></div>
        <Transition name="details-panel">
          <ConversationDetails v-show="detailsOpen" :conversation="activeConversation" @close="closeDetails"
            @clear="clearConfirmOpen = true" />
        </Transition>
      </div>
    </section>

    <nav v-if="!mobileChatOpen"
      class="fixed bottom-0 left-0 right-0 z-30 flex h-16 items-center justify-around border-t border-[#ebe9f7] bg-white/95 px-5 shadow-[0_-8px_24px_rgba(54,42,116,0.08)] backdrop-blur min-[768px]:hidden"
      aria-label="移动端导航">
      <button :class="activeSection === 'conversations' ? 'text-[#5b35f5]' : 'text-[#9893a6]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[10px]" type="button"
        @click="chooseSection('conversations')">
        <MessageCircle :size="21" /><span>消息</span>
      </button>
      <button :class="activeSection === 'contacts' ? 'text-[#5b35f5]' : 'text-[#9893a6]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[10px]" type="button"
        @click="chooseSection('contacts')">
        <Users :size="21" /><span>联系人</span>
      </button>
      <button
        class="grid size-11 -translate-y-3 place-items-center rounded-full bg-gradient-to-br from-[#6d4aff] to-[#4a29e5] text-white shadow-[0_12px_24px_rgba(91,53,245,0.28)]"
        type="button" title="新建房间" @click="roomDialogOpen = true">
        <Plus :size="22" />
      </button>
      <button :class="activeSection === 'profile' ? 'text-[#5b35f5]' : 'text-[#9893a6]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[10px]" type="button"
        @click="chooseSection('profile')">
        <UserRound :size="21" /><span>我的</span>
      </button>
    </nav>

    <teleport to="body">
      <div v-if="roomDialogOpen" class="fixed inset-0 z-70 grid place-items-center bg-slate-900/45 px-4.5"
        @click.self="roomDialogOpen = false">
        <section
          class="w-full max-w-110 rounded-2xl border border-[#e9e5f2] bg-white p-5.5 shadow-[0_24px_70px_rgba(63,44,139,0.18)]">
          <header class="flex items-start justify-between">
            <div>
              <h2 class="m-0 text-[19px] font-bold text-[#17122c]">房间</h2>
              <p class="mt-1 text-[11px] text-[#8d889d]">创建新房间或使用邀请码加入</p>
            </div>
            <button
              class="grid size-9 place-items-center rounded-lg bg-transparent text-[#9893a6] transition-colors hover:bg-[#f3f0ff] hover:text-[#5b35f5]"
              type="button" title="关闭" @click="roomDialogOpen = false">
              <X :size="20" />
            </button>
          </header>
          <div class="mt-5.5 grid grid-cols-2 border-b border-[#e9e5f2]">
            <button :class="roomMode === 'create'
                ? 'border-[#5b35f5] text-[#5b35f5] font-semibold'
                : 'border-transparent text-[#8d889d]'
              " class="min-h-10 border-b-2 bg-transparent text-xs" type="button" @click="roomMode = 'create'">
              创建房间</button><button :class="roomMode === 'join'
                  ? 'border-[#5b35f5] text-[#5b35f5] font-semibold'
                  : 'border-transparent text-[#8d889d]'
                " class="min-h-10 border-b-2 bg-transparent text-xs" type="button" @click="roomMode = 'join'">
              邀请码加入
            </button>
          </div>
          <form class="mt-5 grid gap-4" @submit.prevent="submitRoom">
            <template v-if="roomMode === 'create'">
              <label class="grid gap-1.5"><span class="text-[11px] font-semibold text-[#514c61]">房间名称</span><input
                  v-model.trim="roomForm.roomName" maxlength="32"
                  class="min-h-10.5 rounded-lg placeholder:text-[#b0aabd] placeholder:text-[13px] border border-[#e9e5f2] px-3 text-xs outline-0 focus:border-[#a493ff] focus:ring-4 focus:ring-[rgba(91,53,245,0.08)]" /></label>
              <label class="grid gap-1.5"><span class="text-[11px] font-semibold text-[#514c61]">房间说明</span>
                <textarea v-model.trim="roomForm.description" rows="3" maxlength="120" placeholder="简要说明房间用途"
                  class="resize-y rounded-lg border border-[#e9e5f2] px-3 py-2.5 placeholder:text-[#b0aabd] placeholder:text-[13px] outline-0 focus:border-[#a493ff] focus:ring-4 focus:ring-[rgba(91,53,245,0.08)]">
                </textarea>
              </label>
            </template>
            <label v-else class="grid gap-1.5"><span class="text-[11px] font-semibold text-[#514c61]">邀请码</span><input
                v-model.trim="roomForm.inviteCode" placeholder="请输入房间邀请码"
                class="min-h-10.5 rounded-lg border border-[#e9e5f2] px-3 text-xs placeholder:text-xs outline-0 focus:border-[#a493ff] focus:ring-4 focus:ring-[rgba(91,53,245,0.08)]" /></label>
            <div class="mt-1 flex justify-end gap-2">
              <button
                class="min-h-10.5 rounded-lg border border-[#e9e5f2] bg-white px-4 text-xs text-[#8d889d] hover:bg-[#faf9ff]"
                type="button" @click="roomDialogOpen = false">
                取消</button><button
                class="min-h-10.5 rounded-lg bg-[#5b35f5] px-4 text-xs font-semibold text-white transition-colors hover:bg-[#4724d8] disabled:cursor-not-allowed disabled:opacity-50"
                type="submit" :disabled="roomSubmitting">
                {{ roomSubmitting ? "处理中..." : roomMode === "create" ? "创建" : "加入" }}
              </button>
            </div>
          </form>
        </section>
      </div>
    </teleport>

    <ConfirmDialog v-model:open="clearConfirmOpen" title="清空聊天记录" description="仅清除当前浏览器中已加载的消息，不会删除服务端历史记录。"
      confirm-label="确认清空" @confirm="confirmClear" />
  </main>
</template>
