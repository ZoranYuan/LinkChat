<script setup>
import { MessageCircle, Plus, UserRound, Users, X } from "@lucide/vue";
import { storeToRefs } from "pinia";
import { onMounted, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { findUserByPhoneOrUsername, getRoomInviteCode } from "../services/http/api.js";
import AppRail from "../components/chat/AppRail.vue";
import ChatPanel from "../components/chat/ChatPanel.vue";
import RoomAssistantPanel from "../components/chat/RoomAssistantPanel.vue";
import ContactsPanel from "../components/chat/ContactsPanel.vue";
import ConversationDetails from "../components/chat/ConversationDetails.vue";
import ConversationList from "../components/chat/ConversationList.vue";
import ProfilePanel from "../components/chat/ProfilePanel.vue";
import ConfirmDialog from "../components/common/ConfirmDialog.vue";
import { ConversationType } from "../constants/conversation.js";
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
  sendMessageRead,
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
const roomSidePanel = ref("");
const roomDialogOpen = ref(false);
const roomMode = ref("create");
const roomSubmitting = ref(false);
const clearConfirmOpen = ref(false);
const inviteDialogOpen = ref(false);
const inviteLoading = ref(false);
const inviteCode = ref("");
const inviteRoomName = ref("");

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
  roomSidePanel.value = "";
};

const chooseConversation = async (id) => {
  mobileChatOpen.value = true;
  roomSidePanel.value = "";
  try {
    await selectConversation(id);
    roomSidePanel.value = "";
    sendMessageRead();
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

const initializeRoomConversation = async (room) => {
  const conversation = state.conversations.find(
    (item) =>
      item.conversationId === room.conversationId ||
      (Number(item.convType) === ConversationType.ROOM && item.targetId === room.roomId),
  );
  if (!conversation) throw new Error("房间已处理，但未找到对应会话，请重新加载会话列表。");

  roomDialogOpen.value = false;
  activeSection.value = "conversations";
  mobileChatOpen.value = true;
  roomSidePanel.value = "";
  await selectConversation(conversation.conversationId);
};

const submitRoom = async () => {
  roomSubmitting.value = true;
  try {
    if (roomMode.value === "create") {
      if (!roomForm.roomName.trim()) throw new Error("请输入房间名称。");
      const room = await createRoom(roomForm);
      await initializeRoomConversation(room);
      messageTips.success("房间创建成功");
    } else {
      if (!roomForm.inviteCode.trim()) throw new Error("请输入邀请码。");
      const room = await joinRoom(roomForm.inviteCode);
      await initializeRoomConversation(room);
      messageTips.success("已加入房间");
    }
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
    roomSidePanel.value = "";
    messageTips.success("本地聊天记录已清空");
  } catch (error) {
    messageTips.error({ title: "清空失败", message: error.message });
  }
};

const handleLogout = async () => {
  try {
    await logout();
    messageTips.info("已退出登录");
  } catch (error) {
    messageTips.warning(`退出请求失败：${error.message}`);
  } finally {
    await router.replace("/login");
  }
};

const openDetails = () => {
  roomSidePanel.value = "details";
};

const openRoomAssistant = () => {
  roomSidePanel.value = "assistant";
};

const closeRoomSidePanel = () => {
  roomSidePanel.value = "";
};

const createRoomInvite = async () => {
  if (inviteLoading.value) return;
  const conversation = activeConversation.value;
  const roomId = conversation?.room?.roomId || conversation?.targetId;
  if (Number(conversation?.convType) !== ConversationType.ROOM || !roomId) return;

  inviteLoading.value = true;
  try {
    inviteCode.value = await getRoomInviteCode(roomId);
    inviteRoomName.value = conversation.displayName || "房间";
    inviteDialogOpen.value = true;
  } catch (error) {
    messageTips.error({ title: "邀请码生成失败", message: error.message });
  } finally {
    inviteLoading.value = false;
  }
};

const copyRoomInviteCode = async () => {
  try {
    await navigator.clipboard.writeText(inviteCode.value);
    messageTips.success("邀请码已复制");
  } catch {
    messageTips.error("复制失败，请手动复制邀请码");
  }
};

watch(isMobile, (mobile) => {
  if (mobile && roomSidePanel.value === "details") closeRoomSidePanel();
});

onMounted(initialize);
</script>

<template>
  <main
    class="flex h-dvh min-h-0 w-full gap-3 overflow-hidden bg-[radial-gradient(circle_at_top_left,rgba(126,93,255,0.08),transparent_34%),#f3f1fb] p-3 text-sm max-[767px]:block max-[767px]:p-0">
    <AppRail :active-section="activeSection" :current-user="state.currentUser"
      @select="chooseSection" @logout="handleLogout" />

    <section
      class="im-workspace relative min-h-0 min-w-0 w-full h-full flex-1 overflow-hidden rounded-2xl border border-[#e5e0f3] bg-white shadow-[0_18px_60px_rgba(58,43,110,0.1)]">
      <aside :class="[
        'directory-shell relative min-w-0 overflow-hidden border-r border-[#ebe9f7] bg-[#fdfcff]',
        { 'mobile-hidden': mobileChatOpen },
      ]">
        <ConversationList v-if="activeSection === 'conversations'" :conversations="state.conversations"
          :active-id="state.activeConversationId" :loading="state.loading" :connection="state.connection"
          @select="chooseConversation" @retry="retryConnection" @create-room="roomDialogOpen = true" />
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
          :share-loading="inviteLoading"
          :current-user="state.currentUser" :connection="state.connection" :loading="state.historyLoading"
          :has-more="state.historyHasMore[state.activeConversationId]" :mobile="isMobile" @back="mobileChatOpen = false"
          @details="openDetails" @share-room="createRoomInvite" @send="handleSend" @attachment="handleAttachment" @retry-message="handleRetryMessage"
          @assistant="openRoomAssistant"
          @pause-upload="pauseUpload" @resume-upload="resumeUpload" @cancel-upload="cancelUpload"
          @load-older="loadOlderMessages" />

        <div v-if="roomSidePanel && isMobile" class="fixed inset-0 z-44 bg-slate-900/30" @click="closeRoomSidePanel"></div>
        <Transition name="room-side-panel">
          <div v-if="roomSidePanel" class="room-side-panel-shell">
            <Transition name="room-side-panel-content" mode="out-in">
              <ConversationDetails
                v-if="roomSidePanel === 'details'"
                key="room-details"
                :conversation="activeConversation"
                @close="closeRoomSidePanel"
                @clear="clearConfirmOpen = true"
              />
              <RoomAssistantPanel
                v-else-if="roomSidePanel === 'assistant' && Number(activeConversation?.convType) === ConversationType.ROOM"
                key="room-assistant"
                :conversation="activeConversation"
                @close="closeRoomSidePanel"
              />
            </Transition>
          </div>
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
      <div
        v-if="inviteDialogOpen"
        class="fixed inset-0 z-[80] grid place-items-center bg-slate-900/35 p-4 backdrop-blur-[2px]"
        @click.self="inviteDialogOpen = false"
      >
        <section
          class="w-full max-w-[380px] rounded-2xl border border-[#ebe8f3] bg-white p-5 shadow-[0_24px_70px_rgba(32,24,64,0.22)]"
          role="dialog"
          aria-modal="true"
          aria-labelledby="room-invite-title"
        >
          <div class="flex items-start justify-between gap-4">
            <div>
              <h2 id="room-invite-title" class="m-0 text-[15px] font-bold text-[#28243a]">分享房间</h2>
              <p class="mb-0 mt-1 text-[11px] text-[#89849a]">将邀请码发送给想邀请的成员</p>
            </div>
            <button
              class="grid size-8 place-items-center rounded-lg bg-transparent text-[#9893a6] hover:bg-[#f8f7ff] hover:text-[#5b35f5]"
              type="button"
              aria-label="关闭邀请码窗口"
              @click="inviteDialogOpen = false"
            >
              <X :size="18" />
            </button>
          </div>
          <p class="mb-2 mt-5 truncate text-[11px] text-[#77718c]">{{ inviteRoomName }}</p>
          <div class="flex items-center gap-2 rounded-xl border border-[#e8e2f5] bg-[#faf9ff] p-2">
            <code class="min-w-0 flex-1 truncate px-2 text-[17px] font-bold tracking-[0.16em] text-[#5234dc]">{{ inviteCode }}</code>
            <button
              class="min-h-9 rounded-lg bg-[#5b35f5] px-3 text-[11px] font-semibold text-white transition-colors hover:bg-[#4724d8]"
              type="button"
              @click="copyRoomInviteCode"
            >复制邀请码</button>
          </div>
        </section>
      </div>
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
