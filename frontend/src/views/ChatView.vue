<script setup>
import { LoaderCircle, MessageCircle, Plus, UserRound, Users, X } from "@lucide/vue";
import { storeToRefs } from "pinia";
import { onMounted, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { findUserByPhoneOrUsername, getGroupInviteCode } from "../services/http/api.js";
import AppRail from "../components/chat/AppRail.vue";
import ChatPanel from "../components/chat/ChatPanel.vue";
import GroupAssistantPanel from "../components/chat/GroupAssistantPanel.vue";
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
  createGroup,
  joinGroup,
  retryConnection,
  pauseUpload,
  resumeUpload,
  cancelUpload,
  logout,
} = chatStore;

const activeSection = ref("conversations");
const mobileChatOpen = ref(false);
const groupSidePanel = ref("");
const groupDialogOpen = ref(false);
const groupMode = ref("create");
const groupSubmitting = ref(false);
const clearConfirmOpen = ref(false);
const inviteDialogOpen = ref(false);
const inviteLoading = ref(false);
const inviteCode = ref("");
const inviteGroupName = ref("");

const loadError = ref("");
const groupForm = reactive({ groupName: "", description: "", avatar: "", inviteCode: "" });

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
  groupSidePanel.value = "";
};

const chooseConversation = async (id) => {
  mobileChatOpen.value = true;
  groupSidePanel.value = "";
  try {
    await selectConversation(id);
    groupSidePanel.value = "";
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

const initializeGroupConversation = async (group) => {
  const conversation = state.conversations.find(
    (item) =>
      item.conversationId === group.conversationId ||
      (Number(item.convType) === ConversationType.GROUP && item.targetId === group.groupId),
  );
  if (!conversation) throw new Error("群聊已处理，但未找到对应会话，请重新加载会话列表。");

  groupDialogOpen.value = false;
  activeSection.value = "conversations";
  mobileChatOpen.value = true;
  groupSidePanel.value = "";
  await selectConversation(conversation.conversationId);
};

const submitGroup = async () => {
  groupSubmitting.value = true;
  try {
    if (groupMode.value === "create") {
      if (!groupForm.groupName.trim()) throw new Error("请输入群聊名称。");
      const group = await createGroup(groupForm);
      await initializeGroupConversation(group);
      messageTips.success("群聊创建成功");
    } else {
      if (!groupForm.inviteCode.trim()) throw new Error("请输入邀请码。");
      const group = await joinGroup(groupForm.inviteCode);
      await initializeGroupConversation(group);
      messageTips.success("已加入群聊");
    }
  } catch (error) {
    messageTips.error(error.message);
  } finally {
    groupSubmitting.value = false;
  }
};

const confirmClear = async () => {
  try {
    await clearConversation(activeConversation.value.conversationId);
    clearConfirmOpen.value = false;
    groupSidePanel.value = "";
    messageTips.success("本地聊天记录已清空");
  } catch (error) {
    messageTips.error({ title: "清空失败", message: error.message });
  }
};

const handleLogout = () => {
  // logout 立即发请求 + 清本地状态（不等待后端响应与 IndexedDB 清库），随后马上跳转
  logout();
  messageTips.success("已退出登录");
  void router.replace("/login");
};

const openDetails = () => {
  groupSidePanel.value = "details";
};

const openGroupAssistant = () => {
  groupSidePanel.value = "assistant";
};

const closeGroupSidePanel = () => {
  groupSidePanel.value = "";
};

const createGroupInvite = async () => {
  if (inviteLoading.value) return;
  const conversation = activeConversation.value;
  const groupId = conversation?.group?.groupId || conversation?.targetId;
  if (Number(conversation?.convType) !== ConversationType.GROUP || !groupId) return;

  inviteLoading.value = true;
  try {
    inviteCode.value = await getGroupInviteCode(groupId);
    inviteGroupName.value = conversation.displayName || "群聊";
    inviteDialogOpen.value = true;
  } catch (error) {
    messageTips.error({ title: "邀请码生成失败", message: error.message });
  } finally {
    inviteLoading.value = false;
  }
};

const copyGroupInviteCode = async () => {
  try {
    await navigator.clipboard.writeText(inviteCode.value);
    messageTips.success("邀请码已复制");
  } catch {
    messageTips.error("复制失败，请手动复制邀请码");
  }
};

watch(isMobile, (mobile) => {
  if (mobile && groupSidePanel.value === "details") closeGroupSidePanel();
});

onMounted(initialize);
</script>

<template>
  <main
    class="flex h-dvh min-h-0 w-full gap-3 overflow-hidden bg-[radial-gradient(circle_at_top_left,rgba(126,93,255,0.08),transparent_34%),var(--im-page)] p-3 text-sm max-[767px]:block max-[767px]:p-0">
    <AppRail :active-section="activeSection" :current-user="state.currentUser"
      @select="chooseSection" @logout="handleLogout" />

    <section
      class="im-workspace relative min-h-0 min-w-0 w-full h-full flex-1 overflow-hidden rounded-2xl border border-[var(--im-border-strong)] bg-[var(--im-surface)] shadow-[var(--im-shadow-sm)]">
      <aside :class="[
        'directory-shell relative min-w-0 overflow-hidden border-r border-[var(--im-border)] bg-[var(--im-surface-subtle)]',
        { 'mobile-hidden': mobileChatOpen },
      ]">
        <ConversationList v-if="activeSection === 'conversations'" :conversations="state.conversations"
          :active-id="state.activeConversationId" :loading="state.loading" :connection="state.connection"
          @select="chooseConversation" @retry="retryConnection" @create-group="groupDialogOpen = true" />
        <ContactsPanel v-else-if="activeSection === 'contacts'" :contacts="state.contacts"
          :requests="state.friendRequests" @open-chat="handleContact" @handle-request="handleRequest"
          :on-add-friend="handleAddFriend" />
        <ProfilePanel v-else :current-user="state.currentUser" :save-profile="updateProfile" @logout="handleLogout" />
        <div v-if="loadError"
          class="absolute inset-x-3 top-3 z-10 grid gap-1 rounded-xl border border-[var(--im-danger)] bg-[var(--im-danger-soft)] p-3 text-[var(--im-fs-11)] text-[var(--im-danger)]">
          <strong>暂时无法同步数据</strong><span>{{ loadError }}</span><button
            class="w-fit bg-transparent font-semibold text-[var(--im-primary)]" type="button" @click="initialize">
            重新加载
          </button>
        </div>
      </aside>

      <div class="conversation-stage">
        <ChatPanel :visible="!isMobile || mobileChatOpen" :conversation="activeConversation" :messages="activeMessages"
          :share-loading="inviteLoading"
          :current-user="state.currentUser" :connection="state.connection" :loading="state.historyLoading"
          :has-more="state.historyHasMore[state.activeConversationId]" :mobile="isMobile" @back="mobileChatOpen = false"
          @details="openDetails" @share-group="createGroupInvite" @send="handleSend" @attachment="handleAttachment" @retry-message="handleRetryMessage"
          @assistant="openGroupAssistant"
          @pause-upload="pauseUpload" @resume-upload="resumeUpload" @cancel-upload="cancelUpload"
          @load-older="loadOlderMessages" />

        <div v-if="groupSidePanel && isMobile" class="fixed inset-0 z-44 bg-slate-900/30" @click="closeGroupSidePanel"></div>
        <Transition name="group-side-panel">
          <div v-if="groupSidePanel" class="group-side-panel-shell">
            <Transition name="group-side-panel-content" mode="out-in">
              <ConversationDetails
                v-if="groupSidePanel === 'details'"
                key="group-details"
                :conversation="activeConversation"
                @close="closeGroupSidePanel"
                @clear="clearConfirmOpen = true"
              />
              <GroupAssistantPanel
                v-else-if="groupSidePanel === 'assistant' && Number(activeConversation?.convType) === ConversationType.GROUP"
                key="group-assistant"
                :conversation="activeConversation"
                @close="closeGroupSidePanel"
              />
            </Transition>
          </div>
        </Transition>
      </div>
    </section>

    <nav v-if="!mobileChatOpen"
      class="fixed bottom-0 left-0 right-0 z-30 flex h-16 items-center justify-around border-t border-[var(--im-border)] bg-[var(--im-surface)]/95 px-5 shadow-[0_-8px_24px_var(--im-shadow-sm)] backdrop-blur min-[768px]:hidden"
      aria-label="移动端导航">
      <button :class="activeSection === 'conversations' ? 'text-[var(--im-primary)]' : 'text-[var(--im-text-muted)]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[var(--im-fs-10)]" type="button"
        @click="chooseSection('conversations')">
        <MessageCircle :size="21" /><span>消息</span>
      </button>
      <button :class="activeSection === 'contacts' ? 'text-[var(--im-primary)]' : 'text-[var(--im-text-muted)]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[var(--im-fs-10)]" type="button"
        @click="chooseSection('contacts')">
        <Users :size="21" /><span>联系人</span>
      </button>
      <button
        class="grid size-11 -translate-y-3 place-items-center rounded-full bg-gradient-to-br from-[var(--im-primary)] to-[var(--im-primary-hover)] text-white shadow-[var(--im-shadow-md)]"
        type="button" title="新建群聊" @click="groupDialogOpen = true">
        <Plus :size="22" />
      </button>
      <button :class="activeSection === 'profile' ? 'text-[var(--im-primary)]' : 'text-[var(--im-text-muted)]'"
        class="flex flex-col items-center gap-1 bg-transparent text-[var(--im-fs-10)]" type="button"
        @click="chooseSection('profile')">
        <UserRound :size="21" /><span>我的</span>
      </button>
    </nav>

    <teleport to="body">
      <Transition name="workspace-loading">
        <div
          v-if="state.loading"
          class="fixed inset-0 z-[90] grid place-items-center bg-[var(--im-page)]/70 backdrop-blur-[3px]"
          role="status"
          aria-live="polite"
        >
          <div class="flex flex-col items-center gap-3.5 text-[var(--im-text-secondary)]">
            <span class="grid size-14 place-items-center rounded-2xl bg-[var(--im-surface)] shadow-[var(--im-shadow-md)]">
              <LoaderCircle class="animate-spin text-[var(--im-primary)]" :size="26" />
            </span>
            <span class="text-[var(--im-fs-13)] font-semibold">正在加载会话…</span>
          </div>
        </div>
      </Transition>
    </teleport>

    <teleport to="body">
      <div
        v-if="inviteDialogOpen"
        class="fixed inset-0 z-[80] grid place-items-center bg-slate-900/35 p-4 backdrop-blur-[2px]"
        @click.self="inviteDialogOpen = false"
      >
        <section
          class="w-full max-w-[380px] rounded-2xl border border-[var(--im-border)] bg-[var(--im-surface)] p-5 shadow-[0_24px_70px_rgba(32,24,64,0.22)]"
          role="dialog"
          aria-modal="true"
          aria-labelledby="group-invite-title"
        >
          <div class="flex items-start justify-between gap-4">
            <div>
              <h2 id="group-invite-title" class="m-0 text-[var(--im-fs-15)] font-bold text-[var(--im-text)]">分享群聊</h2>
              <p class="mb-0 mt-1 text-[var(--im-fs-11)] text-[var(--im-text-muted)]">将邀请码发送给想邀请的成员</p>
            </div>
            <button
              class="grid size-8 place-items-center rounded-lg bg-transparent text-[var(--im-text-muted)] hover:bg-[var(--im-primary-softer)] hover:text-[var(--im-primary)]"
              type="button"
              aria-label="关闭邀请码窗口"
              @click="inviteDialogOpen = false"
            >
              <X :size="18" />
            </button>
          </div>
          <p class="mb-2 mt-5 truncate text-[var(--im-fs-11)] text-[var(--im-text-secondary)]">{{ inviteGroupName }}</p>
          <div class="flex items-center gap-2 rounded-xl border border-[var(--im-primary-border)] bg-[var(--im-primary-softer)] p-2">
            <code class="min-w-0 flex-1 truncate px-2 text-[var(--im-fs-17)] font-bold tracking-[0.16em] text-[var(--im-primary-hover)]">{{ inviteCode }}</code>
            <button
              class="min-h-9 rounded-lg bg-[var(--im-primary)] px-3 text-[var(--im-fs-11)] font-semibold text-white transition-colors hover:bg-[var(--im-primary-hover)]"
              type="button"
              @click="copyGroupInviteCode"
            >复制邀请码</button>
          </div>
        </section>
      </div>
      <div v-if="groupDialogOpen" class="fixed inset-0 z-70 grid place-items-center bg-slate-900/45 px-4.5"
        @click.self="groupDialogOpen = false">
        <section
          class="w-full max-w-110 rounded-2xl border border-[var(--im-border-strong)] bg-[var(--im-surface)] p-5.5 shadow-[0_24px_70px_var(--im-shadow-sm)]">
          <header class="flex items-start justify-between">
            <div>
              <h2 class="m-0 text-[var(--im-fs-19)] font-bold text-[var(--im-text)]">群聊</h2>
              <p class="mt-1 text-[var(--im-fs-11)] text-[var(--im-text-secondary)]">创建新群聊或使用邀请码加入</p>
            </div>
            <button
              class="grid size-9 place-items-center rounded-lg bg-transparent text-[var(--im-text-muted)] transition-colors hover:bg-[var(--im-primary-soft)] hover:text-[var(--im-primary)]"
              type="button" title="关闭" @click="groupDialogOpen = false">
              <X :size="20" />
            </button>
          </header>
          <div class="mt-5.5 grid grid-cols-2 border-b border-[var(--im-border-strong)]">
            <button :class="groupMode === 'create'
                ? 'border-[var(--im-primary)] text-[var(--im-primary)] font-semibold'
                : 'border-transparent text-[var(--im-text-secondary)]'
              " class="min-h-10 border-b-2 bg-transparent text-xs" type="button" @click="groupMode = 'create'">
              创建群聊</button><button :class="groupMode === 'join'
                  ? 'border-[var(--im-primary)] text-[var(--im-primary)] font-semibold'
                  : 'border-transparent text-[var(--im-text-secondary)]'
                " class="min-h-10 border-b-2 bg-transparent text-xs" type="button" @click="groupMode = 'join'">
              邀请码加入
            </button>
          </div>
          <form class="mt-5 grid gap-4" @submit.prevent="submitGroup">
            <template v-if="groupMode === 'create'">
              <label class="grid gap-1.5"><span class="text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]">群聊名称</span><input
                  v-model.trim="groupForm.groupName" maxlength="32"
                  class="min-h-10.5 rounded-lg placeholder:text-[var(--im-text-faint)] placeholder:text-[var(--im-fs-13)] border border-[var(--im-border-strong)] px-3 text-xs outline-0 focus:border-[var(--im-primary-tint)] focus:ring-4 focus:ring-[var(--im-shadow-focus)]" /></label>
              <label class="grid gap-1.5"><span class="text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]">群聊说明</span>
                <textarea v-model.trim="groupForm.description" rows="3" maxlength="120" placeholder="简要说明群聊用途"
                  class="resize-y rounded-lg border border-[var(--im-border-strong)] px-3 py-2.5 placeholder:text-[var(--im-text-faint)] placeholder:text-[var(--im-fs-13)] outline-0 focus:border-[var(--im-primary-tint)] focus:ring-4 focus:ring-[var(--im-shadow-focus)]">
                </textarea>
              </label>
            </template>
            <label v-else class="grid gap-1.5"><span class="text-[var(--im-fs-11)] font-semibold text-[var(--im-text-secondary)]">邀请码</span><input
                v-model.trim="groupForm.inviteCode" placeholder="请输入群聊邀请码"
                class="min-h-10.5 rounded-lg border border-[var(--im-border-strong)] px-3 text-xs placeholder:text-xs outline-0 focus:border-[var(--im-primary-tint)] focus:ring-4 focus:ring-[var(--im-shadow-focus)]" /></label>
            <div class="mt-1 flex justify-end gap-2">
              <button
                class="min-h-10.5 rounded-lg border border-[var(--im-border-strong)] bg-[var(--im-surface)] px-4 text-xs text-[var(--im-text-secondary)] hover:bg-[var(--im-primary-softer)]"
                type="button" @click="groupDialogOpen = false">
                取消</button><button
                class="min-h-10.5 rounded-lg bg-[var(--im-primary)] px-4 text-xs font-semibold text-white transition-colors hover:bg-[var(--im-primary-hover)] disabled:cursor-not-allowed disabled:opacity-50"
                type="submit" :disabled="groupSubmitting">
                {{ groupSubmitting ? "处理中..." : groupMode === "create" ? "创建" : "加入" }}
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
