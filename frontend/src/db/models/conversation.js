export const CONVERSATION_STORE_NAME = "conversations";

export const CONVERSATION_KEY_PATH = ["userId", "conversationId"];

export const CONVERSATION_INDEXES = {
  USER_ID: "userId",
};

/** 将后端会话对象附加当前用户 ID，转换为 IndexedDB 会话记录。 */
export const createConversationRecord = (userId, conversation = {}) => {
  if (!userId || !conversation.conversationId) return null;

  return {
    userId,
    conversationId: conversation.conversationId,
    convType: conversation.convType,
    latestSeq: Number(conversation.latestSeq) || 0,
    targetId: conversation.targetId,
    displayName: conversation.displayName,
    avatar: conversation.avatar,
    // 会话表只缓存消息摘要；未读数只保留在运行时内存。
    lastMessage: conversation.lastMessage
      ? {
          messageId: conversation.lastMessage.messageId || "",
          conversationId: conversation.lastMessage.conversationId || conversation.conversationId,
          senderId: conversation.lastMessage.senderId || "",
          cType: Number(conversation.lastMessage.cType) || 1,
          content: conversation.lastMessage.content || "",
          sendTime: Number(conversation.lastMessage.sendTime) || 0,
        }
      : null,
    peerUser: conversation.peerUser,
    room: conversation.room,
    // 本地消息同步边界，初始值为 0，之后由消息落库流程维护。
    lastContinuousSeq: Number(conversation.lastContinuousSeq) || 0,
    // 对端最高已读水位，仅用于私聊最后一条本人消息的“已读/已发送”展示。
    readWatermark: Number(conversation.readWatermark) || 0,
  };
};
