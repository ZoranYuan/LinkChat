export const findConversation = (conversations, conversationId) => (
  (conversations || []).find((item) => item.conversationId === conversationId) || null
);

export const sortConversations = (conversations = []) => [...conversations].sort((left, right) => {
  const rightTime = Number(right.lastMessage?.sendTime) || 0;
  const leftTime = Number(left.lastMessage?.sendTime) || 0;
  return rightTime - leftTime;
});

/** 用已确认的新消息更新会话列表的内存预览和未读展示。 */
export const applyRealtimeMessageToConversation = (
  conversation,
  message,
  { active = false, increaseUnread = false } = {},
) => {
  if (!conversation || !message) return false;

  conversation.latestSeq = Math.max(
    Number(conversation.latestSeq) || 0,
    Number(message.seq) || 0,
  );

  conversation.lastMessage = {
    messageId: message.messageId,
    conversationId: message.conversationId,
    senderId: message.senderId,
    cType: message.cType,
    content: message.content,
    sendTime: message.sendTime,
    status: message.status,
  };

  if (active) {
    conversation.unread = 0;
  } else if (increaseUnread) {
    conversation.unread = (Number(conversation.unread) || 0) + 1;
  }

  return true;
};
