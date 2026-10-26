export const findConversation = (conversations, conversationId) =>
  (conversations || []).find((item) => item.conversationId === conversationId) || null;

export const sortConversations = (conversations = []) =>
  [...conversations].sort((left, right) => {
    const rightTime = Number(right.lastMessage?.sendTime) || 0;
    const leftTime = Number(left.lastMessage?.sendTime) || 0;
    return rightTime - leftTime;
  });

/** 用已确认的新消息更新会话列表的内存预览和未读展示。 */
export const applyMessagesToConversation = (
  conversation,
  messages,
  activeConversationId,
  currentUserId,
) => {
  const message = messages.at(-1);
  if (!message) return;

  if (conversation.conversationId === activeConversationId) {
    conversation.unread = 0;
  } else {
    conversation.unread =
      (Number(conversation.unread) || 0) +
      messages.filter(
        (item) =>
          Number(item.seq) > (Number(conversation.latestSeq) || 0) &&
          item.senderId !== currentUserId,
      ).length;
  }
  conversation.latestSeq = Math.max(Number(conversation.latestSeq) || 0, Number(message.seq) || 0);

  conversation.lastMessage = {
    messageId: message.messageId,
    conversationId: message.conversationId,
    senderId: message.senderId,
    cType: message.cType,
    content: message.content,
    sendTime: message.sendTime,
    status: message.status,
  };
};
