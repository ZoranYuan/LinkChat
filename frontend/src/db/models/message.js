export const MESSAGE_STORE_NAME = "messages";

export const MESSAGE_KEY_PATH = ["conversationId", "seq"];

export const MESSAGE_INDEXES = {
  CONVERSATION: "conversation",
  CONVERSATION_SEQ: "conversationSeq",
};

/** 将消息转换为 IndexedDB 记录，只保留消息本体和业务状态。 */
export const createMessageRecord = (message) => {
  const seq = Number(message?.seq);
  // 正常消息使用 conversationId + seq 作为 IndexedDB 主键。
  if (!message?.conversationId || !message?.messageId || !Number.isSafeInteger(seq) || seq <= 0) {
    return null;
  }

  return {
    messageId: message.messageId,
    conversationId: message.conversationId,
    senderId: message.senderId || "",
    clientMsgId: message.clientMsgId || "",
    seq,
    cType: Number(message.cType) || 1,
    content: message.content || "",

    // 后端消息业务状态：1=normal，2=recall。
    status: Number(message.status) || 1,
    sendTime: Number(message.sendTime) || 0,
    attachmentId: message.attachmentId || "",
  };
};
