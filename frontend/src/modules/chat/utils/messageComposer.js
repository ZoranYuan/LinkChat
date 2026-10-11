import { MessageType } from "../../../constants/message.js";

export const createClientMessageId = () => (
  globalThis.crypto?.randomUUID?.()
  || `message-${Date.now()}-${Math.random().toString(16).slice(2)}`
);

export const createOutgoingMessage = (conversation, payload, currentUser) => {
  const sendTime = Date.now();
  return {
    messageId: "",
    conversationId: conversation.conversationId,
    clientMsgId: payload.clientMsgId,
    senderId: currentUser.userId,
    senderUsername: currentUser.nickName || currentUser.username || "我",
    seq: 0,
    content: payload.content || "",
    cType: payload.cType,
    convType: payload.convType,
    attachmentId: payload.attachmentId || "",
    fileName: payload.fileName || "",
    fileSize: payload.fileSize || 0,
    mediaUrl: payload.mediaUrl || "",
    thumbUrl: payload.thumbUrl || "",
    width: payload.width || 0,
    height: payload.height || 0,
    durationMs: payload.durationMs || 0,
    sendTime,
    status: 1,
  };
};

/** 独立构造发往服务端的消息请求，不复用会话里的展示占位对象。 */
export const createMessageRequest = (conversation, payload) => {
  const request = {
    clientMsgId: payload.clientMsgId,
    recvId: conversation.targetId,
    convType: conversation.convType,
    cType: payload.cType,
  };

  if (Number(payload.cType) === MessageType.TEXT) {
    request.content = payload.content || "";
  } else if (payload.fileId) {
    request.fileId = payload.fileId;
  }

  return request;
};

export const createConversationPreview = (conversation, message, currentUser) => ({
  messageId: message.messageId || "",
  conversationId: conversation.conversationId,
  senderId: currentUser.userId,
  seq: Number(message.seq) || 0,
  convType: conversation.convType,
  cType: message.cType,
  content: message.content || message.fileName || "",
  sendTime: message.sendTime || Date.now(),
});
