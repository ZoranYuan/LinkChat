import assert from "node:assert/strict";
import test from "node:test";
import { MessageType } from "../src/constants/message.js";
import {
  createMessageRequest,
  createOutgoingMessage,
} from "../src/modules/chat/utils/messageComposer.js";

const conversation = {
  conversationId: "conversation-1",
  targetId: "room-1",
  convType: 2,
};

const currentUser = {
  userId: "user-1",
  nickName: "测试用户",
};

test("媒体占位消息不包含上传 fileId，附件标识等待服务端 ACK 回填", () => {
  const displayData = {
    clientMsgId: "client-msg-1",
    cType: MessageType.IMAGE,
    convType: conversation.convType,
    fileId: "uploaded-file-1",
    fileName: "photo.png",
    fileSize: 1024,
    mediaUrl: "blob:preview",
  };

  const placeholder = createOutgoingMessage(conversation, displayData, currentUser);

  assert.equal(placeholder.attachmentId, "");
  assert.equal(Object.hasOwn(placeholder, "fileId"), false);
  assert.equal(placeholder.fileName, "photo.png");
  assert.equal(placeholder.mediaUrl, "blob:preview");
});

test("媒体 WS 请求单独携带 fileId，不复用占位消息对象", () => {
  const displayData = {
    clientMsgId: "client-msg-2",
    cType: MessageType.FILE,
    convType: conversation.convType,
    fileId: "uploaded-file-2",
    fileName: "report.pdf",
  };
  const placeholder = createOutgoingMessage(conversation, displayData, currentUser);
  const request = createMessageRequest(conversation, {
    clientMsgId: displayData.clientMsgId,
    cType: displayData.cType,
    fileId: displayData.fileId,
  });

  assert.notStrictEqual(request, placeholder);
  assert.deepEqual(request, {
    clientMsgId: "client-msg-2",
    recvId: "room-1",
    convType: 2,
    cType: MessageType.FILE,
    fileId: "uploaded-file-2",
  });
  assert.equal(Object.hasOwn(placeholder, "fileId"), false);
});

test("文本占位和文本 WS 请求分别构造，文本正文正确进入请求", () => {
  const displayData = {
    clientMsgId: "client-msg-3",
    cType: MessageType.TEXT,
    convType: conversation.convType,
    content: "你好",
  };
  const placeholder = createOutgoingMessage(conversation, displayData, currentUser);
  const request = createMessageRequest(conversation, displayData);

  assert.notStrictEqual(request, placeholder);
  assert.equal(request.content, "你好");
  assert.equal(Object.hasOwn(request, "fileId"), false);
  assert.equal(placeholder.content, "你好");
});
