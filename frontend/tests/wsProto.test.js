import test from "node:test";
import assert from "node:assert/strict";
import { encodeFrame, decodeFrame, decodePayload } from "../src/wsProto.js";

test("普通视频发送保留 fileId，不再包含一起看时间字段", () => {
  const frame = decodeFrame(encodeFrame("msg", "messageReq", {
    clientMsgId: "client-video-1", conversationId: "conversation-1", cType: 3, fileId: "file-1",
  }));
  const message = decodePayload("messageReq", frame.data);
  assert.equal(message.fileId, "file-1");
  assert.equal(message.cType, 3);
  assert.equal(message.conversationId, "conversation-1");
  assert.equal(Object.hasOwn(message, "recvId"), false);
  assert.equal(Object.hasOwn(message, "convType"), false);
  assert.equal(Object.hasOwn(message, "videoTime"), false);
  assert.equal(Object.hasOwn(message, "hasVideoTime"), false);
});

test("普通视频推送保留 attachmentId 和 seq，不再包含一起看字段", () => {
  const frame = decodeFrame(encodeFrame("msg", "messageEvent", {
    messageId: "message-1", conversationId: "room-1", cType: 3, seq: 12, attachmentId: "attachment-1",
  }));
  const message = decodePayload("messageEvent", frame.data);
  assert.equal(message.attachmentId, "attachment-1");
  assert.equal(message.seq, 12);
  assert.equal(message.cType, 3);
  assert.equal(Object.hasOwn(message, "videoId"), false);
  assert.equal(Object.hasOwn(message, "videoTime"), false);
});
