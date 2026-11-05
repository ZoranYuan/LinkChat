import protobuf from "protobufjs";

const schema = `
syntax = "proto3";
package im.ws;
message WsFrame { string op = 1; bytes data = 2; }
message EventBatch { repeated WsFrame frames = 1; string batch_id = 2; }
message EventBatchAck { string batch_id = 1; }
message MessageReadRequest { string message_id = 1; }
message RoomEnterRequest { string room_id = 1; }
message ConnectionVisibilityRequest { optional bool hidden = 1; }
message MessageSendRequest {
  string client_msg_id = 1; string conversation_id = 19; int32 c_type = 4;
  string content = 5; reserved 6, 7;
  string file_id = 10;
  string sticker_id = 17; string pack_id = 18;
}
message MessageSendResult { string client_msg_id = 1; string message_id = 2; string status = 3; string extra = 4; int64 send_time = 5; string conversation_id = 6; int64 seq = 7; string attachment_id = 8; }
message RoomMessageNotice { string conversation_id = 1; string message_id = 2; int64 seq = 3; }
message MessageReadNotice { string user_id = 1; string conversation_id = 2; int64 last_read_seq = 3; int32 conv_type = 4; string avatar = 6; }
message FriendRequestNotice { string request_id = 1; string applicant_user_id = 2; string peer_user_id = 3; string message = 4; int64 apply_time = 5; }
message ErrorNotice { string request_op = 1; string code = 2; string message = 3; }
message MessagePush {
  string message_id = 1; string conversation_id = 2; string sender_id = 3;
  int64 seq = 5; int32 c_type = 7; string content = 8; int64 send_time = 9;
  string client_msg_id = 11; reserved 12, 13;
  int32 status = 14; string attachment_id = 25;
}
`;

const root = protobuf.parse(schema).root;
const types = {
  frame: root.lookupType("im.ws.WsFrame"),
  eventBatch: root.lookupType("im.ws.EventBatch"),
  eventBatchAck: root.lookupType("im.ws.EventBatchAck"),
  connectionVisibilityRequest: root.lookupType("im.ws.ConnectionVisibilityRequest"),
  messageSendRequest: root.lookupType("im.ws.MessageSendRequest"),
  messageSendResult: root.lookupType("im.ws.MessageSendResult"),
  roomMessageNotice: root.lookupType("im.ws.RoomMessageNotice"),
  messagePush: root.lookupType("im.ws.MessagePush"),
  messageReadRequest: root.lookupType("im.ws.MessageReadRequest"),
  roomEnterRequest: root.lookupType("im.ws.RoomEnterRequest"),
  messageReadNotice: root.lookupType("im.ws.MessageReadNotice"),
  friendRequestNotice: root.lookupType("im.ws.FriendRequestNotice"),
  errorNotice: root.lookupType("im.ws.ErrorNotice"),
};

/** 将 protobuf 消息转换为前端使用的普通 JavaScript 对象。 */
const toPlain = (type, message) =>
  type.toObject(message, { longs: Number, enums: String, bytes: Uint8Array, defaults: true });

/** 将业务 payload 编码成带有操作类型的 WebSocket 二进制帧。 */
export function encodeFrame(op, typeName, payload) {
  const type = types[typeName];
  const data = type.encode(type.create(payload)).finish();
  return types.frame.encode(types.frame.create({ op, data })).finish();
}

/** 解码 WebSocket 二进制数据的外层帧，得到操作类型和业务数据。 */
export function decodeFrame(buffer) {
  return toPlain(types.frame, types.frame.decode(new Uint8Array(buffer)));
}

/** 根据操作对应的 protobuf 类型解码帧内业务数据。 */
export function decodePayload(typeName, data) {
  const type = types[typeName];
  return toPlain(type, type.decode(data));
}
