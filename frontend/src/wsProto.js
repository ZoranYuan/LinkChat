import protobuf from "protobufjs";

const schema = `
syntax = "proto3";
package im.ws;
message WsFrame { string op = 1; bytes data = 2; }
message WsBatch { repeated WsFrame frames = 1; }
message MessageReadAckReq { string message_id = 1; }
message RoomEntryReq { string room_id = 1; }
message ConnectionVisibilityReq { optional bool hidden = 1; }
message MessageReq {
  string client_msg_id = 1; string conversation_id = 19; int32 c_type = 4;
  string content = 5; reserved 6, 7;
  string file_id = 10;
  string sticker_id = 17; string pack_id = 18;
}
message MessageAck { string client_msg_id = 1; string message_id = 2; string status = 3; string extra = 4; int64 send_time = 5; string conversation_id = 6; int64 seq = 7; string attachment_id = 8; }
message RoomMessageNotice { string conversation_id = 1; string message_id = 2; int64 seq = 3; }
message MessageReadAckEvent { string user_id = 1; string conversation_id = 2; int64 last_read_seq = 3; int32 conv_type = 4; string avatar = 6; }
message MessageEvent {
  string message_id = 1; string conversation_id = 2; string sender_id = 3;
  int64 seq = 5; int32 c_type = 7; string content = 8; int64 send_time = 9;
  string client_msg_id = 11; reserved 12, 13;
  int32 status = 14; string attachment_id = 25;
}
`;

const root = protobuf.parse(schema).root;
const types = {
  frame: root.lookupType("im.ws.WsFrame"),
  batch: root.lookupType("im.ws.WsBatch"),
  connectionVisibility: root.lookupType("im.ws.ConnectionVisibilityReq"),
  messageReq: root.lookupType("im.ws.MessageReq"),
  messageAck: root.lookupType("im.ws.MessageAck"),
  roomMessageNotice: root.lookupType("im.ws.RoomMessageNotice"),
  messageEvent: root.lookupType("im.ws.MessageEvent"),
  readAck: root.lookupType("im.ws.MessageReadAckReq"),
  roomEntry: root.lookupType("im.ws.RoomEntryReq"),
  readAckEvent: root.lookupType("im.ws.MessageReadAckEvent"),
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
