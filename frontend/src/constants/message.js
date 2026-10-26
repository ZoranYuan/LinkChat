export const MessageType = Object.freeze({
  TEXT: 1,
  IMAGE: 2,
  VIDEO: 3,
  STICKER: 4,
  FILE: 5,
});

export const messageViewType = (cType) => {
  switch (Number(cType)) {
    case MessageType.IMAGE:
      return "image";
    case MessageType.VIDEO:
      return "video";
    case MessageType.STICKER:
      return "sticker";
    case MessageType.FILE:
      return "file";
    default:
      return "text";
  }
};

export const messageTypeLabel = (cType) => {
  switch (Number(cType)) {
    case MessageType.IMAGE:
      return "[图片]";
    case MessageType.VIDEO:
      return "[视频]";
    case MessageType.STICKER:
      return "[表情]";
    case MessageType.FILE:
      return "[文件]";
    default:
      return "";
  }
};
