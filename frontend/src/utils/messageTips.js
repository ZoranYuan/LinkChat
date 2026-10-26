import { ElMessage } from "element-plus";
import "element-plus/es/components/message/style/css";

function normalizeOptions(input, fallbackType = "info") {
  if (typeof input === "string") {
    return {
      message: input,
      type: fallbackType,
    };
  }

  const {
    title = "",
    message = "",
    duration = 3000,
    grouping = true,
    type = fallbackType,
  } = input || {};

  return {
    message: title && message ? `${title}：${message}` : title || message,
    type,
    duration,
    grouping,
  };
}

function show(input, fallbackType = "info") {
  return ElMessage(normalizeOptions(input, fallbackType));
}

export const messageTips = {
  success(input) {
    return show(input, "success");
  },
  error(input) {
    return show(input, "error");
  },
  warning(input) {
    return show(input, "warning");
  },
  info(input) {
    return show(input, "info");
  },
};
