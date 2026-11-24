import axios from "axios";

export class ApiError extends Error {
  constructor(message, { code = 0, status = 0, data = null } = {}) {
    super(message);
    this.name = "ApiError";
    this.code = code;
    this.status = status;
    this.data = data;
  }
}

const http = axios.create({
  baseURL: "/api/v1",
  timeout: 15000,
  withCredentials: true,
});

const refreshHttp = axios.create({
  baseURL: "/api/v1",
  timeout: 10000,
  withCredentials: true,
});

let refreshPromise = null;

const unwrapResponse = (response) => {
  const payload = response.data;
  if (!payload || typeof payload !== "object" || payload.code !== 200) {
    const apiError = new ApiError(payload?.message || "服务端响应格式无效", {
      code: payload?.code || 0,
      status: response.status,
      data: payload?.data,
    });
    apiError.requestConfig = response.config;
    throw apiError;
  }
  return payload.data ?? null;
};

const refreshAccessToken = () => {
  if (!refreshPromise) {
    refreshPromise = refreshHttp
      .post("/users/refresh")
      .then(unwrapResponse)
      .then((data) => {
        if (!data) throw new ApiError("刷新令牌无效", { status: 401 });
        return data;
      })
      .finally(() => {
        refreshPromise = null;
      });
  }
  return refreshPromise;
};

const clearAuthAndRedirect = () => {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent("auth:expired"));
  if (window.location.pathname !== "/login") {
    const redirect = `${window.location.pathname}${window.location.search}`;
    // 用户登录后跳转到用户希望浏览的地址
    window.location.replace(`/login?redirect=${encodeURIComponent(redirect)}`);
  }
};

http.interceptors.response.use(unwrapResponse, async (error) => {
  const status = error.status || error.response?.status || 0;
  const requestConfig = error.requestConfig || error.config;
  const requestURL = requestConfig?.url || "";
  const isAuthRequest = /\/users\/(login|register|refresh)$/.test(requestURL);
  if (
    status === 401 &&
    !isAuthRequest &&
    !requestConfig?.skipAuthRefresh &&
    requestConfig &&
    !requestConfig._retry
  ) {
    requestConfig._retry = true;
    try {
      await refreshAccessToken();
      return http(requestConfig);
    } catch {
      clearAuthAndRedirect();
    }
  }
  if (error instanceof ApiError) throw error;
  const payload = error.response?.data;
  throw new ApiError(payload?.message || error.message || "网络连接失败", {
    code: payload?.code || error.response?.status || 0,
    status: error.response?.status || 0,
    data: payload?.data,
  });
});

export const loginUser = ({ account, password }) =>
  http.post("/users/login", {
    account: (account || "").trim(),
    password,
  });

export const registerUser = ({ phone, password, reconfirmPassword }) =>
  http.post("/users/register", {
    loginType: 1,
    phone: phone.trim(),
    password,
    reconfirmPassword,
  });

// 退出请求标记 skipAuthRefresh：即便返回 401 也不触发 token 刷新 / 硬跳，
// 退出流程由前端统一处理跳转，避免拦截器与 router 跳转打架。
export const logoutUser = () => http.post("/users/logout", null, { skipAuthRefresh: true });

export { refreshAccessToken as refreshSession };

export const findUserByPhoneOrUsername = (keyword) =>
  http.get("/users/resolve", { params: { keyword: (keyword || "").trim() } });

export const updateUserProfile = ({ username, nickName, avatar }) => {
  const payload = {};
  if (username !== undefined) payload.username = username;
  if (nickName !== undefined) payload.nickName = nickName;
  if (avatar !== undefined) payload.avatar = avatar;
  return http.patch("/users/me", payload);
};

export const getFriends = () => http.get("/friends");

export const getFriendRequests = () => http.get("/friend-requests");

export const createFriendRequest = ({ peerUserId, message }) =>
  http.post("/friend-requests", { peerUserId, message });

export const operateFriendRequest = ({ requestId, action }) =>
  http.post("/friend-requests/actions", { requestId, action });

export const createGroup = ({ groupName, description = "", avatar = "" }) =>
  http.post("/groups", { groupName, description, avatar });

export const getGroupInviteCode = (groupId) => http.get(`/groups/${groupId}/invite-code`);

const groupAgentRequest = (groupId, body, signal) =>
  fetch(`/group-agent/groups/${encodeURIComponent(groupId)}/agent`, {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
    },
    body: JSON.stringify(body),
    signal,
  });

const readErrorBody = async (response) => {
  const body = await response.json().catch(() => ({}));
  return body.detail ?? body.message ?? `HTTP ${response.status}`;
};

/** POST 群聊助手请求并按 SSE 帧顺序回调事件。 */
export const streamGroupAgent = async (groupId, body, onEvent, signal) => {
  let response = await groupAgentRequest(groupId, body, signal);
  if (response.status === 401) {
    await refreshAccessToken();
    response = await groupAgentRequest(groupId, body, signal);
  }
  if (!response.ok) throw new Error(await readErrorBody(response));
  if (!response.body) throw new Error("浏览器未提供 SSE 响应流");

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  const dispatchFrame = (frame) => {
    let event = "message";
    const data = [];
    for (const line of frame.split(/\r?\n/)) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
    }
    if (data.length) onEvent({ event, data: JSON.parse(data.join("\n")) });
  };

  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value, { stream: !done });
    const frames = buffer.split(/\r?\n\r?\n/);
    buffer = frames.pop() ?? "";
    for (const frame of frames) dispatchFrame(frame);
    if (done) {
      if (buffer.trim()) dispatchFrame(buffer);
      return;
    }
  }
};

export const joinGroup = (inviteCode) => http.post("/groups/join", { inviteCode });

export const getConversations = () => http.get("/conversations");

export const syncMessages = (conversationId, afterSeq = 0) =>
  http.get("/messages/sync", {
    params: {
      conversationId,
      afterSeq,
    },
  });

export const getSendMessageStatus = (clientMsgId) =>
  http.get(`/messages/send-status/${encodeURIComponent(clientMsgId)}`);

export const getOfflineMessages = ({ conversationId, afterSeq = 0, snapshotSeq, limit = 10 }) =>
  http.get("/messages/offline", {
    params: { conversationId, afterSeq, snapshotSeq, limit },
  });

// 分片规格协商（OpenIM PartLimit / PartSize）：先问规格，再算分片
export const getPartLimit = () => http.get("/files/uploads/part-limit");

export const getPartSize = (size) => http.post("/files/uploads/part-size", { size });

// payload: { name, fileName, contentType, size, fileHash, partSize }
// name 必须以当前 userID 为前缀，如 `${userID}/${uuid}${ext}`
export const initFileUpload = (payload) => http.post("/files/uploads/initiate-multipart", payload);

// parts: [{ partNumber, etag }]，每片 PUT 成功后从响应头 ETag 收集
export const completeFileUpload = (uploadId, parts = []) =>
  http.post(`/files/uploads/${uploadId}/complete-multipart`, { parts });

export const presignMultipartParts = (uploadId, partNumbers) =>
  http.post(`/files/uploads/${uploadId}/parts/auth-sign`, { partNumbers });

export const uploadMultipartPartToStorage = (url, chunk, signal) =>
  axios.put(url, chunk, {
    headers: { "Content-Type": "application/octet-stream" },
    signal,
  });

export const getAttachmentAccessURLs = (attachmentIds = []) =>
  http.post("/files/attachments/access-url", {
    attachmentIds,
  });

export default http;
