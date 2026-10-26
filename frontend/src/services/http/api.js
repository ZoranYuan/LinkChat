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
  if (status === 401 && !isAuthRequest && requestConfig && !requestConfig._retry) {
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

export const logoutUser = () => http.post("/users/logout");

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

export const createRoom = ({ roomName, description = "", avatar = "" }) =>
  http.post("/rooms", { roomName, description, avatar });

export const joinRoom = (inviteCode) => http.post("/rooms/join", { inviteCode });

export const getConversations = () => http.get("/conversations");

export const syncMessages = (conversationId, afterSeq = 0) =>
  http.get("/messages/sync", {
    params: {
      conversationId,
      afterSeq,
    },
  });

export const getOfflineMessages = ({ conversationId, afterSeq = 0, snapshotSeq, limit = 10 }) =>
  http.get("/messages/offline", {
    params: { conversationId, afterSeq, snapshotSeq, limit },
  });

export const initFileUpload = (payload) => http.post("/files/uploads/init", payload);

export const uploadDirectObjectToStorage = (url, file, signal) =>
  axios.put(url, file, {
    headers: { "Content-Type": file.type || "application/octet-stream" },
    signal,
  });

export const completeFileUpload = (uploadId) => http.post(`/files/uploads/${uploadId}/complete`);

export const presignMultipartParts = (uploadId, partNumbers) =>
  http.post(`/files/uploads/${uploadId}/parts/presign`, { partNumbers });

export const uploadMultipartPartToStorage = (url, chunk, signal) =>
  axios.put(url, chunk, {
    headers: { "Content-Type": "application/octet-stream" },
    signal,
  });

export const getAttachmentAccessURLs = (attachmentIds = []) =>
  http.post("/files/attachments/access-urls", {
    attachmentIds,
  });

export default http;
