import { decodeFrame, decodePayload, encodeFrame } from "../wsProto.js";

const payloadTypes = {
  msg: "messageEvent",
  msg_ack: "messageAck",
  msg_read_notify: "readAckEvent",
  room_msg_notice: "roomMessageNotice",
};

const DEVICE_ID_KEY = "im_device_id";

/** 获取或创建当前浏览器设备标识，用于 WebSocket 连接参数。 */
const getDeviceId = () => {
  try {
    const existing = window.localStorage.getItem(DEVICE_ID_KEY);
    if (existing) return existing;
    const generated = globalThis.crypto?.randomUUID?.()
      || `web-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    window.localStorage.setItem(DEVICE_ID_KEY, generated);
    return generated;
  } catch {
    return `web-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }
};

/** 创建并管理当前用户的 WebSocket 连接、重连和业务帧分发。 */
export function createWsClient(callbacks = {}) {
  let socket = null;
  let authenticated = false;
  let reconnectTimer = 0;
  let reconnectAttempts = 0;
  let manualClose = false;
  let reconnectPreparation = null;
  let visibilityListenerAttached = false;

  const isPageVisible = () =>
    typeof document === "undefined" || document.visibilityState === "visible";

  /** 向上层报告连接状态变化。 */
  const notifyState = (state) => callbacks.onStateChange?.(state);

  /** 重连前只运行一份离线补齐任务，失败时由重连调度再次尝试。 */
  const prepareReconnect = () => {
    if (!callbacks.onBeforeReconnect) return Promise.resolve();
    if (!reconnectPreparation) {
      reconnectPreparation = Promise.resolve()
        .then(() => callbacks.onBeforeReconnect())
        .finally(() => {
          reconnectPreparation = null;
        });
    }
    return reconnectPreparation;
  };

  /** 延迟重连；必须先完成服务端会话快照和离线消息补齐。 */
  const scheduleReconnect = () => {
    if (manualClose || !authenticated || !isPageVisible()) return;
    reconnectAttempts += 1;
    const delay = Math.min(reconnectAttempts * 1000, 5000);
    reconnectTimer = window.setTimeout(async () => {
      if (!isPageVisible()) return;
      try {
        await prepareReconnect();
        if (manualClose || !authenticated || !isPageVisible()) return;
        if (!socket || socket.readyState === WebSocket.CLOSED) open();
      } catch (error) {
        callbacks.onReconnectPreparationError?.(error);
        scheduleReconnect();
      }
    }, delay);
  };

  /** 根据当前页面协议和设备标识生成 WebSocket 地址。 */
  const buildUrl = () => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const params = new URLSearchParams({
      device_id: getDeviceId(),
      platform: "web",
    });
    return `${protocol}//${window.location.host}/api/v1/ws?${params.toString()}`;
  };

  /** 解码单个业务帧，并分发给消息、ACK、已读或大群通知回调。 */
  const dispatchApplicationFrame = (frame) => {
    const payloadType = payloadTypes[frame.op];
    if (!payloadType) {
      callbacks.onUnknownFrame?.(frame);
      return;
    }
    const payload = decodePayload(payloadType, frame.data);
    if (frame.op === "msg") callbacks.onMessage?.(payload);
    if (frame.op === "msg_ack") callbacks.onAck?.(payload);
    if (frame.op === "msg_read_notify") callbacks.onReadNotify?.(payload);
    if (frame.op === "room_msg_notice") callbacks.onRoomMessageNotice?.(payload);
  };

  /** 解码服务端批量帧，并按帧内顺序分发业务事件。 */
  const dispatchFrame = (event) => {
    try {
      const frame = decodeFrame(event.data);
      if (frame.op !== "msg_batch") throw new Error(`unexpected websocket frame: ${frame.op}`);
      const batch = decodePayload("batch", frame.data);
      batch.frames.forEach(dispatchApplicationFrame);
    } catch (error) {
      notifyState("error");
      callbacks.onProtocolError?.(error);
    }
  };

  /** 建立一条新的 WebSocket 连接，并注册连接生命周期回调。 */
  const open = () => {
    if (!authenticated || typeof window === "undefined") return;
    window.clearTimeout(reconnectTimer);
    if (socket) {
      socket.onclose = null;
      socket.close();
    }
    notifyState("connecting");
    socket = new WebSocket(buildUrl());
    socket.binaryType = "arraybuffer";
    socket.onopen = () => {
      const recovered = reconnectAttempts > 0;
      reconnectAttempts = 0;
      notifyState("connected");
      reportPageVisibility();
      callbacks.onOpen?.({ recovered });
    };
    socket.onmessage = dispatchFrame;
    socket.onerror = (error) => {
      notifyState("error");
      callbacks.onError?.(error);
    };
    socket.onclose = () => {
      if (manualClose || !authenticated) return;
      notifyState("disconnected");
      if (isPageVisible()) scheduleReconnect();
    };
  };

  const reportPageVisibility = () => send(
    "connection_visibility",
    "connectionVisibility",
    { hidden: !isPageVisible() },
  );

  const handleVisibilityChange = () => {
    if (!isPageVisible()) {
      reportPageVisibility();
      return;
    }

    if (socket?.readyState === WebSocket.OPEN) {
      reportPageVisibility();
      return;
    }
    if (socket?.readyState === WebSocket.CONNECTING) return;
    void reconnect();
  };

  const attachVisibilityListener = () => {
    if (visibilityListenerAttached || typeof document === "undefined") return;
    document.addEventListener("visibilitychange", handleVisibilityChange);
    visibilityListenerAttached = true;
  };

  const detachVisibilityListener = () => {
    if (!visibilityListenerAttached || typeof document === "undefined") return;
    document.removeEventListener("visibilitychange", handleVisibilityChange);
    visibilityListenerAttached = false;
  };

  /** 在认证状态允许时启动连接；重复调用不会创建重复连接。 */
  const connect = (nextAuthenticated = true) => {
    const nextState = Boolean(nextAuthenticated);
    if (
      authenticated === nextState
      && socket
      && (socket.readyState === 0 || socket.readyState === 1)
    ) return;

    authenticated = nextState;
    manualClose = false;
    if (authenticated) attachVisibilityListener();
    open();
  };

  /** 主动关闭连接并取消自动重连。 */
  const disconnect = () => {
    manualClose = true;
    authenticated = false;
    detachVisibilityListener();
    window.clearTimeout(reconnectTimer);
    if (socket) {
      socket.onclose = null;
      socket.close();
      socket = null;
    }
    notifyState("disconnected");
  };

  /** 手动重连沿用自动重连前的离线补齐流程。 */
  const reconnect = async () => {
    if (!authenticated || !isPageVisible()) return;
    if (socket && (socket.readyState === WebSocket.CONNECTING || socket.readyState === WebSocket.OPEN)) return;
    manualClose = false;
    window.clearTimeout(reconnectTimer);
    try {
      await prepareReconnect();
      if (authenticated && !manualClose && isPageVisible() && (!socket || socket.readyState === WebSocket.CLOSED)) open();
    } catch (error) {
      callbacks.onReconnectPreparationError?.(error);
      scheduleReconnect();
    }
  };

  /** 将业务请求编码后发送；连接未打开时返回 false。 */
  const send = (op, typeName, payload) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    socket.send(encodeFrame(op, typeName, payload));
    return true;
  };

  return {
    connect,
    disconnect,
    reconnect,
    isConnected: () => socket?.readyState === WebSocket.OPEN,
    isConnecting: () => socket?.readyState === WebSocket.CONNECTING,
    sendMessage: (payload) => send("msg", "messageReq", payload),
    sendReadAck: (payload) => send("msg_read_ack", "readAck", payload),
    sendRoomEntry: (payload) => send("room_entry", "roomEntry", payload),
  };
}
