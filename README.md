# SYCHAT

基于 Go + Vue 3 的即时通信项目，支持账号、好友、房间、私聊与群聊、媒体上传、离线补齐和房间摘要 Agent。

## 技术栈

Go、Gin、GORM、MySQL、Redis、Kafka、MinIO、Protobuf、gRPC、SSE、Vue 3、Vite。

## 消息链路

```text
WebSocket MessageReq
  → 鉴权、关系校验和客户端重试检查
  → MySQL 事务：分配会话 seq、持久化消息与附件、写入 Outbox
  → 返回 msg_ack（sent 表示事务提交，不代表对方已接收）
  → Kafka Producer 批量领取 Outbox，以 MessageKey 为 Kafka Key 直接批量投递
  → Kafka Consumer 逐条提交 worker，使用 Inbox 管理幂等与处理租约
  → MessageDelivery / 本地 Gateway
  → Session 出站聚合队列 → WebSocket 批量帧
```

发送请求直接执行单条 MySQL 事务，不经过应用层写入队列或凑批窗口。会话序号由事务内锁定会话行逐条分配，不由 Redis INCR 生成。Kafka 分区键使相同会话进入同一分区，但不能单独保证多实例 Outbox 投递时的 seq 顺序；客户端仍需按 seq 去重、排序和补齐缺口。

普通房间推送完整消息；大群或活跃房间使用 `room_msg_notice` 合并通知，客户端通过 `/messages/sync` 补拉。新群消息事务提交后逐条记录 Redis 活跃度，达到 ACTIVE 时立即更新发送端与 Consumer 共享的本地标识；最后一次 ACTIVE 后保持 120 秒，Consumer 读取不续期，也不再查询 Redis 活跃度。大群、WARN/ACTIVE 房间及本地 ACTIVE 保持期间的新增消息继续预热，Redis 近期消息 TTL 仍为 10 分钟；缓存失败不改变成功 ACK。该标识仅在当前节点有效，重启后丢失，不是多实例共享状态。通知合并器每房间只保留最大 seq 对应的轻量通知，定时向所有在线成员推送，不保存完整消息或预热缓存。待通知房间数不设固定上限。房间判断和通知聚合参数位于 [config.yaml](backend/configs/config.yaml)。

Kafka Key 仍使用会话 ID；Consumer 的本地 worker 调度与 Kafka 分区独立。普通新消息按 eventId 分片，允许同会话不同消息并发处理、乱序到达；其他事件仍按原 Key 串行处理。客户端从本地连续水位补齐缺口后有序展示，worker 独立执行、更新 Inbox 和重试；消费循环持续接收，只有 worker 队列满时施加背压。worker 完成业务和 Inbox 状态更新后直接标记 offset，不维护连续完成水位；较大的 offset 可能先提交，重启后较小的未完成事件可能不再被 Kafka 重放。聊天推送依赖客户端同步补偿，其他事件也需要对应的业务恢复机制。

## WebSocket 协议

当前使用单节点 Gateway，用户推送、房间广播和在线连接统计直接使用本地连接索引。

入口：`GET /api/v1/ws`，需要有效 JWT。Session 绑定用户、设备及登录会话，用户可以有多个连接。服务端维护 Ping/Pong、读写截止时间和慢客户端隔离。

每个 Session 按接收顺序向有界入站 worker pool 提交请求，`ws.inbound_worker_count` 当前配置为 4；执行完成、seq 分配及 ACK 返回顺序可能不同，客户端按 `clientMsgId` 关联 ACK、按 seq 排列消息。成功 ACK 仍在数据库事务提交后返回。连接关闭时取消 worker 上下文及尚未提交的发送事务；已提交但未收到 ACK 的消息使用原 clientMsgId 重试。入站队列满时仍关闭连接；增加并行度不等于提高单房间数据库写入容量。

| Op | 方向 | Payload |
|---|---|---|
| `msg` | 客户端 → 服务端 | `MessageReq` |
| `msg` | 服务端 → 接收方 | `MessageEvent` |
| `msg_ack` | 服务端 → 发送方 | `MessageAck` |
| `msg_read_ack` | 客户端 → 服务端 | `MessageReadAckReq` |
| `msg_read_notify` | 服务端 → 消息发送方 | `MessageReadAckEvent` |
| `room_msg_notice` | 服务端 → 房间成员 | `RoomMessageNotice` |

帧定义以 [ws.proto](backend/internal/transport/ws/ws.proto) 为准：

```protobuf
message WsFrame {
  string op = 1;
  bytes data = 2;
}
message WsBatch {
  repeated WsFrame frames = 1;
}
```

批量下发的外层 `WsFrame.op = "msg_batch"`，`data` 为 `WsBatch`，客户端按内部帧顺序分发。媒体消息发送请求携带 `file_id`，服务端读取已验证文件信息；消息事件携带 `attachment_id`，客户端另外查询媒体卡片和访问 URL。

### 已读水位

客户端发送 `MessageReadAckReq.message_id`。后端查询消息并校验会话权限，单调推进用户的 `lastReadSeq`；推进成功时在同一事务中写入已读 Outbox 事件，消费后通知对应消息发送方。重复或倒退的已读确认不重复推进水位。

### 离线补齐与实时同步

1. 请求 `/conversations` 获取会话快照，并保存 IndexedDB。
2. 对每个会话，从本地 `lastContinuousSeq` 到快照 `latestSeq` 分页请求 `/messages/offline`；不同会话并发补齐，消息与扫描水位原子写入 IndexedDB。
3. 快照补齐结束后建立 WebSocket，再追赶快照之后的新消息。断线恢复同样先补齐固定快照，不会无限追赶活跃会话。
4. 完整实时消息存在序号缺口或收到轻量通知时，通过 `/messages/sync` 拉取增量。
5. 页面历史消息优先查询 IndexedDB；后端仍提供独立 `/messages/history` 接口。

`lastReadSeq` 表示已读位置，`lastContinuousSeq` 表示本地已经补齐的消息位置，两者不能混用。

## Kafka 与持久化

| 事件 | 默认 Topic |
|---|---|
| 新消息 | `im.message` |
| 已读提交 | `im.read` |
| 好友申请 | `im.friend` |
| 房间成员变更 | `im.room` |
| 文件卡片预热 | `im.file-card-warmup` |

Producer 每次在短事务内领取一批 Outbox，提交领取事务后直接调用 SendMessages；Sarama 根据 Kafka Key 选择分区并组织发送。整批结果返回后，分类为 successMessages、retryMessages 和 deadMessages，分别执行批量更新：成功标记 sent，重试回到 pending 并保存各自的错误与重试时间，达到上限标记 dead；所有更新均校验每条记录的租约。不使用本地 Producer worker pool 或二次凑批。Consumer 内部负责 Inbox 抢占、处理、重试和死信。

Outbox/Inbox 表保留，它们不是独立的应用层调度入口。具体批量粒度和失败处理以 [producer.go](backend/internal/infrastructure/mq/kafka/producer.go) 与 [consumer.go](backend/internal/infrastructure/mq/kafka/consumer.go) 为准。

## 统一媒体上传

图片、视频和文件复用相同初始化与完成入口，服务端依据文件大小选择直传或 Multipart Upload。

```text
客户端计算 SHA-256 → 初始化上传
  ├─ 已完成：直接返回 fileId
  ├─ 直传：返回 uploadId + 预签名 PUT URL
  └─ 分片：返回 uploadId + 分片信息，按 partNumber 批量申请 URL
上传至 MinIO → 调用完成接口
  → 服务端校验大小、完整 Hash、MIME，以及分片信息
  → 事务持久化文件并更新上传任务，提交后更新文件缓存
  → 返回 completed + fileId → 发送媒体消息
```

未完成时返回 `uploading`、`uploadId`、`missingParts` 和 `invalidParts`，客户端修复后再次完成。初始化和完成具有对应锁及状态校验；后台 GC 清理过期上传与孤儿文件。发送媒体消息的事务另外写入文件卡片预热事件。对象存储内部地址与浏览器可访问的公开地址分别配置，预签名 URL 不能在签名后直接替换主机名。

## 摘要 Agent

摘要功能采用“HTTP 初始化任务 + gRPC 调用 Agent + SSE 获取结果”的组合方式：

- HTTP 负责鉴权、房间成员校验、任务状态管理和 SSE 连接
- gRPC 负责调用独立的 Room Summary Agent
- SSE 负责把已保存的摘要结果或 Agent 处理结果推送给前端
- MySQL 保存摘要任务的当前状态和最新响应，Redis 保存未读消息快照以及同一用户同一房间的任务绑定

### 摘要接口

| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/rooms/:roomId/summaries` | JWT | 初始化摘要任务，返回 `summaryRunId` |
| GET | `/rooms/:roomId/summaries/:summaryRunId/events` | JWT | 建立 SSE，接收摘要结果 |
| POST | `/rooms/:roomId/summaries/:summaryRunId/retry` | JWT | 对等待人工决策的任务执行重试 |
| POST | `/rooms/:roomId/summaries/:summaryRunId/cancel` | JWT | 取消等待人工决策的任务 |

### 摘要任务链路

```text
[前端]
   │
   │ POST /rooms/:roomId/summaries
   ▼
[HTTP Handler]
   │ 1. JWT 鉴权、校验房间成员
   │ 2. 读取用户未读范围（fromSeq ~ toSeq）
   │ 3. 查询消息，过滤已撤回消息
   ▼
[Redis scope 租约]
   │ key = userId + roomId，TTL 2 分钟，带 lockToken
   │ 已存在任务时直接复用原 summaryRunId
   ▼
[MySQL summary_runs]
   │ 创建 RUNNING 任务，保存消息序号范围
   ▼
[后台 goroutine]
   │ RunSummary(START + room_messages)
   ▼
[Agent gRPC]
   │ 返回 SUCCEEDED / WAITING_USER_DECISION / FAILED
   ▼
[MySQL]
   │ 保存最新 requestId、状态和 response_payload
   ▼
[SSE subscribers]
   │ 向同一个 summaryRunId 下的 SSE 连接广播结果
   ▼
[前端]
```

初始化接口返回 `202 Accepted` 和 `summaryRunId`。当前实现会在初始化成功后立即异步预热调用 Agent，前端随后建立 SSE；因此 SSE 建立时如果 Agent 已经完成，会先从 `summary_runs.response_payload` 回放最新结果，如果仍在执行，则等待内存订阅收到结果。

### SSE 行为

- SSE 建立时会再次校验 `summaryRunId` 的房间和用户归属，防止越权访问。
- 连接建立后先发送 `ready` 事件，随后每 15 秒发送注释心跳 `: ping`。
- Agent 完成后，后端先落 MySQL，再向当前任务的所有 SSE 订阅者广播。
- SSE 断开只会移除当前订阅，不会取消 Agent 任务；客户端可以使用同一个 `summaryRunId` 重新连接并读取已保存结果。
- 同一个任务允许多个设备建立 SSE，但它们消费的是同一条任务结果，不会各自生成摘要。

### 状态和用户决策

```text
RUNNING
  ├── SUCCEEDED
  ├── FAILED
  └── WAITING_USER_DECISION
          ├── RETRY  → RUNNING → Agent RunSummary(RETRY)
          └── CANCEL → FAILED  → Agent RunSummary(CANCEL)
```

`RETRY` 和 `CANCEL` 使用原来的 `summaryRunId`，不重新提交房间消息。后端在 MySQL 事务中使用 `SELECT ... FOR UPDATE` 锁定任务，并且只允许 `WAITING_USER_DECISION` 状态执行操作，提交状态变更后再异步调用 Agent。

### 持久化与幂等

`summary_runs` 当前保存：

- `summary_run_id`、`room_id`、`user_id`
- 任务状态和摘要使用的 `from_seq`、`to_seq`
- 最近一次 `request_id`
- Agent 返回的完整 `response_payload`
- 创建、更新时间和完成时间

Redis 主要保存两类数据：

- 未读摘要快照：`summary:active:room:unread:{userId}:{roomId}`，固定本次摘要的消息范围
- 任务 scope 租约：`summary:scope:run:{userId}:{roomId}`，保证同一个用户在同一个房间只绑定一个活动 `summaryRunId`

scope 租约由初始化请求创建，Agent 执行期间使用原 `lockToken` 按 TTL 的一半周期续期；初始化落库失败时立即释放，Agent 返回成功或失败后停止续期并等待 TTL 过期。Redis 丢失时，后端还会查询 MySQL 中 `RUNNING` 或 `WAITING_USER_DECISION` 的活动任务作为兜底。

### 当前 MVP 边界

当前实现保存的是 `summary_runs` 的最新响应，还没有独立的 `summary_events` 事件表和 `event_seq` 历史，因此 SSE 重连采用“回放最新结果”的方式，不是按 `Last-Event-ID` 补发多条历史事件。gRPC 客户端已具备 `ResumeSummaryState` 映射能力，后续可以在服务重启恢复、事件历史和断点续传场景中接入。

相关代码：

- 应用编排：[summary_application.go](backend/internal/application/agent/summary_application.go)
- HTTP/SSE：[handle.go](backend/internal/transport/http/agent/handle.go)、[router.go](backend/internal/transport/http/agent/router.go)
- gRPC 客户端：[client.go](backend/internal/infrastructure/agent/grpc/client.go)
- 摘要任务仓储：[summary.go](backend/internal/infrastructure/persistence/mysql/repository/summary/summary.go)
- Redis 快照与 scope 租约：[summary.go](backend/internal/infrastructure/persistence/redis/cache/summary/summary.go)

---

---

## HTTP 接口

以下路径统一以 `/api/v1` 为前缀；除注册、登录和刷新外，表中业务接口需要鉴权。

| 模块 | 方法 | 路径 | 用途 |
|---|---|---|---|
| 用户 | POST | `/users/register`、`/users/login`、`/users/refresh` | 注册、登录、刷新令牌 |
| 用户 | GET | `/users/resolve?keyword=...` | 按手机号或用户名查找 |
| 用户 | PATCH / POST | `/users/me` / `/users/logout` | 更新资料 / 退出 |
| 好友 | GET | `/friends`、`/friend-requests` | 好友及申请列表 |
| 好友 | POST | `/friend-requests`、`/friend-requests/actions` | 申请及处理 |
| 会话 | GET | `/conversations` | 会话快照 |
| 消息 | GET | `/messages/offline` | 固定快照内向前分页补齐 |
| 消息 | GET | `/messages/sync` | 实时增量补拉 |
| 消息 | GET | `/messages/history`、`/messages/seqs` | 历史分页 / 按序号补查 |
| 房间 | POST | `/rooms`、`/rooms/join`、`/rooms/:roomId/leave` | 创建、加入、退出 |
| 房间 | GET | `/rooms/:roomId/invite-code` | 邀请码 |
| 上传 | POST | `/files/uploads/init` | 统一初始化 |
| 上传 | POST | `/files/uploads/:uploadId/parts/presign` | 批量生成分片 URL |
| 上传 | POST | `/files/uploads/:uploadId/complete` | 统一完成校验 |
| 媒体 | POST | `/files/attachments/access-urls` | 批量查询媒体访问卡片 |
| 媒体 | GET | `/files/attachments/:attachmentId/access-url` | 单个媒体访问卡片 |

旧的 direct/multipart 独立初始化和完成路由已移除，不保留兼容入口。MySQL 自动迁移只对当前模型建表，不再执行历史表名、字段名及旧数据的自动转换。

## 本地验证

```bash
cd backend
go test ./...
```

```bash
cd frontend
npm test
npm run build
```

服务配置位于 `backend/configs/config.yaml`。协议定义、HTTP 路由及测试代码是接口行为的依据；压测结果需注明场景和环境，不能将同步接口响应耗时等同于消息端到端延迟。
