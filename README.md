# LinkChat

LinkChat 是独立开发的即时聊天系统，采用 Go 后端与 Vue 3 Web 客户端，支持好友管理、私聊、房间通信、离线消息同步和文件传输。普通聊天消息由 WebSocket 入口校验后同步发布到 Kafka，再由 Consumer 批量持久化并推送；大房间按广播成本切换为轻量通知与增量补拉。

项目同时提供 Room Agent 的 gRPC 对接入口，支持身份与房间访问校验、固定未读范围读取、按序号增量同步和区间消息搜索。未读消息接口在一次 gRPC 流调用中按批次返回，不提供客户端游标分页。

## 技术栈

Go、Gin、GORM、MySQL、Redis、Kafka、WebSocket、MinIO、Protobuf、gRPC、Vue 3、Pinia、IndexedDB、Vite。

## 代码组织

后端采用 Domain、Application、Infra、Interfaces 四层，按业务归拢代码。同一业务的主体与辅助方法集中组织，具有独立队列或生命周期的流程保留单独文件。

```text
backend/
  cmd/
    server/                    # 组装依赖、启动服务、协调退出
    migrate/                   # 数据库迁移入口
  configs/                     # 统一 Config 定义与 YAML 配置
  internal/
    domain/
      user/                    # 领域实体、仓储接口、值对象与业务错误
      friend/
      room/
      conversation/
      message/
      file/
    application/
      user/                    # 业务用例、DTO
      friend/
      room/
      conversation/
      message/                 # 发送校验、已读与同步
      file/                    # 上传、完成校验、附件访问、GC
      agent/                   # 房间权限与未读消息查询
      ports/                   # 跨模块共用的缓存、事务、消息和鉴权端口；消息契约统一放在 messaging/
    infra/
      database/                # MySQL 连接、事务管理与持久化实现
        user/                  # model.go、repository.go、converter.go
        friend/
        room/
        conversation/
        message/
        file/
        inbox/
        migration/             # GORM 模型迁移
      cache/                   # Redis 连接与业务缓存
      mq/
        kafka/                 # Producer、Consumer、Topic 路由
        handlers/              # 消费事件解码、调用应用用例
      security/                # JWT、令牌签发、密码哈希
      storage/minio/
      ratelimit/
      id/snowflake/
    interfaces/
      http/                    # HTTP 路由、中间件、Handler、请求和响应
      ws/                      # WebSocket 连接、消息分发、帧聚合
      grpc/
        server.go              # 监听、注册、启动、停止
        agent/                 # Room Agent RPC Handler
        pb/                    # Room Agent 协议与生成代码
```

领域模块内部直接组织实体、值对象和仓储契约；应用层编排用例并声明外部能力端口；基础设施实现这些端口；接口层处理协议适配。领域层不引用应用层及外层实现，应用层不引用 Infra 或 Interfaces；配置继续通过统一 Config 注入。

数据库模型与仓储实现放在同一业务目录，模型使用 `Model` 后缀并显式指定原表名；表字段、索引与接口 JSON 契约沿用现有定义。数据库迁移只由 `cmd/migrate` 调用。

文件 GC 位于应用层。Kafka Consumer 在 `infra/mq/kafka` 按会话键聚合消息批次，消费 Handler 位于 `infra/mq/handlers`；Producer、Consumer 与事件处理一起归属消息基础设施。

前端采用视图、组件、业务状态与服务分层：HTTP 客户端位于 `services/http`，WebSocket 客户端与协议位于 `services/websocket`，IndexedDB 连接和仓储位于 `db`，聊天业务编排位于 `modules/chat`。

Go 源码使用 gofmt 与统一导入分组；前端使用根目录 `.prettierrc.json`，缩进和换行遵循 `.editorconfig`。

## 消息链路

普通聊天消息采用 Kafka 前置、Consumer 批量持久化的链路：

```text
WebSocket message_send / MessageSendRequest
  → 校验会话及发送权限，Redis 原子登记请求指纹与消息 ID
  → 同步发送 Kafka（会话 ID 作为分区键）
  → Kafka 确认后返回 processing 状态的 message_send_result
  → Consumer 按会话聚合消息，批量抢占 Inbox
  → MySQL 事务：预留连续 seq、批量写入消息/附件、推进发送者已读水位、完成 Inbox
  → Redis Pipeline 回写 completed、seq 与附件 ID
  → WebSocket 向接收方投递完整消息，或广播合并后的房间轻量通知
  → Connection 出站聚合队列 → WebSocket 批量帧
```

WebSocket 入口不等待 MySQL 消息事务。Redis 用请求指纹识别冲突，处理中重试复用原消息 ID，完成或失败后直接返回缓存结果；处理态与终态 TTL 分别由配置控制。消息 ID 在发布 Kafka 前生成，并同时作为 Inbox 事件 ID。缓存失效后的重复请求由消息表唯一约束兜底，消费者恢复原消息及附件结果，不重复分配序号。Consumer 按 Kafka 分区键哈希到固定 worker，同一会话在同一 worker 内按批次处理；批次达到配置条数或定时窗口时提交。序号范围、消息数据、附件数据、发送者已读水位和 Inbox 完成状态在同一 MySQL 事务中提交。

应用层直接向 Publisher 传入事件 ID、事件类型、分区键和正文；Producer 使用配置的 Topic，并将 `event_id`、`event_type` 原样写入 Kafka Headers。Consumer 直接把 Sarama 消息交给消息处理器，处理器读取 Header 后将原始事件类型写入 Inbox，不再进行 Topic 与事件类型的双向映射。切换前需消费完不含 `event_type` Header 的旧积压消息。

Kafka 分区键使同一会话进入同一分区，消费端把不同会话分配给不同 worker 并行处理；每个分区按实际接收顺序跟踪完成状态，只标记连续完成的 offset 前缀。抢占冲突与事务错误退避重试，失败批次不会被后续完成的 offset 越过。消息序号在数据库会话行锁下分配，客户端按 seq 去重、排序并通过同步接口补齐缺口。

普通房间推送完整消息；成员规模较大或活跃度达到阈值的房间合并推送最新 seq 通知，客户端通过 `/messages/sync` 补拉。活跃度根据 Redis 中的近期消息量和在线连接数计算；近期消息缓存可用于补拉，缺失时回源 MySQL。通知合并器按会话保留最新 seq，定时批量触发通知。WebSocket 推送与 Inbox 完成标记不是同一个事务，客户端仍需去重和同步补偿。

## WebSocket 协议

当前使用单节点 Gateway，用户推送、房间广播和在线连接统计直接使用本地连接索引。

入口：`GET /api/v1/ws`，需要有效 JWT。Connection 绑定用户及登录会话，用户可以有多个连接。服务端维护 Ping/Pong、读写截止时间和慢客户端隔离。

每条 Connection 的入站分发按接收顺序执行。`message_send_result` 的 `processing` 表示 Kafka 已接收；`completed` 表示已持久化，携带原消息序号与附件 ID；`failed` 携带失败原因。消费者将处理结果写入 Redis，不再单独向发送者推送完整消息或失败结果；完成后的重复请求仍可直接取得 `completed` 结果。投递 Kafka 失败时返回失败结果，Redis 保留原 ID 的处理态供重试。发送状态查询接口及前端轮询作为待办，尚未接入；入站队列满时关闭连接。

| Op | 方向 | Payload |
|---|---|---|
| `message_send` | 客户端 → 服务端 | `MessageSendRequest` |
| `message_push` | 服务端 → 接收方 | `MessagePush` |
| `message_send_result` | 服务端 → 发送方 | `MessageSendResult` |
| `message_read` | 客户端 → 服务端 | `MessageReadRequest` |
| `message_read_notice` | 服务端 → 消息发送方 | `MessageReadNotice` |
| `room_message_notice` | 服务端 → 房间成员 | `RoomMessageNotice` |
| `room_enter` | 客户端 → 服务端 | `RoomEnterRequest` |
| `connection_visibility` | 客户端 → 服务端 | `ConnectionVisibilityRequest` |
| `friend_request_notice` | 服务端 → 客户端 | `FriendRequestNotice` |
| `error_notice` | 服务端 → 客户端 | `ErrorNotice` |
| `event_batch` | 服务端 → 客户端 | `EventBatch` |
| `event_batch_ack` | 客户端 → 服务端 | `EventBatchAck`，确认整个批次处理成功 |

帧定义以 [ws.proto](backend/internal/interfaces/ws/ws.proto) 为准：

```protobuf
message WsFrame {
  string op = 1;
  bytes data = 2;
}
message EventBatch {
  repeated WsFrame frames = 1;
  string batch_id = 2;
}
message EventBatchAck {
  string batch_id = 1;
}
```

批量下发的外层 `WsFrame.op = "event_batch"`，`data` 为 `EventBatch`。每个批次携带独立 `batch_id`，Connection 保存编码后的待确认批次，并在发送循环中每秒检查一次；超过 3 秒未确认时复用原 ID 和字节重发，最多额外重试 3 次，之后仍超时则删除该重试记录，连接继续运行。实际参数由 `ws.batch_retry_interval_seconds`、`ws.batch_ack_timeout_seconds` 和 `ws.batch_max_retries` 配置。重试耗尽后需要后续消息同步恢复，不能据此承诺客户端必达。

客户端按帧顺序触发业务回调，同一会话沿用串行队列；完整消息入库、房间通知补拉及其余回调全部成功后发送 `event_batch_ack`，任一失败则不确认。处理中收到同一批次会复用任务，已保存消息按本地水位跳过，避免重复增加未读数；确认仅发往接收该批次的当前连接，不代表用户已读。Kafka 内部发送事件名统一为 `message_send`，Topic 为 `im.messages`。媒体消息发送请求携带 `file_id`，服务端读取已验证文件信息；消息事件携带 `attachment_id`，客户端另外查询媒体卡片和访问 URL。

会话创建时生成独立 Snowflake ID，房间 ID 与会话 ID 不再等同。客户端使用 `/conversations` 返回的 ID，并在 `MessageSendRequest.conversation_id` 中传递；请求不再携带 `recv_id`、`conv_type`，接收者和类型由后端会话信息确定，发送权限仍单独校验。前后端需一起更新，不支持旧事件名称及发送请求。

会话基本信息使用 Redis Key `im:conversation:info:{conversationId}`，只保存类型、私聊双方或房间 ID，正常 TTL 由 `cache.conversation_info.ttl_seconds` 配置。未命中或缓存异常时按主键查 MySQL，确认不存在才缓存 `null` 两分钟；创建提交后写入正常信息覆盖空缓存。`latestSeq` 和用户已读水位不放入该缓存，已有会话 ID 继续按数据库记录使用，无需修改表结构或历史关联数据。

### 已读水位

客户端发送 `MessageReadRequest.message_id`。后端查询消息并校验会话权限，在事务中单调推进用户的 `lastReadSeq`；推进成功后通过本地 Gateway 通知对应消息发送方。重复或倒退的已读确认不重复推进水位。

私聊与房间共用已读确认。实时消息或补拉结果写入 IndexedDB 后，前端仅在目标会话正在打开、连续水位对应的消息不是本人发送时尝试发送回执；连接未建立或找不到边界消息时不发送。

### 房间会话与未读快照

一个房间对应一个公共会话，两名用户共享一个私聊会话。房间会话只填写房间标识，私聊会话填写双方用户标识；`user_conversations` 保存每个用户在公共会话中的已读水位、未读摘要范围和免打扰设置。

加入房间后，接口返回房间信息、成员角色、加入时间，以及 `conversationId` 和查询时的 `latestSeq`。前端仅更新目标会话，以返回的 `latestSeq` 初始化本地同步水位，再同步加入期间的新消息。

用户打开房间时发送 `room_enter`，后端在同一事务中固定从旧已读水位之后到当前最新序号的未读范围，并推进已读水位；没有未读时范围记为零。事务提交后将范围缓存到 Redis，数据库中的边界不随缓存过期而丢失。普通已读回执只推进已读水位，不修改快照；再次进入房间会覆盖上一次范围。快照保存的是序号边界，不是消息正文副本。

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

WebSocket 聊天事件由入口同步发布 Kafka。Consumer 通过 Inbox 管理事件处理状态，按会话归并批次，并在 MySQL 事务内完成消息写入与 Inbox 完成标记；不同会话可以并发处理。Kafka 确认、数据库提交、WebSocket 写入和客户端已读是不同阶段，任何一个阶段的成功都不代表后续阶段已经完成。

已读回执和好友申请通知在业务处理成功后通过本地 Gateway 直接推送。好友关系、会话和初始消息仍在同一数据库事务中创建，提交后向在线双方推送初始消息；离线用户通过会话同步读取。


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

未完成时返回 `uploading`、`uploadId`、`missingParts` 和 `invalidParts`，客户端修复后再次完成。初始化和完成具有对应锁及状态校验；后台 GC 清理过期上传与孤儿文件。发送媒体消息时保存附件与文件引用，文件卡片和访问 URL 由附件读取接口按需缓存：先批量校验权限，再读缓存，未命中时批量回源文件元信息、生成 URL 并回填；不再发布 Kafka 文件预热事件。对象存储内部地址与浏览器可访问的公开地址分别配置，预签名 URL 不能在签名后直接替换主机名。

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

## Room Agent 对接

gRPC 监听地址由 `grpc.room_agent_address` 配置，协议以 [room_agent.proto](backend/internal/interfaces/grpc/pb/room_agent.proto) 为准。调用方通过 metadata 的 `authorization: Bearer <access_token>` 传递身份；服务端验证 access token 和登录会话后，从令牌中取得用户 ID。

- `VerifyRoomAccess`：校验房间成员状态。正常及禁言成员允许访问，退出或被踢成员拒绝访问；成功时返回按用户和房间稳定生成的 `thread_id`。
- `GetRoomUnreadMessages`：读取用户进入房间时保存的未读序号范围。调用前需由客户端通过 WebSocket `room_enter` 触发未读快照；接口读取该固定范围，不重新计算或修改它。服务端按请求中的 `limit` 分批查询，并在同一次 gRPC server stream 中发送；没有快照时返回空结果。当前只返回未撤回的文本消息，接口不支持客户端传入分页 cursor。
- `SyncRoomMessages`：从 `after_seq` 之后同步至请求开始时的会话最新序号，按批次通过 gRPC stream 返回。
- `SearchRoomMessages`：按请求提供的序号区间查询房间消息，并通过 gRPC stream 分批返回。

## 本地启动

准备 MySQL、Redis、Kafka 和 MinIO，并根据实际部署修改 [config.yaml](backend/configs/config.yaml) 中的连接地址、凭据及对象存储公开地址。容器内的服务名需在运行后端的网络中能够解析。

后端首次运行前执行迁移，再启动服务：

```bash
cd backend
CONFIG_PATH=./configs/config.yaml go run ./cmd/migrate
CONFIG_PATH=./configs/config.yaml go run ./cmd/server
```

另开终端启动前端：

```bash
cd frontend
npm ci
npm run dev
```

## Docker 部署

项目已提供 [docker-compose.yml](docker-compose.yml)、[后端 Dockerfile](backend/Dockerfile) 和 [前端 Dockerfile](frontend/Dockerfile)。Compose 启动 MySQL、Redis、Kafka、MinIO、Go 后端及前端 Nginx；Nginx 将 `/api/` 请求和 WebSocket 连接转发到后端。以下命令在项目根目录执行，需要 Docker 与 Compose 插件。

### 部署配置

- 后端通过容器内的服务名访问依赖：`mysql:3306`、`redis:6379`、`kafka:9092`、`minio:9000`。配置在构建时从 `backend/configs/config.yaml` 复制到镜像中，修改后需要重新构建后端镜像。
- MinIO 的 `storage.minio.public_endpoint` 必须是浏览器可以访问的地址。本机部署可用 `http://127.0.0.1:9000`；远程部署应改成服务器地址或对象存储域名，不能使用容器服务名。
- 前端默认通过 `http://localhost:8080` 或 `http://127.0.0.1:8080` 访问。将实际前端来源加入 Compose 中 MinIO 的 `MINIO_API_CORS_ALLOW_ORIGIN`，并与后端 `security.allowed_origins` 保持一致。修改 Compose 环境变量后需要重建对应容器。

例如，本机部署时，MinIO 的来源配置可包含：

```yaml
MINIO_API_CORS_ALLOW_ORIGIN: "http://localhost:8080,http://127.0.0.1:8080,http://localhost:5173,http://127.0.0.1:5173"
```

### 启动依赖与执行迁移

Compose 使用名为 `common-bridge` 的外部网络，先确认网络存在：

```bash
docker network inspect common-bridge >/dev/null 2>&1 || docker network create common-bridge
docker compose -p linkchat up -d mysql redis kafka minio
docker compose -p linkchat logs -f mysql redis kafka minio
```

确认依赖启动完成后退出日志跟踪，再执行迁移。当前后端运行镜像仅包含服务二进制，迁移使用 Dockerfile 的 `builder` 阶段，其中保留了 Go 工具链和源码：

```bash
docker build --target builder -t linkchat-backend-builder ./backend
docker run --rm --network common-bridge \
  -e CONFIG_PATH=/src/backend/configs/config.yaml \
  linkchat-backend-builder go run ./cmd/migrate
```

数据库迁移需要单独执行，后端启动不会自动建表。Compose 的 `depends_on` 只指定启动顺序，当前未配置依赖就绪检查，因此应在依赖可用后执行迁移和启动应用。

### 启动应用与访问

```bash
docker compose -p linkchat up -d --build backend frontend
docker compose -p linkchat ps
```

| 服务 | 访问地址 |
|---|---|
| LinkChat Web | `http://localhost:8080` |
| HTTP API | `http://localhost:8081/api/v1` |
| WebSocket（经 Nginx） | `ws://localhost:8080/api/v1/ws` |
| MinIO 对象接口 | `http://localhost:9000` |
| MinIO 控制台 | `http://localhost:9001` |

Room Agent 的 gRPC 服务默认在容器内监听 `50051`，同一网络中的调用方可访问 `backend:50051`；当前 Compose 未发布该端口到宿主机，如需从宿主机调用，应在后端服务的 `ports` 中添加 `"50051:50051"`。

MySQL 和 MinIO 分别使用命名数据卷保存数据；当前 Compose 没有为 Redis、Kafka 配置数据卷。Room Agent 的 Python 服务由独立项目运行，不包含在本 Compose 中。

### 监控与压测

```bash
docker compose -p linkchat up -d kafka-exporter prometheus grafana
```

Prometheus 访问 `http://localhost:9090`，Grafana 访问 `http://localhost:3001`，初始账号和密码为 `admin / admin`。Grafana 已配置 Prometheus 数据源，将 [linkchat-overview.json](monitoring/grafana/dashboards/linkchat-overview.json) 导入即可查看出站等待、Kafka 积压、消费事务、Go 运行时和 MySQL Buffer Pool 指标。

[Prometheus 配置](monitoring/prometheus.yml) 默认抓取开发容器的 `linux:8081/metrics`；使用 Compose 后端时改为 `backend:8081`。k6 通过 Remote Write 将客户端 ACK、消息获取和补拉延迟写入 Prometheus，后端 `/metrics` 提供服务端指标，Kafka Exporter 提供消费组积压。

[房间负载脚本](benchmarks/room-load/k6-room-load.js) 使用独立账号定时发送，收到房间通知后异步补拉，覆盖通知目标后确认批次；[运行入口](benchmarks/room-load/run-k6.sh) 默认 300 人、每人每秒 1 条、持续 5 分钟。先使用 [账号准备脚本](benchmarks/room-load/prepare-fixture.mjs) 生成账号和房间数据，再运行入口脚本。账号令牌、fixture 和压测结果不提交到仓库。[帧聚合脚本](benchmarks/ws-outbound/k6-frame-aggregation.js) 用于比较不同聚合配置下的完整消息推送帧数与批量大小。

### 日志与停止

```bash
docker compose -p linkchat logs -f backend frontend
docker compose -p linkchat down
```

`down` 默认保留命名数据卷。以上命令无需添加删除数据卷的参数。

## 本地验证

```bash
cd backend
go build ./...
go vet ./...
```

前端类型与构建检查：

```bash
cd frontend
npm run typecheck
npm run build
```

服务配置位于 `backend/configs/config.yaml`。协议定义、HTTP 路由是接口行为的依据；压测结果需注明场景和环境，不能将同步接口响应耗时等同于消息端到端延迟。
