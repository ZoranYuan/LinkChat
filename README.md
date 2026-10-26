# LinkChat

LinkChat 是独立开发的即时聊天系统，采用 Go 后端与 Vue 3 Web 客户端，支持好友管理、私聊、房间通信、离线消息同步和文件传输。消息链路采用事务化写入、Kafka 异步分发及按广播成本切换的推拉策略。

项目同时提供 Room Agent 的 gRPC 对接入口，目前已接入身份与房间访问权限校验；未读消息分页接口仍在开发中，尚未形成完整的摘要链路。

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
      user/                    # entity.go、repository.go、valueobject.go、errors.go
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
      message/                 # 发送、已读、同步、Sequencer、消息投递
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
        outbox/
        migration/             # GORM 模型迁移
      cache/                   # Redis 连接与业务缓存；房间本地状态位于 room/activity_memory.go
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

消息 Sequencer 和文件 GC 位于应用层。房间 ACTIVE 标识是节点内存缓存，集中在 `infra/cache/room/activity_memory.go`，通过应用端口访问。Kafka 的消费 Handler 位于 `infra/mq/handlers`，与 Producer/Consumer 一起归属消息基础设施。

前端采用视图、组件、业务状态与服务分层：HTTP 客户端位于 `services/http`，WebSocket 客户端与协议位于 `services/websocket`，IndexedDB 连接和仓储位于 `db`，聊天业务编排位于 `modules/chat`。

Go 源码使用 gofmt 与统一导入分组；前端使用根目录 `.prettierrc.json`，缩进和换行遵循 `.editorconfig`。

## 消息链路

```text
WebSocket MessageReq
  → 鉴权、关系校验和客户端重试检查
  → MySQL 事务：分配会话 seq、持久化消息与附件、写入 Outbox
  → 返回 msg_ack（sent 表示事务提交，不代表对方已接收）
  → Kafka Producer 批量领取 Outbox，以 PartitionKey 为 Kafka Key 直接批量投递
  → Kafka Consumer 分区内串行处理，使用 Inbox 管理幂等与处理租约
  → MessageDelivery / 本地 Gateway
  → Connection 出站聚合队列 → WebSocket 批量帧
```

发送请求进入按会话哈希的 Sequencer 分片，定时或容量触发批量处理；按会话分组后，在一个事务中预留连续 seq、批量写入消息、发送者已读水位和 Outbox。成功 ACK 在事务提交后返回。Kafka 分区键使相同会话进入同一分区，但不能单独保证多实例 Outbox 投递时的 seq 顺序；客户端仍需按 seq 去重、排序和补齐缺口。

普通房间推送完整消息；大型房间或活跃房间使用 `room_msg_notice` 合并通知，客户端通过 `/messages/sync` 补拉。房间新消息事务提交后记录 Redis 活跃度，达到 ACTIVE 时立即更新发送端与 Consumer 共享的本地标识；最后一次 ACTIVE 后保持 120 秒，Consumer 读取不续期，也不再查询 Redis 活跃度。大型房间、WARN/ACTIVE 房间及本地 ACTIVE 保持期间的新增消息继续预热，Redis 近期消息 TTL 仍为 10 分钟；缓存失败不改变成功 ACK。该标识仅在当前节点有效，重启后丢失，不是多实例共享状态。通知合并器每房间只保留最大 seq 对应的轻量通知，定时向所有在线成员推送，不保存完整消息或预热缓存。待通知房间数不设固定上限。房间判断和通知聚合参数位于 [config.yaml](backend/configs/config.yaml)。

聊天事件的 PartitionKey 使用会话 ID；已读事件使用用户 ID 与会话 ID，好友申请使用接收用户 ID，成员变更使用房间 ID 与用户 ID。Consumer 在每个 ConsumeClaim 中串行处理该分区；当前事件完成或转入死信并写入 Inbox 终态后，才标记对应 offset，再处理下一条。不同分区仍并行消费。投递目标取自业务事件字段，不从 Kafka Key 推断。WebSocket 推送与 Inbox 完成标记不是同一个事务，客户端仍需去重和同步补偿。

## WebSocket 协议

当前使用单节点 Gateway，用户推送、房间广播和在线连接统计直接使用本地连接索引。

入口：`GET /api/v1/ws`，需要有效 JWT。Connection 绑定用户及登录会话，用户可以有多个连接。服务端维护 Ping/Pong、读写截止时间和慢客户端隔离。

每条 Connection 的入站分发按接收顺序执行；消息发送处理器将任务提交至实例级 Sequencer，由分片统一凑批与持久化。批次使用服务生命周期上下文，单条连接断开不会取消其他连接的消息。客户端按 `clientMsgId` 关联 ACK、按 seq 排列消息；已提交但未收到 ACK 的消息使用原 clientMsgId 重试。入站队列满时关闭连接。

| Op | 方向 | Payload |
|---|---|---|
| `msg` | 客户端 → 服务端 | `MessageReq` |
| `msg` | 服务端 → 接收方 | `MessageEvent` |
| `msg_ack` | 服务端 → 发送方 | `MessageAck` |
| `msg_read_ack` | 客户端 → 服务端 | `MessageReadAckReq` |
| `msg_read_notify` | 服务端 → 消息发送方 | `MessageReadAckEvent` |
| `room_msg_notice` | 服务端 → 房间成员 | `RoomMessageNotice` |
| `room_entry` | 客户端 → 服务端 | `RoomEntryReq` |
| `connection_visibility` | 客户端 → 服务端 | `ConnectionVisibilityReq` |

帧定义以 [ws.proto](backend/internal/interfaces/ws/ws.proto) 为准：

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

会话创建时生成独立 Snowflake ID，房间 ID 与会话 ID 不再等同。客户端使用 `/conversations` 返回的 ID，并在 `MessageReq.conversation_id` 中传递；请求不再携带 `recv_id`、`conv_type`，接收者和类型由后端会话信息确定，发送权限仍单独校验。前后端需一起更新，不支持旧发送请求。

会话基本信息使用 Redis Key `im:conversation:info:{conversationId}`，只保存类型、私聊双方或房间 ID，正常 TTL 由 `cache.conversation_info.ttl_seconds` 配置。未命中或缓存异常时按主键查 MySQL，确认不存在才缓存 `null` 两分钟；创建提交后写入正常信息覆盖空缓存。`latestSeq` 和用户已读水位不放入该缓存，已有会话 ID 继续按数据库记录使用，无需修改表结构或历史关联数据。

### 已读水位

客户端发送 `MessageReadAckReq.message_id`。后端查询消息并校验会话权限，单调推进用户的 `lastReadSeq`；推进成功时在同一事务中写入已读 Outbox 事件，消费后通知对应消息发送方。重复或倒退的已读确认不重复推进水位。

私聊与房间共用已读确认。实时消息或补拉结果写入 IndexedDB 后，前端仅在目标会话正在打开、连续水位对应的消息不是本人发送时尝试发送回执；连接未建立或找不到边界消息时不发送。

### 房间会话与未读快照

一个房间对应一个公共会话，两名用户共享一个私聊会话。房间会话只填写房间标识，私聊会话填写双方用户标识；`user_conversations` 保存每个用户在公共会话中的已读水位、未读摘要范围和免打扰设置。

加入房间后，接口返回房间信息、成员角色、加入时间，以及 `conversationId` 和查询时的 `latestSeq`。目前前端加入后仍刷新工作区，尚未使用新增响应直接初始化本地同步水位。

用户打开房间时发送 `room_entry`，后端在同一事务中固定从旧已读水位之后到当前最新序号的未读范围，并推进已读水位；没有未读时范围记为零。事务提交后将范围缓存到 Redis，数据库中的边界不随缓存过期而丢失。普通已读回执只推进已读水位，不修改快照；再次进入房间会覆盖上一次范围。快照保存的是序号边界，不是消息正文副本。

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

Producer 每次在短事务内领取一批 Outbox，提交领取事务后直接调用 SendMessages；Sarama 根据 PartitionKey 选择分区并组织发送。整批结果返回后，分类为 sent、retry 和 dead，分别执行批量更新：成功标记 sent，重试回到 pending 并保存各自的错误与重试时间，达到上限标记 dead；所有更新均校验每条记录的 event_id 与 lock_token。不使用本地 Producer worker pool 或二次凑批。Consumer 内部负责 Inbox 抢占、处理、重试和死信。

Outbox/Inbox 表保留，它们不是独立的应用层调度入口。具体批量粒度和失败处理以 [producer.go](backend/internal/infra/mq/kafka/producer.go) 与 [consumer.go](backend/internal/infra/mq/kafka/consumer.go) 为准。

Outbox 保存 `event_id`、`event_type`、`partition_key` 和完整业务 JSON `payload`，不再嵌套 Envelope。Inbox 保存处理状态，`attempt_count` 在首次抢占时为 1；Consumer 的 `max_attempts: 3` 表示首次尝试加最多两次重试。Inbox 的 `finished_at` 仅在 completed/dead 时填写，created_at/updated_at 分别记录创建和更新。Outbox 的 sent 表示 Kafka 确认，Inbox 的 completed 表示 Handler 成功返回，都不是客户端已收到的证明。


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

gRPC 监听地址由 `grpc.room_agent_address` 配置，协议以 [room_agent.proto](backend/internal/interfaces/grpc/pb/room_agent.proto) 为准。调用方通过 metadata 的 `authorization: Bearer <access_token>` 传递身份，用户标识由服务端解析，并校验登录会话是否仍有效。

- `VerifyRoomAccess`：校验房间成员状态，正常及禁言成员允许访问，退出或被踢成员拒绝访问；成功时返回按用户和房间稳定生成的 `thread_id`。
- `GetRoomUnreadMessages`：协议已定义分页大小和游标，当前尚未实现 RPC Handler；应用层方法也保留明确的未实现返回。

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
