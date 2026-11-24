package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	"IM_backend/configs"
	groupagentapp "IM_backend/core/application/agent"
	conversationapp "IM_backend/core/application/conversation"
	fileapp "IM_backend/core/application/file"
	friendapp "IM_backend/core/application/friend"
	messageapp "IM_backend/core/application/message"
	ratelimitport "IM_backend/core/application/ports/ratelimit"
	groupapp "IM_backend/core/application/group"
	userapp "IM_backend/core/application/user"
	infracache "IM_backend/core/infra/cache"
	agentcache "IM_backend/core/infra/cache/agent"
	authcache "IM_backend/core/infra/cache/auth"
	conversationcache "IM_backend/core/infra/cache/conversation"
	filecache "IM_backend/core/infra/cache/file"
	friendrelationcache "IM_backend/core/infra/cache/friend"
	messagecache "IM_backend/core/infra/cache/message"
	groupcache "IM_backend/core/infra/cache/group"
	usercache "IM_backend/core/infra/cache/user"
	"IM_backend/core/infra/database"
	infraconversation "IM_backend/core/infra/database/conversation"
	infrafile "IM_backend/core/infra/database/file"
	infrafriend "IM_backend/core/infra/database/friend"
	infrainbox "IM_backend/core/infra/database/inbox"
	inframessage "IM_backend/core/infra/database/message"
	infragroup "IM_backend/core/infra/database/group"
	infrauser "IM_backend/core/infra/database/user"
	"IM_backend/core/infra/id/snowflake"
	"IM_backend/core/infra/metrics"
	mqhandler "IM_backend/core/infra/mq/handlers"
	messagesend "IM_backend/core/infra/mq/handlers/message/send"
	messageread "IM_backend/core/infra/mq/handlers/message/read"
	"IM_backend/core/infra/mq/kafka"
	"IM_backend/core/infra/ratelimit"
	authsvc "IM_backend/core/infra/security/auth"
	passwordsecurity "IM_backend/core/infra/security/password"
	minioobj "IM_backend/core/infra/storage/minio"
	grpcserver "IM_backend/core/interfaces/grpc"
	groupagentgrpc "IM_backend/core/interfaces/grpc/agent"
	userconversationhttp "IM_backend/core/interfaces/http/conversation"
	filehttp "IM_backend/core/interfaces/http/file"
	friendhttp "IM_backend/core/interfaces/http/friend"
	friendrequesthttp "IM_backend/core/interfaces/http/friendrequest"
	messagehttp "IM_backend/core/interfaces/http/message"
	"IM_backend/core/interfaces/http/middleware"
	monitoringhttp "IM_backend/core/interfaces/http/monitoring"
	grouphttp "IM_backend/core/interfaces/http/group"
	userhttp "IM_backend/core/interfaces/http/user"
	"IM_backend/core/interfaces/ws"
)

func main() {
	r := gin.Default()
	r.Use(middleware.ErrorLoggerMiddleware())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/workspace/IM/backend/configs/config.yaml"
	}
	cfg := configs.LoadConfig(configPath)

	if cfg.App.Env == "development" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	redisClient := infracache.InitRedis(cfg.Database.Redis.DSN)
	defer redisClient.Close()
	db := database.OpenMySQL(cfg.Database.MySQL)
	dbSQL, err := db.DB()
	if err != nil {
		log.Fatal("获取数据库连接失败：", err)
	}
	defer dbSQL.Close()
	prometheus.MustRegister(metrics.NewMySQLCollector(dbSQL, time.Duration(cfg.Server.HealthTimeoutSeconds)*time.Second))

	txManager := database.NewTransactionManager(db)
	idGenerator, err := snowflake.NewGenerator(int(cfg.App.MachineID))
	if err != nil {
		log.Fatalln("初始化 ID 生成器失败：", err)
	}
	passwordHasher := passwordsecurity.NewHasher(cfg.Security)

	realtimeGateway := ws.NewGateway(ctx)
	realtimeGateway.SweepHiddenConnections(
		realtimeGateway.NetworkContext(),
		time.Duration(cfg.WebSocket.TimerInterval)*time.Second,
		time.Duration(cfg.WebSocket.PageHiddenTimeoutSeconds)*time.Second,
	)

	authCache := authcache.NewAuthCache(redisClient)
	userProfileCache := usercache.NewProfileCache(redisClient)
	groupCache := groupcache.NewGroupCache(redisClient, cfg)
	groupInfoCache := groupcache.NewGroupInfoCache(redisClient)
	groupUnreadSnapshotCache := agentcache.NewGroupUnreadSnapshotCache(redisClient)
	groupMemberCache := groupcache.NewGroupMemberCache(redisClient, cfg.Message)
	fileCache := filecache.NewFileCache(redisClient)
	messageCache := messagecache.NewMessageCache(redisClient)

	kafkaClient, err := kafka.NewClient(cfg.Kafka)

	if err != nil {
		log.Fatalln("连接 Kafka 失败：", err)
	}
	defer func() {
		if err := kafkaClient.Close(); err != nil {
			log.Printf("关闭消息队列客户端失败：%v", err)
		}
	}()

	tokenService := authsvc.NewTokenService(cfg.JWT)

	objectStorage, err := minioobj.NewObjectStorage(ctx, cfg.Storage.MinIO)
	if err != nil {
		log.Fatalln("连接 MinIO 失败：", err)
	}

	r.GET("/livez", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/readyz", func(c *gin.Context) {
		checkCtx, cancel := context.WithTimeout(c.Request.Context(), time.Duration(cfg.Server.HealthTimeoutSeconds)*time.Second)
		defer cancel()
		checks := gin.H{}
		if err := dbSQL.PingContext(checkCtx); err != nil {
			checks["mysql"] = err.Error()
		}
		if err := redisClient.Ping(checkCtx).Err(); err != nil {
			checks["redis"] = err.Error()
		}
		if err := objectStorage.Health(checkCtx); err != nil {
			checks["minio"] = err.Error()
		}
		if len(checks) > 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "checks": checks})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	// 构造依赖
	fileRepository := infrafile.NewFileRepository(db)
	fileUploadRepository := infrafile.NewFileUploadRepository(db)
	messageAttachmentsRepository := inframessage.NewMessageAttachmentRepository(db)
	fileApplication := fileapp.NewFileApplication(cfg, fileRepository, fileUploadRepository, messageAttachmentsRepository, fileCache, objectStorage, idGenerator, txManager)

	// file 清除 worker
	fileDeleteOutdatedData := fileapp.NewDeleteOutdatedData(
		fileRepository,
		objectStorage,
		cfg.FileCleanup,
	)
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		fileDeleteOutdatedData.Run(ctx)
	}()
	fileHandler := filehttp.NewHandler(fileApplication)

	messageRepository := inframessage.NewMessageRepository(db)
	messageStickerRepository := inframessage.NewMessageStickerRepository(db)
	inboxRepository := infrainbox.NewInboxRepository(db)
	conversationRepository := infraconversation.NewConversationRepository(db)
	conversationInfoCache := conversationcache.NewConversationCache(redisClient, cfg.Cache.ConversationInfo)
	conversationApplication := conversationapp.NewConversationApplication(conversationRepository, conversationInfoCache, time.Duration(cfg.Cache.ConversationInfo.TTLSeconds)*time.Second)
	userConversationRepository := infraconversation.NewUserConversationRepository(db)

	userRepository := infrauser.NewUserRepository(db)
	userApp := userapp.NewUserApplication(userRepository, userProfileCache, cfg.JWT, authCache, tokenService, idGenerator, passwordHasher, txManager)
	userHandler := userhttp.NewHandler(
		userApp,
		cfg.JWT,
		realtimeGateway,
	)

	friendRepository := infrafriend.NewFriendRepository(db)
	friendRequestRepository := infrafriend.NewFriendRequestRepository(db)
	friendRelationCache := friendrelationcache.NewRelationCache(redisClient)
	friendRequestApp := friendapp.NewFriendRequestApplication(
		friendRequestRepository,
		userRepository,
		messageRepository,
		conversationRepository,
		userConversationRepository,
		friendRepository,
		txManager,
		idGenerator,
		friendRelationCache,
		cfg.Friend,
		time.Duration(cfg.Cache.FriendRelation.TTLSeconds)*time.Second,
	)
	friendRequestHandler := friendrequesthttp.NewHandler(friendRequestApp)
	friendRequestApp.SetConversationApplication(conversationApplication)
	friendRequestApp.SetUserDelivery(realtimeGateway)

	friendApp := friendapp.NewFriendApplication(friendRepository, userRepository)
	friendHandler := friendhttp.NewHandler(friendApp)

	groupMemberRepository := infragroup.NewGroupMemberRepository(db)
	groupRepository := infragroup.NewGroupRepository(db)
	groupAgentApplication := groupagentapp.NewGroupAgentApplication(
		groupMemberRepository,
		conversationRepository,
		userConversationRepository,
		userRepository,
		messageRepository,
		groupMemberCache,
		cfg.Message,
		cfg.GRPC,
	)
	groupAgentHandler := groupagentgrpc.NewGroupAgentContextHandler(groupAgentApplication, tokenService, authCache)
	groupAgentGRPCServer, err := grpcserver.NewServer(cfg.GRPC.GroupAgentAddress, groupAgentHandler)
	if err != nil {
		log.Fatal("启动 Group Agent gRPC 监听失败：", err)
	}
	defer groupAgentGRPCServer.Stop()
	grpcServeErr := make(chan error, 1)
	go func() {
		grpcServeErr <- groupAgentGRPCServer.Serve()
	}()
	log.Printf("Group Agent gRPC 服务已启动：address=%s", cfg.GRPC.GroupAgentAddress)
	groupApp := groupapp.NewGroupApplication(
		groupRepository,
		groupMemberRepository,
		userConversationRepository,
		conversationRepository,
		groupCache,
		groupMemberCache,
		txManager,
		idGenerator,
		cfg.Group,
	)
	groupApp.SetGroupInfoCache(groupInfoCache, time.Duration(cfg.Cache.GroupInfo.TTLSeconds)*time.Second)
	groupApp.SetConversationApplication(conversationApplication)
	groupHandler := grouphttp.NewHandler(groupApp, conversationApplication)

	messageDelivery := mqhandler.NewMessageDelivery(
		realtimeGateway,
		cfg.Message,
	)
	defer messageDelivery.Close(context.Background())
	messageProducer := kafka.NewProducer(kafkaClient)
	deadLetterWriter := kafka.NewDeadLetterWriter(messageProducer)
	dispatcher := ws.NewDispatcher()

	messageApplication := messageapp.NewMessageApplication(
		messageCache,
		groupMemberCache,
		groupCache,
		txManager,
		userConversationRepository,
		conversationRepository,
		friendRepository,
		friendRelationCache,
		fileRepository,
		fileCache,
		messageStickerRepository,
		messageRepository,
		groupMemberRepository,
		groupApp,
		cfg,
		groupUnreadSnapshotCache,
		messageAttachmentsRepository,
		messageProducer,
		inboxRepository,
	)
	messageApplication.SetConversationApplication(conversationApplication)
	messageApplication.SetUserDelivery(realtimeGateway)
	messageSendHandler := messagesend.NewMessageHandler(
		ctx, inboxRepository, txManager, cfg, conversationApplication, conversationRepository,
		messageRepository, userConversationRepository, fileRepository, fileCache,
		messageAttachmentsRepository, messageStickerRepository, idGenerator,
		groupCache, groupApp, messageDelivery, messageCache, messageProducer,
	)
	messageRedisFillHandler := messagesend.NewMessageRedisFillHandler(messageCache, cfg)
	messageTransferHandler := messagesend.NewMessageTransferHandler(conversationApplication, messageDelivery, groupCache, cfg)
	readReceiptHandler := messageread.NewReadReceiptHandler(
		cfg, inboxRepository, txManager, messageRepository,
		conversationRepository, userConversationRepository, realtimeGateway,
	)

	consumerCtx, stopConsume := context.WithCancel(ctx)

	messageConsumer, err := kafka.NewConsumer(
		kafkaClient,
		[]string{
			cfg.Kafka.Topics.MessageSendPersist,
			cfg.Kafka.Topics.MessageSendCache,
			cfg.Kafka.Topics.MessageSendPush,
			cfg.Kafka.Topics.MessageReadReceipt,
		},
		[]kafka.EventHandler{messageSendHandler, messageRedisFillHandler, messageTransferHandler, readReceiptHandler},
		cfg.Kafka.Consumer,
		deadLetterWriter,
	)

	if err != nil {
		log.Fatal("创建消息消费组失败：", err)
	}

	// message 消费 worker
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := messageConsumer.Consume(consumerCtx); err != nil && consumerCtx.Err() == nil {
			log.Printf("消息队列消费者已停止：%v", err)
		}
	}()

	messageHandler := messagehttp.NewHandler(messageApplication, cfg.Message)

	wsHandler := ws.NewHandler(
		messageApplication,
		cfg,
		dispatcher,
		realtimeGateway,
	)

	userConversationApplication := conversationapp.NewUserConversationApplication(
		userConversationRepository,
		conversationRepository,
		messageRepository,
		friendRepository,
		userRepository,
		userProfileCache,
		groupApp,
	)

	userConversationHandler := userconversationhttp.NewHandler(userConversationApplication)

	// 注册中间件
	authMiddle := middleware.NewAuthMiddleware(cfg, authCache, tokenService)

	limiter := ratelimit.NewRedisLimiter(redisClient, cfg.Security.RateLimitPrefix)
	limiterMiddleware := middleware.NewRateLimitMiddleware(limiter, false)
	wsHandler.SetLimiter(limiter)

	// 注册路由
	monitoringhttp.RegisterRoutes(r)

	apiGroup := r.Group("/api/v1")
	apiGroup.Use(authMiddle.CookieOriginProtectionMiddleware())
	friendrequesthttp.RegisterRoutes(apiGroup, friendRequestHandler, authMiddle)
	userAPIRateLimit := ratelimitport.Policy{
		Rate:  cfg.HTTP.UserAPIRateLimit.RatePerSecond,
		Burst: cfg.HTTP.UserAPIRateLimit.Burst,
	}
	userhttp.RegisterRoutes(apiGroup, userHandler, authMiddle, limiterMiddleware, userAPIRateLimit)
	userconversationhttp.RegisterRoutes(apiGroup, userConversationHandler, authMiddle)
	friendhttp.RegisterRoutes(apiGroup, friendHandler, authMiddle)
	grouphttp.RegisterRoutes(apiGroup, groupHandler, authMiddle)
	messagehttp.RegisterRoutes(apiGroup, messageHandler, authMiddle)
	filehttp.RegisterRoutes(apiGroup, fileHandler, authMiddle, limiterMiddleware, ratelimitport.Policy{
		Rate:  cfg.Storage.MinIO.UploadRate,
		Burst: cfg.Storage.MinIO.UploadBurst,
	})
	ws.RegisterWSRouter(apiGroup, wsHandler, authMiddle)

	srv := &http.Server{
		Addr:        cfg.Server.Port,
		Handler:     r,
		ReadTimeout: time.Duration(cfg.Server.ReadTimeoutSeconds) * time.Second,
		// SSE 和 WebSocket 都是长连接，写超时由各自的连接心跳/写 deadline 控制。
		WriteTimeout: 0,
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeoutSeconds) * time.Second,
		// 关键：最大连接数限制
		MaxHeaderBytes: cfg.Server.MaxHeaderBytes,
	}

	defer srv.Close()

	log.Println("服务已启动")

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("启动服务失败：", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
	case err := <-grpcServeErr:
		if err != nil {
			log.Printf("Group Agent gRPC 服务异常退出：%v", err)
		}
		log.Printf("Group Agent gRPC 服务已停止，开始关闭后端")
	}

	log.Println("服务正在停止")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeoutSeconds)*time.Second)
	defer shutdownCancel()
	grpcStopDone := make(chan struct{})
	go func() {
		defer close(grpcStopDone)
		groupAgentGRPCServer.GracefulStop()
	}()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 HTTP 服务失败：%v", err)
	}
	select {
	case <-grpcStopDone:
	case <-shutdownCtx.Done():
		log.Printf("等待 Group Agent gRPC 请求优雅结束超时，强制停止")
		groupAgentGRPCServer.Stop()
		<-grpcStopDone
	}
	// 关闭顺序：先停 Kafka 消费（停止入队）→ 等消费循环退出 → drain 攒批器（flush 在途消息）→
	// 再关 WS 通道。这样在途的推送能在网关仍存活时送达，避免消息已落库但推送丢失。
	stopConsume()
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		log.Printf("等待消息消费循环退出超时")
	}
	messageSendHandler.Drain()
	messageDelivery.Close(shutdownCtx)
	if err := realtimeGateway.Shutdown(shutdownCtx); err != nil {
		log.Printf("等待 WebSocket 入站分发结束失败：%v", err)
	}
	cancel()
	select {
	case <-cleanupDone:
	case <-shutdownCtx.Done():
		log.Printf("等待后台清理任务退出超时")
	}
	log.Println("服务已停止")
}
