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
	roomagentapp "IM_backend/internal/application/agent"
	conversationapp "IM_backend/internal/application/conversation"
	fileapp "IM_backend/internal/application/file"
	friendapp "IM_backend/internal/application/friend"
	messageapp "IM_backend/internal/application/message"
	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	roomapp "IM_backend/internal/application/room"
	userapp "IM_backend/internal/application/user"
	infracache "IM_backend/internal/infra/cache"
	agentcache "IM_backend/internal/infra/cache/agent"
	authcache "IM_backend/internal/infra/cache/auth"
	conversationcache "IM_backend/internal/infra/cache/conversation"
	filecache "IM_backend/internal/infra/cache/file"
	friendrelationcache "IM_backend/internal/infra/cache/friend"
	messagecache "IM_backend/internal/infra/cache/message"
	roomcache "IM_backend/internal/infra/cache/room"
	usercache "IM_backend/internal/infra/cache/user"
	"IM_backend/internal/infra/database"
	infraconversation "IM_backend/internal/infra/database/conversation"
	infrafile "IM_backend/internal/infra/database/file"
	infrafriend "IM_backend/internal/infra/database/friend"
	infrainbox "IM_backend/internal/infra/database/inbox"
	inframessage "IM_backend/internal/infra/database/message"
	infraroom "IM_backend/internal/infra/database/room"
	infrauser "IM_backend/internal/infra/database/user"
	"IM_backend/internal/infra/id/snowflake"
	"IM_backend/internal/infra/metrics"
	mqhandler "IM_backend/internal/infra/mq/handlers"
	"IM_backend/internal/infra/mq/kafka"
	"IM_backend/internal/infra/ratelimit"
	authsvc "IM_backend/internal/infra/security/auth"
	passwordsecurity "IM_backend/internal/infra/security/password"
	minioobj "IM_backend/internal/infra/storage/minio"
	grpcserver "IM_backend/internal/interfaces/grpc"
	roomagentgrpc "IM_backend/internal/interfaces/grpc/agent"
	userconversationhttp "IM_backend/internal/interfaces/http/conversation"
	filehttp "IM_backend/internal/interfaces/http/file"
	friendhttp "IM_backend/internal/interfaces/http/friend"
	friendrequesthttp "IM_backend/internal/interfaces/http/friendrequest"
	messagehttp "IM_backend/internal/interfaces/http/message"
	"IM_backend/internal/interfaces/http/middleware"
	monitoringhttp "IM_backend/internal/interfaces/http/monitoring"
	roomhttp "IM_backend/internal/interfaces/http/room"
	userhttp "IM_backend/internal/interfaces/http/user"
	"IM_backend/internal/interfaces/ws"
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
	roomCache := roomcache.NewRoomCache(redisClient, cfg)
	roomInfoCache := roomcache.NewRoomInfoCache(redisClient)
	roomUnreadSnapshotCache := agentcache.NewRoomUnreadSnapshotCache(redisClient)
	roomMemberCache := roomcache.NewRoomMemberCache(redisClient, cfg.Message)
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
	fileCleanupWorker := fileapp.NewCleanupWorker(
		txManager,
		fileUploadRepository,
		fileRepository,
		fileCache,
		objectStorage,
		cfg.FileCleanup,
		cfg.Storage.MinIO,
	)
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		fileCleanupWorker.Run(ctx)
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

	roomMemberRepository := infraroom.NewRoomMemberRepository(db)
	roomRepository := infraroom.NewRoomRepository(db)
	roomAgentApplication := roomagentapp.NewRoomAgentApplication(
		roomMemberRepository,
		conversationRepository,
		userConversationRepository,
		userRepository,
		messageRepository,
		roomMemberCache,
		cfg.Message,
		cfg.GRPC,
	)
	roomAgentHandler := roomagentgrpc.NewRoomAgentContextHandler(roomAgentApplication, tokenService, authCache)
	roomAgentGRPCServer, err := grpcserver.NewServer(cfg.GRPC.RoomAgentAddress, roomAgentHandler)
	if err != nil {
		log.Fatal("启动 Room Agent gRPC 监听失败：", err)
	}
	defer roomAgentGRPCServer.Stop()
	grpcServeErr := make(chan error, 1)
	go func() {
		grpcServeErr <- roomAgentGRPCServer.Serve()
	}()
	log.Printf("Room Agent gRPC 服务已启动：address=%s", cfg.GRPC.RoomAgentAddress)
	roomApp := roomapp.NewRoomApplication(
		roomRepository,
		roomMemberRepository,
		userConversationRepository,
		conversationRepository,
		roomCache,
		roomMemberCache,
		txManager,
		idGenerator,
		cfg.Room,
	)
	roomApp.SetRoomInfoCache(roomInfoCache, time.Duration(cfg.Cache.RoomInfo.TTLSeconds)*time.Second)
	roomApp.SetConversationApplication(conversationApplication)
	roomHandler := roomhttp.NewHandler(roomApp, conversationApplication)

	messageDelivery := mqhandler.NewMessageDelivery(
		realtimeGateway,
		cfg.Message,
	)
	defer messageDelivery.Close(context.Background())
	messageProducer := kafka.NewProducer(kafkaClient, cfg.Kafka.Topics.Message)
	dispatcher := ws.NewDispatcher()

	messageApplication := messageapp.NewMessageApplication(
		messageCache,
		roomMemberCache,
		roomCache,
		txManager,
		userConversationRepository,
		conversationRepository,
		friendRepository,
		friendRelationCache,
		fileRepository,
		fileCache,
		messageStickerRepository,
		messageRepository,
		roomMemberRepository,
		roomApp,
		idGenerator,
		cfg,
		roomUnreadSnapshotCache,
		messageAttachmentsRepository,
		messageProducer,
	)
	messageApplication.SetConversationApplication(conversationApplication)
	messageApplication.SetUserDelivery(realtimeGateway)
	messageSendHandler := mqhandler.NewMessageHandler(
		inboxRepository, txManager, cfg, conversationApplication, conversationRepository,
		messageRepository, userConversationRepository, fileRepository, fileCache,
		messageAttachmentsRepository, messageStickerRepository, idGenerator,
		roomCache, roomApp, messageDelivery, messageCache,
	)

	messageConsumer, err := kafka.NewConsumer(
		kafkaClient,
		[]string{cfg.Kafka.Topics.Message},
		messageSendHandler,
		cfg.Kafka.Consumer,
	)

	if err != nil {
		log.Fatal("创建消息消费组失败：", err)
	}

	// message 消费 worker
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := messageConsumer.Consume(ctx); err != nil && ctx.Err() == nil {
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
		roomApp,
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
	roomhttp.RegisterRoutes(apiGroup, roomHandler, authMiddle)
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
			log.Printf("Room Agent gRPC 服务异常退出：%v", err)
		}
		log.Printf("Room Agent gRPC 服务已停止，开始关闭后端")
	}

	log.Println("服务正在停止")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeoutSeconds)*time.Second)
	defer shutdownCancel()
	grpcStopDone := make(chan struct{})
	go func() {
		defer close(grpcStopDone)
		roomAgentGRPCServer.GracefulStop()
	}()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 HTTP 服务失败：%v", err)
	}
	select {
	case <-grpcStopDone:
	case <-shutdownCtx.Done():
		log.Printf("等待 Room Agent gRPC 请求优雅结束超时，强制停止")
		roomAgentGRPCServer.Stop()
		<-grpcStopDone
	}
	if err := realtimeGateway.Shutdown(shutdownCtx); err != nil {
		log.Printf("等待 WebSocket 入站分发结束失败：%v", err)
		cancel()
	}
	messageDelivery.Close(shutdownCtx)
	cancel()
	for _, done := range []<-chan struct{}{cleanupDone, consumerDone} {
		select {
		case <-done:
		case <-shutdownCtx.Done():
			log.Printf("等待后台任务退出超时")
		}
	}
	log.Println("服务已停止")
}
