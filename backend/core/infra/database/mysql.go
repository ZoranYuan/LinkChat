package database

import (
	"log"
	"os"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"IM_backend/configs"
)

func OpenMySQL(config configs.MySQL) *gorm.DB {
	var logLevel logger.LogLevel
	switch strings.ToLower(strings.TrimSpace(config.LogLevel)) {
	case "silent":
		logLevel = logger.Silent
	case "error":
		logLevel = logger.Error
	case "warn", "warning":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	}
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // 输出位置
		logger.Config{
			SlowThreshold: time.Duration(config.SlowThresholdMilliseconds) * time.Millisecond,
			LogLevel:      logLevel,
			Colorful:      true,
		},
	)

	db, err := gorm.Open(mysql.Open(config.DSN), &gorm.Config{
		Logger: gormLogger,
	})

	if err != nil {
		log.Fatal("初始化 MySQL 失败：", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}

	sqlDB.SetMaxOpenConns(config.MaxOpenConns)                                           // 最大连接数（关键）
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)                                           // 空闲连接
	sqlDB.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetimeSeconds) * time.Second) // 连接复用时间

	return db
}
