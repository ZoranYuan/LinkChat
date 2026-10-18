package main

import (
	"IM_backend/configs"
	"IM_backend/internal/infrastructure/persistence/mysql"
	"flag"
	"log"
	"os"
)

func main() {
	eventDelivery := flag.Bool("event-delivery", false, "迁移 Outbox/Inbox 字段及事件载荷，执行前停止旧服务并处理旧格式 Kafka 积压")
	flag.Parse()
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/workspace/IM/backend/configs/config.yaml"
	}

	cfg := configs.LoadConfig(configPath)
	if err := cfg.Validate(); err != nil {
		log.Fatalf("配置校验失败：%v", err)
	}
	db := mysql.OpenMySQL(cfg.Database.MySQL.DSN)
	if *eventDelivery {
		if err := mysql.MigrateEventDelivery(db); err != nil {
			log.Fatalf("事件表迁移失败：%v", err)
		}
	}
	if err := mysql.Migrate(db); err != nil {
		log.Fatalf("数据库迁移失败：%v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("获取 MySQL 连接失败：%v", err)
	}
	if err := sqlDB.Close(); err != nil {
		log.Fatalf("关闭 MySQL 连接失败：%v", err)
	}
	log.Println("数据库迁移完成")
}
