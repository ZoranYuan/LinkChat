package main

import (
	"log"
	"os"

	"IM_backend/configs"
	"IM_backend/internal/infra/database"
	"IM_backend/internal/infra/database/migration"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/workspace/IM/backend/configs/config.yaml"
	}

	cfg := configs.LoadConfig(configPath)
	db := database.OpenMySQL(cfg.Database.MySQL)
	if err := migration.Migrate(db); err != nil {
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
