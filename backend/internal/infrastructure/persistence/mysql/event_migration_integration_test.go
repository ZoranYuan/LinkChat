package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"IM_backend/configs"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/infrastructure/persistence/mysql/model"
	inboxrepo "IM_backend/internal/infrastructure/persistence/mysql/repository/inbox"
	"IM_backend/internal/shared/protocol"
	driver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestEventDeliveryMigrationAndInboxLifecycleOnMySQL(t *testing.T) {
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	if configPath := os.Getenv("IM_EVENT_TEST_MYSQL_CONFIG"); configPath != "" {
		dsn = configs.LoadConfig(configPath).Database.MySQL.DSN
	}
	if dsn == "" {
		t.Skip("未配置 MySQL 集成测试连接")
	}
	connection, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal("MySQL 测试连接配置无效")
	}
	connection.ParseTime = true
	connection.Timeout = 3 * time.Second
	connection.ReadTimeout = 10 * time.Second
	connection.WriteTimeout = 10 * time.Second
	db, err := gorm.Open(mysql.Open(connection.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("连接 MySQL 测试服务失败：%v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	tablePrefix := fmt.Sprintf("im_event_migration_test_%d", time.Now().UnixNano())
	outboxTable, inboxTable := tablePrefix+"_outboxes", tablePrefix+"_inboxes"
	// 测试只操作唯一前缀的临时表，结束时删除测试表，不修改现有业务表。
	t.Cleanup(func() {
		if err := db.Migrator().DropTable(outboxTable, inboxTable); err != nil {
			t.Errorf("清理临时测试表失败：%v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	testDB := db.WithContext(ctx)

	for _, statement := range []string{
		`CREATE TABLE ` + outboxTable + ` (
			id VARCHAR(32) PRIMARY KEY, event_type VARCHAR(64), message_key VARCHAR(128), payload LONGBLOB,
			status VARCHAR(16) NOT NULL DEFAULT 'pending', retry_count INT NOT NULL DEFAULT 0,
			next_retry_at DATETIME(3) NOT NULL, locked_at DATETIME(3) NULL,
			lock_token VARCHAR(64) NOT NULL DEFAULT '', last_error TEXT, sent_at DATETIME(3) NULL,
			created_at DATETIME(3), updated_at DATETIME(3))`,
		`CREATE TABLE ` + inboxTable + ` (
			event_id VARCHAR(64) PRIMARY KEY, event_type VARCHAR(64) NOT NULL,
			status VARCHAR(16) NOT NULL DEFAULT 'completed', locked_at DATETIME(3) NULL,
			lock_token VARCHAR(64) NOT NULL DEFAULT '', retry_count INT NOT NULL DEFAULT 0,
			last_error TEXT, processed_at DATETIME(3) NOT NULL)`,
	} {
		if err := testDB.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	readPayload, _ := json.Marshal(protocol.MessageReadCommittedEvent{ReaderId: "a", ConversationId: "c", LastReadSeq: 101, ConvType: protocol.PrivateChat})
	stored, _ := json.Marshal(struct {
		From, To string
		Payload  []byte
	}{To: "b", Payload: readPayload})
	for _, row := range []struct {
		id, eventType string
		payload       []byte
	}{
		{"event-read", protocol.EventReadMessageCommitted, stored},
		{"event-cache", "file_card_warmup", []byte(`{"obsolete":true}`)},
	} {
		if err := testDB.Exec("INSERT INTO "+outboxTable+" (id, event_type, message_key, payload, next_retry_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", row.id, row.eventType, "key", row.payload, now, now, now).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, status := range []string{"processing", "completed", "dead"} {
		if err := testDB.Exec("INSERT INTO "+inboxTable+" (event_id, event_type, status, retry_count, processed_at) VALUES (?, ?, ?, ?, ?)", status, protocol.EventTypeSendMessage, status, 2, now).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := migrateEventDeliveryTables(testDB, outboxTable, inboxTable); err != nil {
			t.Fatalf("迁移第 %d 次失败：%v", i+1, err)
		}
	}
	if err := verifyPendingEventFormat(testDB, outboxTable); err != nil {
		t.Fatal(err)
	}
	if err := testDB.Table(outboxTable).Where("event_id = ?", "event-read").Update("payload", stored).Error; err != nil {
		t.Fatal(err)
	}
	if err := verifyPendingEventFormat(testDB, outboxTable); err == nil {
		t.Fatal("旧字段已重命名但载荷尚未转换时，必须阻止启动")
	}
	if err := migrateEventDeliveryTables(testDB, outboxTable, inboxTable); err != nil {
		t.Fatal(err)
	}
	if err := verifyPendingEventFormat(testDB, outboxTable); err != nil {
		t.Fatal(err)
	}
	for _, column := range [][2]string{{outboxTable, "id"}, {outboxTable, "message_key"}, {inboxTable, "retry_count"}, {inboxTable, "processed_at"}} {
		if testDB.Migrator().HasColumn(column[0], column[1]) {
			t.Fatalf("旧字段未删除：%v", column)
		}
	}
	var outboxes []model.OutboxRecord
	if err := testDB.Table(outboxTable).Order("event_id").Find(&outboxes).Error; err != nil {
		t.Fatal(err)
	}
	if len(outboxes) != 2 || outboxes[0].EventID != "event-cache" || outboxes[0].Status != outboxport.StatusDead || outboxes[0].LastError == "" || outboxes[1].EventID != "event-read" || outboxes[1].PartitionKey != "key" || outboxes[1].Status != outboxport.StatusPending {
		t.Fatalf("事件标识、分区键或状态未保留：%+v", outboxes)
	}
	var event protocol.MessageReadCommittedEvent
	if err := json.Unmarshal(outboxes[1].Payload, &event); err != nil || event.RecipientUserId != "b" || event.LastReadSeq != 101 {
		t.Fatalf("已读事件未正确转换：%+v err=%v", event, err)
	}
	var inboxes []model.InboxRecord
	if err := testDB.Table(inboxTable).Find(&inboxes).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range inboxes {
		if row.AttemptCount != 2 || row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || (row.FinishedAt == nil) != (row.Status == "processing") {
			t.Fatalf("历史计数和时间语义错误：%+v", row)
		}
	}

	inboxDB := testDB.Table(inboxTable)
	repository := inboxrepo.NewInboxRepository(inboxDB)
	var token string
	for attempt := 1; attempt <= 2; attempt++ {
		if err := inboxDB.Transaction(func(tx *gorm.DB) error {
			claimed, currentToken, count, err := repository.WithTx(tx).TryClaim(ctx, "new-event", protocol.EventTypeSendMessage, time.Now(), time.Now().Add(-time.Minute))
			if err != nil {
				return err
			}
			if !claimed || count != attempt {
				return fmt.Errorf("尝试计数错误：claimed=%t count=%d", claimed, count)
			}
			token = currentToken
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var row model.InboxRecord
		if err := inboxDB.First(&row, "event_id = ?", "new-event").Error; err != nil || row.FinishedAt != nil || row.AttemptCount != attempt {
			t.Fatalf("处理中不能填写结束时间：%+v err=%v", row, err)
		}
		if attempt == 1 {
			if err := repository.MarkRetry(ctx, "new-event", token, "temporary"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := repository.RenewBatch(ctx, []string{"new-event"}, token, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkCompleted(ctx, "new-event", token, time.Now()); err != nil {
		t.Fatal(err)
	}
	var completed model.InboxRecord
	if err := inboxDB.First(&completed, "event_id = ?", "new-event").Error; err != nil || completed.Status != "completed" || completed.FinishedAt == nil || completed.AttemptCount != 2 || completed.LockToken != "" || completed.LockedAt != nil {
		t.Fatalf("完成状态或租约未正确更新：%+v err=%v", completed, err)
	}
}
