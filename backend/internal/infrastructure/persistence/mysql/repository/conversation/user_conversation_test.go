package conversation

import (
	"context"
	"strings"
	"testing"

	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestUpdateReadSeqBatchEmptyDoesNotAccessDatabase(t *testing.T) {
	if err := NewUserConversationRepository(nil).UpdateReadSeqBatch(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateReadSeqBatchUsesSingleMonotonicUpsert(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: "root@tcp(127.0.0.1:1)/im", SkipInitializeWithVersion: true,
	}), &gorm.Config{DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	calls := 0
	var statement string
	if err := db.Callback().Create().After("gorm:create").Register("capture_read_seq_batch", func(tx *gorm.DB) {
		calls++
		statement = tx.Statement.SQL.String()
	}); err != nil {
		t.Fatal(err)
	}

	err = NewUserConversationRepository(db).UpdateReadSeqBatch(context.Background(), []conversationrepo.ReadSeqUpdate{
		{UserID: "a", ConversationID: "room", LastReadSeq: 101},
		{UserID: "b", ConversationID: "room", LastReadSeq: 102},
		{UserID: "a", ConversationID: "room", LastReadSeq: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !strings.HasPrefix(statement, "INSERT INTO `user_conversations`") || strings.Count(statement, "),(") != 2 {
		t.Fatalf("watermarks must use one multi-row INSERT: calls=%d SQL=%s", calls, statement)
	}
	parts := strings.SplitN(statement, "ON DUPLICATE KEY UPDATE ", 2)
	if len(parts) != 2 || parts[1] != "`last_read_seq`=GREATEST(last_read_seq, VALUES(last_read_seq))" {
		t.Fatalf("conflicts must only advance the read watermark and preserve other fields: %s", statement)
	}
}
