package outbox

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	outboxport "IM_backend/internal/application/ports/outbox"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestMarkSentBatchRejectsInvalidLeases(t *testing.T) {
	repository := NewOutboxRepository(nil, nil)
	if err := repository.MarkSentBatch(context.Background(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, leases := range [][]outboxport.Lease{
		{{EventID: "a"}},
		{{LockToken: "token"}},
		{{EventID: "a", LockToken: "old"}, {EventID: "a", LockToken: "new"}},
	} {
		if err := repository.MarkSentBatch(context.Background(), leases, time.Now()); !errors.Is(err, outboxport.ErrLeaseLost) {
			t.Fatalf("invalid leases must be rejected before SQL: %v", err)
		}
	}
}

func TestMarkSentBatchUsesPairedLeaseConditions(t *testing.T) {
	for _, test := range []struct {
		name string
		rows int64
		lost bool
	}{
		{name: "混合租约一次更新", rows: 2},
		{name: "部分租约失效", rows: 1, lost: true},
		{name: "全部租约失效", rows: 0, lost: true},
	} {
		t.Run(test.name, func(t *testing.T) {
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
			var statement string
			var args []any
			calls := 0
			if err := db.Callback().Update().After("gorm:update").Register("test_capture_update", func(tx *gorm.DB) {
				calls++
				statement = tx.Statement.SQL.String()
				args = append([]any(nil), tx.Statement.Vars...)
				tx.RowsAffected = test.rows
			}); err != nil {
				t.Fatal(err)
			}
			leases := []outboxport.Lease{{EventID: "a", LockToken: "t1"}, {EventID: "b", LockToken: "t2"}, {EventID: "a", LockToken: "t1"}}
			err = NewOutboxRepository(db, nil).MarkSentBatch(context.Background(), leases, time.Now())
			if test.lost && !errors.Is(err, outboxport.ErrLeaseLost) || !test.lost && err != nil {
				t.Fatalf("unexpected result: %v", err)
			}
			if calls != 1 || !strings.Contains(statement, "status = ? AND (event_id, lock_token) IN ((?,?),(?,?))") {
				t.Fatalf("must update using paired leases in one statement: calls=%d SQL=%s", calls, statement)
			}
			want := []any{outboxport.StatusProcessing, "a", "t1", "b", "t2"}
			if len(args) < len(want) || !reflect.DeepEqual(args[len(args)-len(want):], want) {
				t.Fatalf("unexpected lease condition parameters: %v", args)
			}
		})
	}
}

func TestFailureBatchesRejectInvalidLeases(t *testing.T) {
	repository := NewOutboxRepository(nil, nil)
	if err := repository.MarkRetryBatch(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDeadBatch(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for _, leases := range [][]outboxport.Lease{
		{{EventID: "a"}}, {{LockToken: "token"}}, {{EventID: "a", LockToken: "old"}, {EventID: "a", LockToken: "new"}},
	} {
		var retries []outboxport.RetryUpdate
		var dead []outboxport.DeadUpdate
		for _, lease := range leases {
			retries = append(retries, outboxport.RetryUpdate{Lease: lease})
			dead = append(dead, outboxport.DeadUpdate{Lease: lease})
		}
		if err := repository.MarkRetryBatch(context.Background(), retries); !errors.Is(err, outboxport.ErrLeaseLost) {
			t.Fatalf("invalid retry lease must be rejected before SQL: %v", err)
		}
		if err := repository.MarkDeadBatch(context.Background(), dead); !errors.Is(err, outboxport.ErrLeaseLost) {
			t.Fatalf("invalid dead lease must be rejected before SQL: %v", err)
		}
	}
}

func TestFailureBatchesPreserveMetadataInOneUpdate(t *testing.T) {
	for _, kind := range []string{"retry", "dead"} {
		for _, rows := range []int64{2, 1, 0} {
			t.Run(kind+"/"+string(rune('0'+rows)), func(t *testing.T) {
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
				var statement string
				var args []any
				calls := 0
				if err := db.Callback().Update().After("gorm:update").Register("capture_failure_batch", func(tx *gorm.DB) {
					calls++
					statement = tx.Statement.SQL.String()
					args = append([]any(nil), tx.Statement.Vars...)
					tx.RowsAffected = rows
				}); err != nil {
					t.Fatal(err)
				}
				repository := NewOutboxRepository(db, nil)
				first := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
				second := first.Add(30 * time.Second)
				if kind == "retry" {
					err = repository.MarkRetryBatch(context.Background(), []outboxport.RetryUpdate{
						{Lease: outboxport.Lease{EventID: "a", LockToken: "t1"}, LastError: "error 'A", NextRetryAt: first},
						{Lease: outboxport.Lease{EventID: "b", LockToken: "t2"}, LastError: "error B", NextRetryAt: second},
					})
					if !strings.Contains(statement, "retry_count + 1") || !strings.Contains(statement, "ELSE next_retry_at END") {
						t.Fatalf("retry must advance each row's count and time: %s", statement)
					}
					for _, value := range []time.Time{first, second} {
						found := false
						for _, arg := range args {
							if reflect.DeepEqual(arg, value) {
								found = true
							}
						}
						if !found {
							t.Fatalf("retry time missing: %v args=%v", value, args)
						}
					}
				} else {
					err = repository.MarkDeadBatch(context.Background(), []outboxport.DeadUpdate{
						{Lease: outboxport.Lease{EventID: "a", LockToken: "t1"}, LastError: "error 'A"},
						{Lease: outboxport.Lease{EventID: "b", LockToken: "t2"}, LastError: "error B"},
					})
					if strings.Contains(statement, "retry_count") || strings.Contains(statement, "next_retry_at") {
						t.Fatalf("dead update must not schedule another retry: %s", statement)
					}
				}
				if rows < 2 && !errors.Is(err, outboxport.ErrLeaseLost) || rows == 2 && err != nil {
					t.Fatalf("unexpected lease outcome: %v", err)
				}
				if calls != 1 || !strings.Contains(statement, "CASE event_id WHEN ? THEN ? WHEN ? THEN ? ELSE last_error END") ||
					!strings.Contains(statement, "status = ? AND (event_id, lock_token) IN ((?,?),(?,?))") {
					t.Fatalf("must use one UPDATE with per-row errors and paired leases: calls=%d SQL=%s", calls, statement)
				}
				if !reflect.DeepEqual(args[:4], []any{"a", "error 'A", "b", "error B"}) ||
					!reflect.DeepEqual(args[len(args)-5:], []any{outboxport.StatusProcessing, "a", "t1", "b", "t2"}) {
					t.Fatalf("metadata must be parameterized and retain each lease: %v", args)
				}
			})
		}
	}
}
