package inbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	inboxport "IM_backend/internal/application/ports/inbox"
	mysqlerr "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 使用内存 SQL 脚本验证 GORM 实际生成的语句和事务边界，不连接开发数据库。
type sqlStep struct {
	kind      string
	contains  []string
	args      []any
	rows      int64
	resultIDs []string
	err       error
}
type sqlScript struct {
	t     *testing.T
	steps []sqlStep
}

func (s *sqlScript) take(kind, query string, args []driver.NamedValue) sqlStep {
	s.t.Helper()
	if len(s.steps) == 0 {
		s.t.Fatalf("unexpected %s: %s", kind, query)
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	if step.kind != kind {
		s.t.Fatalf("want %s got %s: %s", step.kind, kind, query)
	}
	for _, fragment := range step.contains {
		if !strings.Contains(query, fragment) {
			s.t.Fatalf("SQL missing %q: %s", fragment, query)
		}
	}
	for _, expected := range step.args {
		found := false
		for _, arg := range args {
			if fmt.Sprint(arg.Value) == fmt.Sprint(expected) {
				found = true
				break
			}
		}
		if !found {
			s.t.Fatalf("SQL missing argument %v: %v", expected, args)
		}
	}
	return step
}

type scriptConnector struct{ script *sqlScript }

func (c scriptConnector) Connect(context.Context) (driver.Conn, error) {
	return &scriptConn{script: c.script}, nil
}
func (c scriptConnector) Driver() driver.Driver { return scriptDriver{} }

type scriptDriver struct{}

func (scriptDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type scriptConn struct{ script *sqlScript }

func (c *scriptConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *scriptConn) Close() error { return nil }
func (c *scriptConn) Begin() (driver.Tx, error) {
	c.script.take("begin", "", nil)
	return scriptTx{script: c.script}, nil
}
func (c *scriptConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	step := c.script.take("exec", query, args)
	return driver.RowsAffected(step.rows), step.err
}
func (c *scriptConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	step := c.script.take("query", query, args)
	return &scriptRows{ids: step.resultIDs}, step.err
}

type scriptTx struct{ script *sqlScript }

func (tx scriptTx) Commit() error   { return tx.script.take("commit", "", nil).err }
func (tx scriptTx) Rollback() error { return tx.script.take("rollback", "", nil).err }

type scriptRows struct{ ids []string }

func (r *scriptRows) Columns() []string { return []string{"event_id"} }
func (r *scriptRows) Close() error      { return nil }
func (r *scriptRows) Next(values []driver.Value) error {
	if len(r.ids) == 0 {
		return io.EOF
	}
	values[0] = r.ids[0]
	r.ids = r.ids[1:]
	return nil
}
func newSQLTest(t *testing.T, steps ...sqlStep) *InboxRepository {
	t.Helper()
	script := &sqlScript{t: t, steps: steps}
	conn := sql.OpenDB(scriptConnector{script: script})
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true, SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		if len(script.steps) != 0 {
			t.Errorf("%d SQL steps not executed", len(script.steps))
		}
	})
	return NewInboxRepository(db)
}

func TestTryClaimRequiresEventID(t *testing.T) {
	repo := newSQLTest(t)
	claimed, token, attemptCount, err := repo.TryClaim(context.Background(), "", "message", time.Now(), time.Now())
	if !errors.Is(err, inboxport.ErrEventIDRequired) || claimed || token != "" || attemptCount != 0 {
		t.Fatalf("unexpected empty event_id claim result: claimed=%t token=%q attempts=%d err=%v", claimed, token, attemptCount, err)
	}
}

func TestTryClaimBatchRequiresEventID(t *testing.T) {
	repo := newSQLTest(t)
	err := repo.TryClaimBatch(context.Background(), []inboxport.ClaimEvent{{EventID: "ok"}, {EventType: "message"}}, "token", time.Now())
	if !errors.Is(err, inboxport.ErrEventIDRequired) {
		t.Fatalf("expected missing event_id error, got %v", err)
	}
}

func TestTryClaimBatchUsesOneInsert(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "exec", contains: []string{"INSERT INTO `inboxes`", "),("}, args: []any{"a", "b", "token", inboxport.StatusProcessing}, rows: 2})
	err := repo.TryClaimBatch(context.Background(), []inboxport.ClaimEvent{{EventID: "a", EventType: "message"}, {EventID: "b", EventType: "message"}}, "token", time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestTryClaimBatchConflictRollsBack(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "begin"}, sqlStep{kind: "exec", contains: []string{"INSERT INTO `inboxes`"}, err: &mysqlerr.MySQLError{Number: 1062}}, sqlStep{kind: "rollback"})
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		return repo.WithTx(tx).TryClaimBatch(context.Background(), []inboxport.ClaimEvent{{EventID: "a"}, {EventID: "b"}}, "token", time.Now())
	})
	if !errors.Is(err, inboxport.ErrBatchConflict) {
		t.Fatalf("err=%v", err)
	}
}

func TestCompleteBatchUsesTokenGuardAndRollsBackPartialMatch(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "begin"}, sqlStep{kind: "exec", contains: []string{"UPDATE `inboxes`", "event_id IN (?,?) AND status = ? AND lock_token = ?"}, args: []any{"a", "b", "token", inboxport.StatusCompleted, inboxport.StatusProcessing}, rows: 1}, sqlStep{kind: "rollback"})
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		return repo.WithTx(tx).CompleteBatch(context.Background(), []string{"a", "b"}, "token", time.Now())
	})
	if !errors.Is(err, inboxport.ErrLeaseLost) {
		t.Fatalf("err=%v", err)
	}
}

func TestRenewBatchVerifiesOwnershipEvenWhenUpdateAffectsZero(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "begin"}, sqlStep{kind: "query", contains: []string{"FOR UPDATE", "lock_token = ?"}, args: []any{"token"}, resultIDs: []string{"a", "b"}}, sqlStep{kind: "exec", contains: []string{"UPDATE `inboxes`", "lock_token = ?"}, rows: 0}, sqlStep{kind: "commit"})
	if err := repo.RenewBatch(context.Background(), []string{"a", "b"}, "token", time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestRenewBatchDetectsLeaseLoss(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "begin"}, sqlStep{kind: "query", resultIDs: []string{"a"}}, sqlStep{kind: "rollback"})
	if err := repo.RenewBatch(context.Background(), []string{"a", "b"}, "old-token", time.Now()); !errors.Is(err, inboxport.ErrLeaseLost) {
		t.Fatal(err)
	}
}

func TestReleaseBatchUndoesOnlyUnexecutedAttempts(t *testing.T) {
	repo := newSQLTest(t, sqlStep{kind: "exec", contains: []string{"attempt_count - 1", "lock_token = ?"}, args: []any{"old-token"}}, sqlStep{kind: "exec", contains: []string{"UPDATE `inboxes`", "lock_token = ?"}, args: []any{"old-token"}})
	if err := repo.ReleaseBatch(context.Background(), []string{"a", "b"}, "old-token", true); err != nil {
		t.Fatal(err)
	}
	if err := repo.ReleaseBatch(context.Background(), []string{"c"}, "old-token", false); err != nil {
		t.Fatal(err)
	}
}
