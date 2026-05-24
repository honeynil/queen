package queen_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/sqlite"
	"github.com/yaop-labs/queen/tap"
)

func newTapTestQueen(t *testing.T, sink tap.Sink) (*queen.Queen, func()) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	q := queen.New(sqlite.New(db), queen.WithTap(sink))
	return q, func() {
		_ = q.Close()
	}
}

func TestTap_SQLMigrationEmitsStartTxExecTxEnd(t *testing.T) {
	rec := tap.NewRecorderSink()
	q, cleanup := newTapTestQueen(t, rec)
	defer cleanup()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY)",
		DownSQL: "DROP TABLE users",
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}

	events := rec.Events()
	if len(events) != 5 {
		t.Fatalf("expected 5 events (start, tx_begin, exec, tx_commit, end), got %d: %+v", len(events), events)
	}
	if events[0].Kind != tap.KindStart {
		t.Errorf("event[0] kind = %q, want start", events[0].Kind)
	}
	if events[1].Kind != tap.KindTxBegin {
		t.Errorf("event[1] kind = %q, want tx_begin", events[1].Kind)
	}
	if events[2].Kind != tap.KindExec {
		t.Errorf("event[2] kind = %q, want exec", events[2].Kind)
	}
	if !strings.Contains(events[2].SQL, "CREATE TABLE users") {
		t.Errorf("exec event missing SQL: %q", events[2].SQL)
	}
	if events[3].Kind != tap.KindTxCommit {
		t.Errorf("event[3] kind = %q, want tx_commit", events[3].Kind)
	}
	if events[4].Kind != tap.KindEnd {
		t.Errorf("event[4] kind = %q, want end", events[4].Kind)
	}
	if events[4].Error != "" {
		t.Errorf("expected no error, got %q", events[4].Error)
	}
	if events[4].Duration <= 0 {
		t.Errorf("expected positive duration, got %v", events[4].Duration)
	}
	for _, e := range events {
		if e.Version != "001" || e.Name != "create_users" {
			t.Errorf("bad metadata on event: %+v", e)
		}
		if e.Direction != tap.DirectionUp {
			t.Errorf("expected direction=up, got %q", e.Direction)
		}
	}
}

func TestTap_GoFuncMigrationWithObservedTx(t *testing.T) {
	rec := tap.NewRecorderSink()
	q, cleanup := newTapTestQueen(t, rec)
	defer cleanup()

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "go_func_mig",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, "CREATE TABLE t (id INTEGER)"); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx, "INSERT INTO t (id) VALUES (1)"); err != nil {
				return err
			}
			return nil
		},
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}

	events := rec.Events()
	// Expect: start, tx_begin, exec(CREATE), exec(INSERT), tx_commit, end
	if len(events) != 6 {
		t.Fatalf("expected 6 events, got %d: %+v", len(events), events)
	}
	if events[0].Kind != tap.KindStart || events[1].Kind != tap.KindTxBegin || events[4].Kind != tap.KindTxCommit || events[5].Kind != tap.KindEnd {
		t.Errorf("missing start/tx/end events: %+v", events)
	}
	if !strings.Contains(events[2].SQL, "CREATE TABLE") {
		t.Errorf("event[2] SQL = %q", events[2].SQL)
	}
	if !strings.Contains(events[3].SQL, "INSERT INTO t") {
		t.Errorf("event[3] SQL = %q", events[3].SQL)
	}
	if events[3].RowsAffected != 1 {
		t.Errorf("expected rows_affected=1, got %d", events[3].RowsAffected)
	}
}

func TestTap_QueryRowAndPrepareCaptureStatementDetails(t *testing.T) {
	rec := tap.NewRecorderSink()
	analyzed := tap.NewAnalyzerSink(rec, tap.DefaultAnalyzerConfig())
	q, cleanup := newTapTestQueen(t, analyzed)
	defer cleanup()

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "queryrow_prepare",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
				return err
			}
			stmt, err := t.PrepareContext(ctx, "INSERT INTO users (email) VALUES (?)")
			if err != nil {
				return err
			}
			defer func() { _ = stmt.Close() }()
			if _, err := stmt.ExecContext(ctx, "alice@example.com"); err != nil {
				return err
			}
			var email string
			return t.QueryRowContext(ctx, "SELECT email FROM users WHERE id = ?", 1).Scan(&email)
		},
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}

	events := rec.Events()
	var execs []tap.Event
	for _, e := range events {
		if e.Kind == tap.KindExec {
			execs = append(execs, e)
		}
	}
	if len(execs) != 3 {
		t.Fatalf("expected 3 exec events, got %d: %+v", len(execs), execs)
	}
	for i, e := range execs {
		if e.Index != i+1 {
			t.Fatalf("exec[%d].Index=%d, want %d", i, e.Index, i+1)
		}
	}
	if execs[1].BoundSQL != "INSERT INTO users (email) VALUES ('alice@example.com')" {
		t.Fatalf("prepared BoundSQL=%q", execs[1].BoundSQL)
	}
	if execs[2].Operation != "select" {
		t.Fatalf("queryrow operation=%q", execs[2].Operation)
	}
}

func TestTap_ErrorRecordedOnEnd(t *testing.T) {
	rec := tap.NewRecorderSink()
	q, cleanup := newTapTestQueen(t, rec)
	defer cleanup()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "bad",
		UpSQL:   "THIS IS NOT VALID SQL",
	})

	if err := q.Up(context.Background()); err == nil {
		t.Fatal("expected error from bad SQL")
	}

	events := rec.Events()
	if len(events) < 2 {
		t.Fatalf("expected at least start+end, got %d", len(events))
	}
	if events[len(events)-2].Kind != tap.KindTxRollback {
		t.Fatalf("penultimate event kind=%q, want tx_rollback: %+v", events[len(events)-2].Kind, events)
	}
	last := events[len(events)-1]
	if last.Kind != tap.KindEnd {
		t.Fatalf("last event kind=%q, want end", last.Kind)
	}
	if last.Error == "" {
		t.Errorf("expected end event to carry error message")
	}
}

func TestTap_NoSinkIsNoOp(t *testing.T) {
	// Sanity: without WithTap, migrations still work and tap.ObserveTx is a passthrough.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q := queen.New(sqlite.New(db))
	defer func() { _ = q.Close() }()

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "go_func",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tap.ObserveTx(ctx, tx).ExecContext(ctx, "CREATE TABLE x (id INT)")
			return err
		},
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up with no sink: %v", err)
	}
}

func TestMixedMigrationExecutesSQLAndFunc(t *testing.T) {
	rec := tap.NewRecorderSink()
	q, cleanup := newTapTestQueen(t, rec)
	defer cleanup()

	var downFuncSawRows int
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "mixed_schema_and_data",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			tapped := tap.ObserveTx(ctx, tx)
			_, err := tapped.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", 1, "Ada")
			return err
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			tapped := tap.ObserveTx(ctx, tx)
			return tapped.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&downFuncSawRows)
		},
		DownSQL: "DROP TABLE users",
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}

	var count int
	if err := q.SQLDB().QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("query users after Up: %v", err)
	}
	if count != 1 {
		t.Fatalf("users count after mixed Up = %d, want 1", count)
	}

	if err := q.Down(context.Background(), 1); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if downFuncSawRows != 1 {
		t.Fatalf("DownFunc saw %d rows, want 1 before DownSQL drops table", downFuncSawRows)
	}
	if _, err := q.SQLDB().Exec("SELECT COUNT(*) FROM users"); err == nil {
		t.Fatal("users table still exists after mixed Down")
	}

	var upExecs, downExecs int
	for _, e := range rec.Events() {
		if e.Kind != tap.KindExec {
			continue
		}
		switch e.Direction {
		case tap.DirectionUp:
			upExecs++
		case tap.DirectionDown:
			downExecs++
		}
	}
	if upExecs != 2 {
		t.Fatalf("up exec events = %d, want 2 (UpSQL and UpFunc)", upExecs)
	}
	if downExecs != 2 {
		t.Fatalf("down exec events = %d, want 2 (DownFunc and DownSQL)", downExecs)
	}
}

func TestMixedMigrationFuncFailureRollsBackSQL(t *testing.T) {
	q, cleanup := newTapTestQueen(t, tap.NopSink{})
	defer cleanup()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "mixed_failure",
		UpSQL:   "CREATE TABLE rolled_back_users (id INTEGER PRIMARY KEY)",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			return errors.New("boom")
		},
	})

	if err := q.Up(context.Background()); err == nil {
		t.Fatal("Up succeeded; want UpFunc failure")
	}

	var count int
	err := q.SQLDB().QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'rolled_back_users'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled_back_users table exists after failed mixed migration")
	}
}
