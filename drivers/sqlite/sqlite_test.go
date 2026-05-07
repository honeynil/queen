//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// TestQuoteIdentifier tests the identifier quoting function.
func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple table name",
			input:    "users",
			expected: `"users"`,
		},
		{
			name:     "table name with double quote",
			input:    `my"table`,
			expected: `"my""table"`,
		},
		{
			name:     "table name with multiple quotes",
			input:    `my"ta"ble`,
			expected: `"my""ta""ble"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
		{
			name:     "table name with spaces",
			input:    "my table",
			expected: `"my table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := base.QuoteDoubleQuotes(tt.input)
			if result != tt.expected {
				t.Errorf("base.QuoteDoubleQuotes(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestDriverCreation tests driver creation functions.
func TestDriverCreation(t *testing.T) {
	db := &sql.DB{}

	t.Run("New creates driver with default table name", func(t *testing.T) {
		driver := New(db)
		if driver.DB != db {
			t.Error("driver.DB should be set")
		}
		if driver.TableName != "queen_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "queen_migrations")
		}
	})

	t.Run("NewWithTableName creates driver with custom table name", func(t *testing.T) {
		driver := NewWithTableName(db, "custom_migrations")
		if driver.DB != db {
			t.Error("driver.DB should be set")
		}
		if driver.TableName != "custom_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "custom_migrations")
		}
	})
}

// setupTestDB creates a test database connection using in-memory SQLite.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open SQLite: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("failed to ping SQLite: %v", err)
	}

	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		_ = db.Close()
		t.Fatalf("failed to enable foreign keys: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
	}

	return db, cleanup
}

// setupTestDBFile creates a test database using a temporary file.
func setupTestDBFile(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	tmpfile, err := os.CreateTemp("", "queen-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_ = tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name()+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		_ = os.Remove(tmpfile.Name())
		t.Fatalf("failed to open SQLite: %v", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		_ = os.Remove(tmpfile.Name())
		t.Fatalf("failed to ping SQLite: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(tmpfile.Name())
		_ = os.Remove(tmpfile.Name() + "-wal")
		_ = os.Remove(tmpfile.Name() + "-shm")
	}

	return db, cleanup
}

func TestInit(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	err := driver.Init(ctx)
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	var tableName string
	err = db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name='queen_migrations'").Scan(&tableName)
	if err != nil {
		t.Fatalf("migrations table was not created: %v", err)
	}
	if tableName != "queen_migrations" {
		t.Errorf("table name = %q; want %q", tableName, "queen_migrations")
	}

	err = driver.Init(ctx)
	if err != nil {
		t.Fatalf("second Init() failed: %v", err)
	}
}

func TestRecordAndGetApplied(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	applied, err := driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("expected 0 migrations, got %d", len(applied))
	}

	m1 := &queen.Migration{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER)",
	}
	if err := driver.Record(ctx, m1, nil); err != nil {
		t.Fatalf("Record() failed: %v", err)
	}

	applied, err = driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}
	if len(applied) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(applied))
	}
	if applied[0].Version != "001" {
		t.Errorf("version = %q; want %q", applied[0].Version, "001")
	}
	if applied[0].Name != "create_users" {
		t.Errorf("name = %q; want %q", applied[0].Name, "create_users")
	}

	m2 := &queen.Migration{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   "CREATE TABLE posts (id INTEGER)",
	}
	if err := driver.Record(ctx, m2, nil); err != nil {
		t.Fatalf("Record() failed: %v", err)
	}

	applied, err = driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(applied))
	}
	if applied[0].Version != "001" {
		t.Errorf("first version = %q; want %q", applied[0].Version, "001")
	}
	if applied[1].Version != "002" {
		t.Errorf("second version = %q; want %q", applied[1].Version, "002")
	}
}

func TestRemove(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	m := &queen.Migration{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER)",
	}
	if err := driver.Record(ctx, m, nil); err != nil {
		t.Fatalf("Record() failed: %v", err)
	}

	applied, _ := driver.GetApplied(ctx)
	if len(applied) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(applied))
	}

	if err := driver.Remove(ctx, "001"); err != nil {
		t.Fatalf("Remove() failed: %v", err)
	}

	applied, err := driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("expected 0 migrations after removal, got %d", len(applied))
	}
}

func TestLocking(t *testing.T) {
	db, cleanup := setupTestDBFile(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err := driver.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Lock() failed: %v", err)
	}

	var lockingMode string
	err = db.QueryRowContext(ctx, "PRAGMA locking_mode").Scan(&lockingMode)
	if err != nil {
		t.Fatalf("failed to query locking mode: %v", err)
	}
	if lockingMode != "exclusive" {
		t.Errorf("locking_mode = %q; want %q", lockingMode, "exclusive")
	}

	if err := driver.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() failed: %v", err)
	}

	err = db.QueryRowContext(ctx, "PRAGMA locking_mode").Scan(&lockingMode)
	if err != nil {
		t.Fatalf("failed to query locking mode: %v", err)
	}
	if lockingMode != "normal" {
		t.Errorf("locking_mode = %q; want %q after unlock", lockingMode, "normal")
	}

	if err := driver.Unlock(ctx); err != nil {
		t.Errorf("double Unlock() should be safe, got error: %v", err)
	}
}

func TestExec(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err := driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE test_users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			)
		`)
		return err
	})
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	var tableName string
	err = db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name='test_users'").Scan(&tableName)
	if err != nil {
		t.Fatalf("table was not created: %v", err)
	}

	err = driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO test_users (name) VALUES ('Alice')")
		if err != nil {
			return err
		}
		return sql.ErrTxDone
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_users").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

func TestFullMigrationCycle(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	ctx := context.Background()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE test_users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				email TEXT NOT NULL UNIQUE
			)
		`,
		DownSQL: `DROP TABLE test_users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE test_posts (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
			)
		`,
		DownSQL: `DROP TABLE test_posts`,
	})

	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up() failed: %v", err)
	}

	var tableCount int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('test_users', 'test_posts')").Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}
	if tableCount != 2 {
		t.Errorf("expected 2 tables, got %d", tableCount)
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("Status() failed: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(statuses))
	}
	for _, s := range statuses {
		if s.Status != queen.StatusApplied {
			t.Errorf("migration %s status = %s; want applied", s.Version, s.Status)
		}
	}

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("Reset() failed: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('test_users', 'test_posts')").Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}
	if tableCount != 0 {
		t.Errorf("expected 0 tables after reset, got %d", tableCount)
	}
}

func TestWALMode(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "queen-wal-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_ = tmpfile.Close()
	defer func() { _ = os.Remove(tmpfile.Name()) }()
	defer func() { _ = os.Remove(tmpfile.Name() + "-wal") }()
	defer func() { _ = os.Remove(tmpfile.Name() + "-shm") }()

	db, err := sql.Open("sqlite3", tmpfile.Name()+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("failed to open SQLite: %v", err)
	}
	defer func() { _ = db.Close() }()

	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("failed to get journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("journal_mode = %q; want 'wal'", journalMode)
	}

	driver := New(db)
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	ctx := context.Background()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE users`,
	})

	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up() failed in WAL mode: %v", err)
	}

	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
	if err != nil {
		t.Fatalf("table was not created in WAL mode: %v", err)
	}
}

func TestTimestampParsing(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	m := &queen.Migration{
		Version: "001",
		Name:    "test_migration",
		UpSQL:   "CREATE TABLE test (id INTEGER)",
	}
	if err := driver.Record(ctx, m, nil); err != nil {
		t.Fatalf("Record() failed: %v", err)
	}

	applied, err := driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}

	if len(applied) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(applied))
	}

	if applied[0].AppliedAt.IsZero() {
		t.Error("AppliedAt should not be zero")
	}

	elapsed := time.Since(applied[0].AppliedAt)
	if elapsed > time.Minute {
		t.Errorf("AppliedAt timestamp seems incorrect: %v (elapsed: %v)", applied[0].AppliedAt, elapsed)
	}
}

func TestLockUnit(t *testing.T) {
	t.Parallel()

	t.Run("handles busy_timeout error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		timeoutErr := errors.New("pragma busy_timeout failed")
		mock.ExpectExec("PRAGMA busy_timeout").WillReturnError(timeoutErr)

		err = driver.Lock(ctx, 5*time.Second)
		if err == nil {
			t.Error("Lock() should return error when PRAGMA busy_timeout fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles locking_mode error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("PRAGMA busy_timeout").WillReturnResult(sqlmock.NewResult(0, 0))

		lockErr := errors.New("pragma locking_mode failed")
		mock.ExpectExec("PRAGMA locking_mode = EXCLUSIVE").WillReturnError(lockErr)

		err = driver.Lock(ctx, 5*time.Second)
		if err == nil {
			t.Error("Lock() should return error when PRAGMA locking_mode fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestUnlockUnit(t *testing.T) {
	t.Parallel()

	t.Run("handles unlock PRAGMA error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		unlockErr := errors.New("pragma normal failed")
		mock.ExpectExec("PRAGMA locking_mode = NORMAL").WillReturnError(unlockErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when PRAGMA fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles unlock transaction error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("PRAGMA locking_mode = NORMAL").
			WillReturnResult(sqlmock.NewResult(0, 0))

		txErr := errors.New("begin tx failed")
		mock.ExpectBegin().WillReturnError(txErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when BeginTx fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}
