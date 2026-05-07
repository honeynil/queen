package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

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
				t.Errorf("quoteIdentifier(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDriverCreation(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("New creates driver with default table name", func(t *testing.T) {
		driver := New(db)
		if driver.DB != db {
			t.Error("driver.db should be set")
		}
		if driver.TableName != "queen_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "queen_migrations")
		}
		expectedLockID := hashTableName("queen_migrations")
		if driver.lockID != expectedLockID {
			t.Errorf("driver.lockID = %d; want %d", driver.lockID, expectedLockID)
		}
	})

	t.Run("NewWithTableName creates driver with custom table name", func(t *testing.T) {
		driver := NewWithTableName(db, "custom_migrations")
		if driver.DB != db {
			t.Error("driver.db should be set")
		}
		if driver.TableName != "custom_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "custom_migrations")
		}
		expectedLockID := hashTableName("custom_migrations")
		if driver.lockID != expectedLockID {
			t.Errorf("driver.lockID = %d; want %d", driver.lockID, expectedLockID)
		}
	})
}

func TestHashTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"default table", "queen_migrations", hashTableName("queen_migrations")},
		{"custom table", "custom_migrations", hashTableName("custom_migrations")},
		{"empty", "", hashTableName("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashTableName(tt.input)
			if result != tt.expected {
				t.Errorf("hashTableName(%q) = %d; want %d", tt.input, result, tt.expected)
			}
			result2 := hashTableName(tt.input)
			if result != result2 {
				t.Error("hashTableName is not deterministic")
			}
		})
	}

	t.Run("different names produce different hashes", func(t *testing.T) {
		hash1 := hashTableName("table1")
		hash2 := hashTableName("table2")
		if hash1 == hash2 {
			t.Error("different table names should produce different hashes")
		}
	})
}

func TestInit(t *testing.T) {
	t.Run("creates migrations table successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(`ALTER TABLE "queen_migrations" ADD COLUMN`).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles CREATE TABLE error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		createErr := errors.New("create table failed")
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnError(createErr)

		err = driver.Init(ctx)
		if !errors.Is(err, createErr) {
			t.Errorf("Init() error = %v; want %v", err, createErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("continues on ALTER TABLE errors (idempotent)", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(`ALTER TABLE "queen_migrations" ADD COLUMN`).
				WillReturnError(errors.New("column already exists"))
		}

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() should not fail on ALTER errors: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestLock(t *testing.T) {
	t.Run("acquires advisory lock successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("SELECT pg_advisory_lock").
			WithArgs(driver.lockID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Lock() failed: %v", err)
		}

		if driver.lockConn == nil {
			t.Error("lockConn should be set after successful lock")
		}

		_ = driver.lockConn.Close()

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles timeout correctly", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("SELECT pg_advisory_lock").
			WillDelayFor(2 * time.Second).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Lock(ctx, 100*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})
}

func TestUnlock(t *testing.T) {
	t.Run("unlocks successfully when locked", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("SELECT pg_advisory_lock").
			WithArgs(driver.lockID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		mock.ExpectExec("SELECT pg_advisory_unlock").
			WithArgs(driver.lockID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() failed: %v", err)
		}

		if driver.lockConn != nil {
			t.Error("lockConn should be nil after unlock")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("gracefully handles unlock when not locked", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() when not locked should be graceful, got error: %v", err)
		}
	})

	t.Run("handles unlock error", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec("SELECT pg_advisory_lock").
			WithArgs(driver.lockID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		unlockErr := errors.New("unlock failed")
		mock.ExpectExec("SELECT pg_advisory_unlock").
			WithArgs(driver.lockID).
			WillReturnError(unlockErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when unlock fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestInvalidMigrations(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
	}
	_ = db.Close()

	driver := New(db)
	q := queen.New(driver)

	t.Run("EmptyVersion", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "",
			Name:    "valid_name",
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("Add() with empty version succeeded; want error")
		}
	})

	t.Run("EmptyName", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "002",
			Name:    "",
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("Add() with empty name succeeded; want error")
		}
	})

	t.Run("VersionWithSpaces", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "003 with spaces",
			Name:    "valid_name",
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("Add() with spaces succeeded; want error")
		}
	})

	t.Run("LongMigrationName", func(t *testing.T) {
		longName := strings.Repeat("a", 64)
		err := q.Add(queen.M{
			Version: "004",
			Name:    longName,
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("SpecialCharsInVersion", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "005@№;%!4special",
			Name:    "valid_name",
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("Add() with special characters in version succeeded; want error")
		}
	})

	t.Run("SpecialCharsInName", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "006",
			Name:    "%",
			UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
			`,
			DownSQL: `DROP TABLE test_users`,
		})
		if err == nil {
			t.Error("Add() with special characters in name succeeded; want error")
		}
	})

	t.Run("DuplicateVersions", func(t *testing.T) {
		err := q.Add(queen.M{
			Version: "007",
			Name:    "first",
			UpSQL:   "CREATE TABLE dummy1 ()",
			DownSQL: "DROP TABLE dummy1",
		})
		if err != nil {
			t.Fatalf("first Add() failed: %v", err)
		}

		err = q.Add(queen.M{
			Version: "007",
			Name:    "second",
			UpSQL:   "CREATE TABLE dummy2 ()",
			DownSQL: "DROP TABLE dummy2",
		})
		if err == nil {
			t.Error("Add() with duplicate version succeeded; want error")
		}
	})
}

// Note: Integration tests that require a real PostgreSQL database are in PostgreSQL_integration_test.go
// Run with: go test -tags=integration -v

// setupTestDB creates a test database connection.
// This requires PostgreSQL to be running. Tests will be skipped if PostgreSQL is not available.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	dsn := os.Getenv("POSTGRESQL_TEST_DSN")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skip("PostgreSQL not available:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skip("PostgreSQL not available:", err)
	}

	cleanup := func() {
		var errs []error

		tables := []string{"queen_migrations", "test_users", "test_posts"}

		for _, table := range tables {
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
				errs = append(errs, fmt.Errorf("failed to drop table %s: %w", table, err))
			}
		}
		if err := db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close db: %w", err))
		}

		if len(errs) > 0 {
			t.Fatalf("cleanup failed with errors: %v", errs)
		}
	}

	return db, cleanup
}

func TestIntegrationInit(t *testing.T) {
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
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'queen_migrations';").Scan(&tableName)
	if err != nil {
		t.Fatalf("migrations table was not created: %v", err)
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
		UpSQL: `CREATE TABLE test_users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		)`,
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
		UpSQL: `CREATE TABLE test_posts (
			id UUID DEFAULT gen_random_uuid(),
		)`,
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

func TestIntegrationRemove(t *testing.T) {
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
		UpSQL: `CREATE TABLE test_users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		)`,
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

func TestIntegrationLocking(t *testing.T) {
	db, cleanup := setupTestDB(t)
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

	db2, err := sql.Open("pgx", os.Getenv("POSTGRESQL_TEST_DSN"))
	if err != nil {
		t.Fatalf("failed to open second connection: %v", err)
	}
	defer func() { _ = db2.Close() }()

	driver2 := New(db2)
	err = driver2.Lock(ctx, 1*time.Second)

	if !errors.Is(err, queen.ErrLockTimeout) {
		t.Errorf("expected ErrLockTimeout, got %v", err)
	}

	if err := driver.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() failed: %v", err)
	}

	err = driver2.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("second Lock() failed after unlock: %v", err)
	}

	_ = driver2.Unlock(ctx)
}

func TestIntegrationExec(t *testing.T) {
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
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				name VARCHAR(255) NOT NULL
			)
		`)
		return err
	})
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	var tableName string
	err = db.QueryRowContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'test_users';").Scan(&tableName)
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
}

func TestIntegrationFullMigrationCycle(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	q := queen.New(driver)

	ctx := context.Background()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE test_users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				email VARCHAR(255) NOT NULL
			)
		`,
		DownSQL: `DROP TABLE test_users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE test_posts (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				user_id UUID NOT NULL,
				title VARCHAR(255),
				FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
			)
		`,
		DownSQL: `DROP TABLE test_posts`,
	})

	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up() failed: %v", err)
	}

	var tableCount uint64
	err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('test_users', 'test_posts');").Scan(&tableCount)
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
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('test_users', 'test_posts');").Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}
	if tableCount != 0 {
		t.Errorf("expected 0 tables after reset, got %d", tableCount)
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
		UpSQL: `CREATE TABLE test (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		)`,
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

func TestUnlock_WhenNotLocked(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err := driver.Unlock(ctx)
	if err != nil {
		t.Errorf("expected nil when unlocking without lock (graceful), got: %v", err)
	}
}

func TestLock_ContextCancellation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver := New(db)

	if err := driver.Init(context.Background()); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	if err := driver.Lock(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Lock() failed: %v", err)
	}
	defer func() { _ = driver.Unlock(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := driver.Lock(ctx, 5*time.Second)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
