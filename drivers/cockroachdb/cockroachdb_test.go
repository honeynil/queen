package cockroachdb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const CockroachTestDSN = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

// TestInitUnit tests Init using sqlmock
func TestInitUnit(t *testing.T) {
	t.Run("creates migrations table successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(`ALTER TABLE "queen_migrations" ADD COLUMN`).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations_lock"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

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

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

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

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		// ALTER TABLE statements can fail (e.g., column already exists)
		// The driver should continue and not return error
		for i := 0; i < 7; i++ {
			mock.ExpectExec(`ALTER TABLE "queen_migrations" ADD COLUMN`).
				WillReturnError(errors.New("column already exists"))
		}

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations_lock"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() should not fail on ALTER errors: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles lock table creation error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations"`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		// All ALTER TABLE statements succeed
		for i := 0; i < 7; i++ {
			mock.ExpectExec(`ALTER TABLE "queen_migrations" ADD COLUMN`).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}

		// Lock table creation fails
		lockErr := errors.New("lock table creation failed")
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "queen_migrations_lock"`).
			WillReturnError(lockErr)

		err = driver.Init(ctx)
		if !errors.Is(err, lockErr) {
			t.Errorf("Init() error = %v; want %v", err, lockErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

// TestLockUnit tests Lock using sqlmock
func TestLockUnit(t *testing.T) {
	t.Run("acquires table-based lock successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		// First iteration of retry loop:
		// Cleanup query - should succeed even if no rows
		mock.ExpectExec(`DELETE FROM "queen_migrations_lock" WHERE lock_key = \$1 AND expires_at < now\(\)`).
			WithArgs("migration_lock", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		// Check if lock exists - should return no rows
		mock.ExpectQuery(`SELECT 1 FROM "queen_migrations_lock" WHERE lock_key = \$1 AND expires_at >= now\(\) LIMIT 1`).
			WithArgs("migration_lock").
			WillReturnError(sql.ErrNoRows)

		// Insert the lock
		mock.ExpectExec(`INSERT INTO "queen_migrations_lock" \(lock_key, expires_at, owner_id\) VALUES \(\$1, \$2, \$3\)`).
			WithArgs("migration_lock", sqlmock.AnyArg(), driver.ownerID).
			WillReturnResult(sqlmock.NewResult(1, 1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Lock() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles timeout when lock already held", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		// Will retry multiple times with 1ms timeout, so we need multiple expectations
		for i := 0; i < 5; i++ {
			mock.ExpectExec(`DELETE FROM "queen_migrations_lock" WHERE lock_key = \$1 AND expires_at < now\(\)`).
				WithArgs("migration_lock", sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 0))

			mock.ExpectQuery(`SELECT 1 FROM "queen_migrations_lock" WHERE lock_key = \$1 AND expires_at >= now\(\) LIMIT 1`).
				WithArgs("migration_lock").
				WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		}

		// With very short timeout, should fail
		err = driver.Lock(ctx, 1*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})
}

func TestUnlockUnit(t *testing.T) {
	t.Run("unlocks successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		mock.ExpectExec(`DELETE FROM "queen_migrations_lock" WHERE lock_key = \$1 AND owner_id = \$2`).
			WithArgs("migration_lock", driver.ownerID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles unlock error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		unlockErr := errors.New("unlock failed")
		mock.ExpectExec(`DELETE FROM "queen_migrations_lock" WHERE lock_key = \$1 AND owner_id = \$2`).
			WithArgs("migration_lock", driver.ownerID).
			WillReturnError(unlockErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when delete fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("uses correct table name in unlock query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := NewWithTableName(db, "custom_table")
		if err != nil {
			t.Fatalf("NewWithTableName() failed: %v", err)
		}

		ctx := context.Background()

		// Should use custom_table_lock, not queen_migrations_lock
		mock.ExpectExec(`DELETE FROM "custom_table_lock" WHERE lock_key = \$1 AND owner_id = \$2`).
			WithArgs("migration_lock", driver.ownerID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles when no rows were deleted", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()

		// Return that 0 rows were deleted
		mock.ExpectExec(`DELETE FROM "queen_migrations_lock" WHERE lock_key = \$1 AND owner_id = \$2`).
			WithArgs("migration_lock", driver.ownerID).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = driver.Unlock(ctx)
		// Should still succeed - idempotent behavior
		if err != nil {
			t.Errorf("Unlock() should not fail when no rows deleted: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

// TestConfigurationValuesUnit tests driver configuration with sqlmock.
func TestConfigurationValuesUnit(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if driver.Config.Placeholder(1) != "$1" {
		t.Errorf("Placeholder(1) = %q; want $1", driver.Config.Placeholder(1))
	}
	if driver.Config.Placeholder(42) != "$42" {
		t.Errorf("Placeholder(42) = %q; want $42", driver.Config.Placeholder(42))
	}

	quoted := driver.Config.QuoteIdentifier("test")
	if quoted != `"test"` {
		t.Errorf("QuoteIdentifier(test) = %q; want %q", quoted, `"test"`)
	}

	if driver.Config.ParseTime != nil {
		t.Error("ParseTime should be nil")
	}
}

// TestLockAndUnlockSequence tests the complete lock/unlock flow.
func TestLockAndUnlockSequence(t *testing.T) {
	t.Run("lock and unlock sequence with custom table name", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver, err := NewWithTableName(db, "custom_migrations")
		if err != nil {
			t.Fatalf("NewWithTableName() failed: %v", err)
		}

		ctx := context.Background()

		mock.ExpectExec(`DELETE FROM "custom_migrations_lock" WHERE lock_key = \$1 AND expires_at < now\(\)`).
			WithArgs("migration_lock", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectQuery(`SELECT 1 FROM "custom_migrations_lock" WHERE lock_key = \$1 AND expires_at >= now\(\) LIMIT 1`).
			WithArgs("migration_lock").
			WillReturnError(sql.ErrNoRows)

		mock.ExpectExec(`INSERT INTO "custom_migrations_lock" \(lock_key, expires_at, owner_id\) VALUES \(\$1, \$2, \$3\)`).
			WithArgs("migration_lock", sqlmock.AnyArg(), driver.ownerID).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec(`DELETE FROM "custom_migrations_lock" WHERE lock_key = \$1 AND owner_id = \$2`).
			WithArgs("migration_lock", driver.ownerID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		err = driver.Unlock(ctx)
		if err != nil {
			t.Fatalf("Unlock() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

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
		{
			name:     "table name with special chars",
			input:    `my-table`,
			expected: `"my-table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdentifier(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestQuoteIdentifierBase tests the base QuoteDoubleQuotes function.
func TestQuoteIdentifierBase(t *testing.T) {
	t.Parallel()

	result := base.QuoteDoubleQuotes("test_table")
	if result != `"test_table"` {
		t.Errorf("base.QuoteDoubleQuotes() = %q; want %q", result, `"test_table"`)
	}
}

// TestDriverCreation tests driver creation functions.
func TestDriverCreation(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("New creates driver with default table name", func(t *testing.T) {
		driver, err := New(db)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		if driver.DB != db {
			t.Error("driver.DB should be set")
		}
		if driver.TableName != "queen_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "queen_migrations")
		}
		if driver.lockTableName != "queen_migrations_lock" {
			t.Errorf("driver.lockTableName = %q; want %q", driver.lockTableName, "queen_migrations_lock")
		}
		if driver.lockKey != "migration_lock" {
			t.Errorf("driver.lockKey = %q; want %q", driver.lockKey, "migration_lock")
		}
		if driver.ownerID == "" {
			t.Error("driver.ownerID should not be empty")
		}
	})

	t.Run("NewWithTableName creates driver with custom table name", func(t *testing.T) {
		driver, err := NewWithTableName(db, "custom_migrations")
		if err != nil {
			t.Fatalf("NewWithTableName() failed: %v", err)
		}
		if driver.DB != db {
			t.Error("driver.DB should be set")
		}
		if driver.TableName != "custom_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "custom_migrations")
		}
		if driver.lockTableName != "custom_migrations_lock" {
			t.Errorf("driver.lockTableName = %q; want %q", driver.lockTableName, "custom_migrations_lock")
		}
		if driver.ownerID == "" {
			t.Error("driver.ownerID should not be empty")
		}
	})

	t.Run("drivers have unique ownerIDs", func(t *testing.T) {
		driver1, _ := New(db)
		driver2, _ := New(db)
		if driver1.ownerID == driver2.ownerID {
			t.Errorf("ownerIDs should be unique, got same ID: %s", driver1.ownerID)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// setupTestDB creates a test database connection.
// This requires CockroachDB to be running. Tests will be skipped if CockroachDB is not available.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	dsn := os.Getenv("COCKROACHDB_TEST_DSN")
	if dsn == "" {
		dsn = CockroachTestDSN
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skip("CockroachDB not available:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skip("CockroachDB not available:", err)
	}

	cleanup := func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS queen_migrations")
		_, _ = db.Exec("DROP TABLE IF EXISTS queen_migrations_lock")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_users")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_posts")
		_ = db.Close()
	}

	return db, cleanup
}

func TestInit(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	ctx := context.Background()

	err = driver.Init(ctx)
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	var tableName string
	err = db.QueryRowContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'queen_migrations';").Scan(&tableName)
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

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
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

func TestRemove(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
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

	applied, err = driver.GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied() failed: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("expected 0 migrations after removal, got %d", len(applied))
	}
}

func TestLocking(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err = driver.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Lock() failed: %v", err)
	}

	err = driver.Lock(ctx, 100*time.Millisecond)
	if !errors.Is(err, queen.ErrLockTimeout) {
		t.Errorf("expected ErrLockTimeout, got %v", err)
	}

	if err := driver.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() failed: %v", err)
	}

	err = driver.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("second Lock() failed after unlock: %v", err)
	}

	_ = driver.Unlock(ctx)
}

func TestExec(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err = driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
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

	// Test failed transaction (should rollback)
	// Note: ClickHouse doesn't support full ACID transactions like PostgreSQL/MySQL,
	// so rollback behavior may be limited. This test verifies the error handling.
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

func TestFullMigrationCycle(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

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
	err = db.QueryRowContext(ctx,
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

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
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

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	ctx := context.Background()

	if err := driver.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	err = driver.Unlock(ctx)
	if err != nil {
		t.Errorf("expected nil when unlocking without lock (graceful), got: %v", err)
	}
}

func TestLockOwnership_PreventsCrossProcessUnlock(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driverA, err := New(db)
	if err != nil {
		t.Fatalf("New(driverA) failed: %v", err)
	}
	driverB, err := New(db)
	if err != nil {
		t.Fatalf("New(driverB) failed: %v", err)
	}

	ctx := context.Background()

	if err := driverA.Init(ctx); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	if err := driverA.Lock(ctx, 30*time.Second); err != nil {
		t.Fatalf("driverA.Lock() failed: %v", err)
	}

	_, err = db.ExecContext(ctx, "DELETE FROM queen_migrations_lock WHERE lock_key = 'migration_lock'")
	if err != nil {
		t.Fatalf("failed to delete lock: %v", err)
	}

	var count int64
	err = db.QueryRowContext(ctx,
		"SELECT count(*) FROM queen_migrations_lock WHERE lock_key = 'migration_lock'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check lock count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected lock to be deleted, got count=%d", count)
	}

	if err := driverB.Lock(ctx, 30*time.Second); err != nil {
		t.Fatalf("driverB.Lock() should succeed after A's lock expired: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT count(*) FROM queen_migrations_lock WHERE lock_key = 'migration_lock'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check lock count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected Process B's lock to exist, got count=%d", count)
	}

	if err := driverA.Unlock(ctx); err != nil {
		t.Fatalf("driverA.Unlock() should be graceful: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT count(*) FROM queen_migrations_lock WHERE lock_key = 'migration_lock'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check lock count: %v", err)
	}
	if count != 1 {
		t.Errorf("CRITICAL: Process A unlocked Process B's lock! Expected count=1, got count=%d", count)
	}

	if err := driverB.Unlock(ctx); err != nil {
		t.Fatalf("driverB.Unlock() failed: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT count(*) FROM queen_migrations_lock WHERE lock_key = 'migration_lock'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check lock count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected lock to be deleted after driverB.Unlock(), got count=%d", count)
	}
}

func TestLock_ContextCancellation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	driver, err := New(db)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if err := driver.Init(context.Background()); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	if err := driver.Lock(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Lock() failed: %v", err)
	}
	defer func() { _ = driver.Unlock(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = driver.Lock(ctx, 5*time.Second)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
