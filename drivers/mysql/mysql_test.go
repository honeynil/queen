package mysql

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// TestQuoteIdentifier tests the identifier quoting function.
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
			expected: "`users`",
		},
		{
			name:     "table name with backtick",
			input:    "my`table",
			expected: "`my``table`",
		},
		{
			name:     "table name with multiple backticks",
			input:    "my`ta`ble",
			expected: "`my``ta``ble`",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "``",
		},
		{
			name:     "table name with spaces",
			input:    "my table",
			expected: "`my table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := base.QuoteBackticks(tt.input)
			if result != tt.expected {
				t.Errorf("base.QuoteBackticks(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestDriverCreation tests driver creation functions.
func TestDriverCreation(t *testing.T) {
	t.Parallel()

	db := &sql.DB{}

	t.Run("New creates driver with default table name", func(t *testing.T) {
		driver := New(db)
		if driver.DB != db {
			t.Error("driver.DB should be set")
		}
		if driver.TableName != "queen_migrations" {
			t.Errorf("driver.TableName = %q; want %q", driver.TableName, "queen_migrations")
		}
		if driver.lockName != "queen_lock_queen_migrations" {
			t.Errorf("driver.lockName = %q; want %q", driver.lockName, "queen_lock_queen_migrations")
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
		if driver.lockName != "queen_lock_custom_migrations" {
			t.Errorf("driver.lockName = %q; want %q", driver.lockName, "queen_lock_custom_migrations")
		}
	})
}

func TestInit(t *testing.T) {
	t.Parallel()

	t.Run("creates migrations table successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `queen_migrations`")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS")).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE `queen_migrations` ADD COLUMN")).
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
		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `queen_migrations`")).
			WillReturnError(createErr)

		err = driver.Init(ctx)
		if !errors.Is(err, createErr) {
			t.Errorf("Init() error = %v; want %v", err, createErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("continues when columns already exist", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `queen_migrations`")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS")).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		}

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() should not fail when columns exist: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestLock(t *testing.T) {
	t.Parallel()

	t.Run("acquires named lock successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Lock() failed: %v", err)
		}

		if driver.conn == nil {
			t.Error("conn should be set after successful lock")
		}

		// Cleanup
		if driver.conn != nil {
			_ = driver.conn.Close()
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles lock timeout", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 100*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles lock NULL result", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		var nullInt *int
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(nullInt))

		err = driver.Lock(ctx, 5*time.Second)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles GET_LOCK query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		lockErr := errors.New("get lock failed")
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WillReturnError(lockErr)

		err = driver.Lock(ctx, 5*time.Second)
		if err == nil {
			t.Error("Lock() should return error when GET_LOCK fails")
		}
	})
}

func TestUnlock(t *testing.T) {
	t.Parallel()

	t.Run("unlocks successfully when locked", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
			WithArgs("queen_lock_queen_migrations").
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() failed: %v", err)
		}

		if driver.conn != nil {
			t.Error("conn should be nil after unlock")
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
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		unlockErr := errors.New("release lock failed")
		mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
			WithArgs("queen_lock_queen_migrations").
			WillReturnError(unlockErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when RELEASE_LOCK fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

// Note: Integration tests that require a real MySQL database are in mysql_integration_test.go
// Run with: go test -tags=integration -v

// setupTestDB creates a test database connection.
// This requires MySQL to be running. Tests will be skipped if MySQL is not available.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	//  3307 to avoid conflicts
	db, err := sql.Open("mysql", "root:test@tcp(localhost:3307)/testdb?parseTime=true&multiStatements=true")
	if err != nil {
		t.Skip("MySQL not available:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skip("MySQL not available:", err)
	}

	cleanup := func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS queen_migrations")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_users")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_posts")
		_ = db.Close()
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
	err = db.QueryRowContext(ctx, "SHOW TABLES LIKE 'queen_migrations'").Scan(&tableName)
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

func TestIntegrationRecordAndGetApplied(t *testing.T) {
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
		UpSQL:   "CREATE TABLE users (id INT)",
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
		UpSQL:   "CREATE TABLE posts (id INT)",
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
		UpSQL:   "CREATE TABLE users (id INT)",
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

	db2, err := sql.Open("mysql", "root:test@tcp(localhost:3307)/testdb?parseTime=true")
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
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255)
			) ENGINE=InnoDB
		`)
		return err
	})
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	var tableName string
	err = db.QueryRowContext(ctx, "SHOW TABLES LIKE 'test_users'").Scan(&tableName)
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

func TestIntegrationFullMigrationCycle(t *testing.T) {
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
				id INT AUTO_INCREMENT PRIMARY KEY,
				email VARCHAR(255) NOT NULL UNIQUE
			) ENGINE=InnoDB
		`,
		DownSQL: `DROP TABLE test_users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE test_posts (
				id INT AUTO_INCREMENT PRIMARY KEY,
				user_id INT NOT NULL,
				title VARCHAR(255),
				FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
			) ENGINE=InnoDB
		`,
		DownSQL: `DROP TABLE test_posts`,
	})

	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up() failed: %v", err)
	}

	var tableCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name IN ('test_users', 'test_posts')").Scan(&tableCount)
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

	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name IN ('test_users', 'test_posts')").Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}
	if tableCount != 0 {
		t.Errorf("expected 0 tables after reset, got %d", tableCount)
	}
}
