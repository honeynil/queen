package mssql

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

// TestQuoteIdentifier tests the identifier quoting function for MSSQL
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
			expected: "[users]",
		},
		{
			name:     "table name with bracket",
			input:    "my]table",
			expected: "[my]]table]",
		},
		{
			name:     "table name with multiple brackets",
			input:    "my]ta]ble",
			expected: "[my]]ta]]ble]",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "[]",
		},
		{
			name:     "table name with spaces",
			input:    "my table",
			expected: "[my table]",
		},
		{
			name:     "table name with dashes",
			input:    "my-table",
			expected: "[my-table]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := base.QuoteBrackets(tt.input)
			if result != tt.expected {
				t.Errorf("base.QuoteBrackets(%q) = %q; want %q", tt.input, result, tt.expected)
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

	t.Run("Config has correct settings", func(t *testing.T) {
		driver := New(db)
		if driver.Config.QuoteIdentifier == nil {
			t.Error("Config.QuoteIdentifier should be set")
		}
		if driver.Config.QuoteIdentifier("test") != "[test]" {
			t.Errorf("Config.QuoteIdentifier should use QuoteBrackets")
		}
		if driver.Config.Placeholder == nil {
			t.Error("Config.Placeholder should be set")
		}
	})
}

// TestInit tests the Init function which creates the migrations table.
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

		mock.ExpectExec(regexp.QuoteMeta("IF OBJECT_ID(N'queen_migrations', N'U') IS NULL")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(regexp.QuoteMeta("IF NOT EXISTS")).
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
		mock.ExpectExec(regexp.QuoteMeta("IF OBJECT_ID(N'queen_migrations', N'U') IS NULL")).
			WillReturnError(createErr)

		err = driver.Init(ctx)
		if !errors.Is(err, createErr) {
			t.Errorf("Init() error = %v; want %v", err, createErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("continues when ALTER statements fail (idempotent)", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectExec(regexp.QuoteMeta("IF OBJECT_ID(N'queen_migrations', N'U') IS NULL")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(regexp.QuoteMeta("IF NOT EXISTS")).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() should not fail even if ALTER statements are ignored: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("uses correct table name", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := NewWithTableName(db, "my_migrations")
		ctx := context.Background()

		mock.ExpectExec(regexp.QuoteMeta("IF OBJECT_ID(N'my_migrations', N'U') IS NULL")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		for i := 0; i < 7; i++ {
			mock.ExpectExec(regexp.QuoteMeta("IF NOT EXISTS")).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}

		err = driver.Init(ctx)
		if err != nil {
			t.Errorf("Init() with custom table name failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

// TestLock tests the Lock function for acquiring exclusive locks.
func TestLock(t *testing.T) {
	t.Parallel()

	t.Run("acquires app lock successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", int(5000)).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Lock() failed: %v", err)
		}

		if driver.conn == nil {
			t.Error("conn should be set after successful lock")
		}

		if driver.conn != nil {
			_ = driver.conn.Close()
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles lock timeout error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", int(100)).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(-1))

		err = driver.Lock(ctx, 100*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles lock canceled error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(-2))

		err = driver.Lock(ctx, 5*time.Second)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles lock deadlock error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(-3))

		err = driver.Lock(ctx, 5*time.Second)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("Lock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles sp_getapplock query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		lockErr := errors.New("get lock failed")
		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WillReturnError(lockErr)

		err = driver.Lock(ctx, 5*time.Second)
		if err == nil {
			t.Error("Lock() should return error when sp_getapplock fails")
		}
	})

	t.Run("uses custom lock name for custom table", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := NewWithTableName(db, "custom_migs")
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_custom_migs", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Lock() with custom table name failed: %v", err)
		}

		if driver.conn != nil {
			_ = driver.conn.Close()
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

// TestUnlock tests the Unlock function for releasing locks.
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

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations").
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

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

	t.Run("handles sp_releaseapplock error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := New(db)
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		unlockErr := errors.New("release lock failed")
		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_queen_migrations").
			WillReturnError(unlockErr)

		err = driver.Unlock(ctx)
		if err == nil {
			t.Error("Unlock() should return error when sp_releaseapplock fails")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles unlock with custom lock name", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := NewWithTableName(db, "custom_migs")
		ctx := context.Background()

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_custom_migs", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Lock(ctx, 5*time.Second)
		if err != nil {
			t.Fatalf("Lock() failed: %v", err)
		}

		mock.ExpectQuery(regexp.QuoteMeta("DECLARE @result INT")).
			WithArgs("queen_lock_custom_migs").
			WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))

		err = driver.Unlock(ctx)
		if err != nil {
			t.Errorf("Unlock() with custom lock name failed: %v", err)
		}

		if driver.conn != nil {
			t.Error("conn should be nil after unlock")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}
