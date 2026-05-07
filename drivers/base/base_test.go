package base

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/honeynil/queen"
)

func TestPlaceholderDollar(t *testing.T) {
	tests := []struct {
		n        int
		expected string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
		{100, "$100"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := PlaceholderDollar(tt.n)
			if result != tt.expected {
				t.Errorf("PlaceholderDollar(%d) = %q; want %q", tt.n, result, tt.expected)
			}
		})
	}
}

func TestPlaceholderQuestion(t *testing.T) {
	for i := 1; i <= 10; i++ {
		t.Run("placeholder", func(t *testing.T) {
			result := PlaceholderQuestion(i)
			if result != "?" {
				t.Errorf("PlaceholderQuestion(%d) = %q; want ?", i, result)
			}
		})
	}
}

func TestQuoteDoubleQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "users", `"users"`},
		{"with quotes", `my"table`, `"my""table"`},
		{"multiple quotes", `my"ta"ble`, `"my""ta""ble"`},
		{"empty", "", `""`},
		{"special chars", "table_123", `"table_123"`},
		{"spaces", "my table", `"my table"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteDoubleQuotes(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteDoubleQuotes(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteBackticks(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "users", "`users`"},
		{"with backtick", "my`table", "`my``table`"},
		{"multiple backticks", "my`ta`ble", "`my``ta``ble`"},
		{"empty", "", "``"},
		{"special chars", "table_123", "`table_123`"},
		{"spaces", "my table", "`my table`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteBackticks(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteBackticks(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseTimeISO8601(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "valid timestamp",
			input:    "2024-01-28 12:34:56",
			expected: time.Date(2024, 1, 28, 12, 34, 56, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "another valid timestamp",
			input:    "2023-12-31 23:59:59",
			expected: time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:    "invalid format",
			input:   "28-01-2024 12:34:56",
			wantErr: true,
		},
		{
			name:    "not a string",
			input:   12345,
			wantErr: true,
		},
		{
			name:    "nil",
			input:   nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseTimeISO8601(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !result.Equal(tt.expected) {
				t.Errorf("ParseTimeISO8601(%v) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteBrackets(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "users", "[users]"},
		{"with bracket", "my]table", "[my]]table]"},
		{"multiple brackets", "my]ta]ble", "[my]]ta]]ble]"},
		{"empty", "", "[]"},
		{"special chars", "table_123", "[table_123]"},
		{"spaces", "my table", "[my table]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteBrackets(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteBrackets(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		quoteChar QuoteChar
		expected  string
	}{
		{"double quotes simple", "users", DoubleQuote, `"users"`},
		{"backticks simple", "users", Backtick, "`users`"},
		{"brackets simple", "users", Bracket, "[users]"},
		{"double quotes with escape", `my"table`, DoubleQuote, `"my""table"`},
		{"backticks with escape", "my`table", Backtick, "`my``table`"},
		{"brackets with escape", "my]table", Bracket, "[my]]table]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdentifier(tt.input, tt.quoteChar)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q, %v) = %q; want %q", tt.input, tt.quoteChar, result, tt.expected)
			}
		})
	}
}

func TestExec(t *testing.T) {
	t.Run("successful transaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE test").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		ctx := context.Background()
		err = driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "CREATE TABLE test")
			return err
		})

		if err != nil {
			t.Errorf("Exec() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("transaction with error triggers rollback", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		testErr := errors.New("test error")
		mock.ExpectBegin()
		mock.ExpectRollback()

		ctx := context.Background()
		err = driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
			return testErr
		})

		if !errors.Is(err, testErr) {
			t.Errorf("Exec() error = %v; want %v", err, testErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("begin transaction error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{DB: db}

		beginErr := errors.New("begin failed")
		mock.ExpectBegin().WillReturnError(beginErr)

		ctx := context.Background()
		err = driver.Exec(ctx, sql.LevelDefault, func(tx *sql.Tx) error {
			return nil
		})

		if !errors.Is(err, beginErr) {
			t.Errorf("Exec() error = %v; want %v", err, beginErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}

	driver := &Driver{DB: db}

	mock.ExpectClose()

	if err := driver.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetApplied(t *testing.T) {
	t.Run("returns applied migrations", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		appliedTime := time.Date(2024, 1, 28, 12, 0, 0, 0, time.UTC)
		rows := sqlmock.NewRows([]string{
			"version", "name", "applied_at", "checksum",
			"applied_by", "duration_ms", "hostname", "environment",
			"action", "status", "error_message",
		}).AddRow(
			"001", "create_users", appliedTime, "abc123",
			"test_user", int64(100), "localhost", "test",
			"apply", "success", nil,
		).AddRow(
			"002", "create_posts", appliedTime.Add(time.Hour), "def456",
			nil, nil, nil, nil,
			nil, nil, nil,
		)

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT version, name, applied_at, checksum`)).
			WillReturnRows(rows)

		ctx := context.Background()
		applied, err := driver.GetApplied(ctx)
		if err != nil {
			t.Fatalf("GetApplied() failed: %v", err)
		}

		if len(applied) != 2 {
			t.Fatalf("expected 2 migrations, got %d", len(applied))
		}

		if applied[0].Version != "001" {
			t.Errorf("version = %q; want %q", applied[0].Version, "001")
		}
		if applied[0].Name != "create_users" {
			t.Errorf("name = %q; want %q", applied[0].Name, "create_users")
		}
		if applied[0].AppliedBy != "test_user" {
			t.Errorf("applied_by = %q; want %q", applied[0].AppliedBy, "test_user")
		}
		if applied[0].DurationMS != 100 {
			t.Errorf("duration_ms = %d; want %d", applied[0].DurationMS, 100)
		}

		if applied[1].Version != "002" {
			t.Errorf("version = %q; want %q", applied[1].Version, "002")
		}
		if applied[1].AppliedBy != "" {
			t.Errorf("applied_by should be empty for null, got %q", applied[1].AppliedBy)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles ParseTime function", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
				ParseTime:       ParseTimeISO8601,
			},
		}

		rows := sqlmock.NewRows([]string{
			"version", "name", "applied_at", "checksum",
			"applied_by", "duration_ms", "hostname", "environment",
			"action", "status", "error_message",
		}).AddRow(
			"001", "test", "2024-01-28 12:00:00", "abc",
			nil, nil, nil, nil,
			nil, nil, nil,
		)

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT version, name, applied_at, checksum`)).
			WillReturnRows(rows)

		ctx := context.Background()
		applied, err := driver.GetApplied(ctx)
		if err != nil {
			t.Fatalf("GetApplied() with ParseTime failed: %v", err)
		}

		if len(applied) != 1 {
			t.Fatalf("expected 1 migration, got %d", len(applied))
		}

		expectedTime := time.Date(2024, 1, 28, 12, 0, 0, 0, time.UTC)
		if !applied[0].AppliedAt.Equal(expectedTime) {
			t.Errorf("applied_at = %v; want %v", applied[0].AppliedAt, expectedTime)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		queryErr := errors.New("query failed")
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT version, name, applied_at, checksum`)).
			WillReturnError(queryErr)

		ctx := context.Background()
		_, err = driver.GetApplied(ctx)
		if !errors.Is(err, queryErr) {
			t.Errorf("GetApplied() error = %v; want %v", err, queryErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestRecord(t *testing.T) {
	t.Run("records migration with metadata", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		migration := &queen.Migration{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users",
		}

		meta := &queen.MigrationMetadata{
			AppliedBy:   "test_user",
			DurationMS:  150,
			Hostname:    "localhost",
			Environment: "test",
			Action:      "apply",
			Status:      "success",
		}

		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "queen_migrations"`)).
			WithArgs(
				"001", "create_users", migration.Checksum(),
				"test_user", int64(150), "localhost", "test",
				"apply", "success", sqlmock.AnyArg(),
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()
		err = driver.Record(ctx, migration, meta)
		if err != nil {
			t.Errorf("Record() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("records migration without metadata", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderQuestion,
				QuoteIdentifier: QuoteBackticks,
			},
		}

		migration := &queen.Migration{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users",
		}

		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `queen_migrations`")).
			WithArgs(
				"001", "create_users", migration.Checksum(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()
		err = driver.Record(ctx, migration, nil)
		if err != nil {
			t.Errorf("Record() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles insert error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		migration := &queen.Migration{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users",
		}

		insertErr := errors.New("insert failed")
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "queen_migrations"`)).
			WillReturnError(insertErr)

		ctx := context.Background()
		err = driver.Record(ctx, migration, nil)
		if !errors.Is(err, insertErr) {
			t.Errorf("Record() error = %v; want %v", err, insertErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestRemove(t *testing.T) {
	t.Run("removes migration successfully", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM "queen_migrations" WHERE version = $1`)).
			WithArgs("001").
			WillReturnResult(sqlmock.NewResult(0, 1))

		ctx := context.Background()
		err = driver.Remove(ctx, "001")
		if err != nil {
			t.Errorf("Remove() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("handles delete error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := &Driver{
			DB:        db,
			TableName: "queen_migrations",
			Config: Config{
				Placeholder:     PlaceholderDollar,
				QuoteIdentifier: QuoteDoubleQuotes,
			},
		}

		deleteErr := errors.New("delete failed")
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM "queen_migrations" WHERE version = $1`)).
			WillReturnError(deleteErr)

		ctx := context.Background()
		err = driver.Remove(ctx, "001")
		if !errors.Is(err, deleteErr) {
			t.Errorf("Remove() error = %v; want %v", err, deleteErr)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestAcquireTableLock(t *testing.T) {
	t.Run("acquires lock successfully on first try", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		config := TableLockConfig{
			CleanupQuery: "DELETE FROM locks WHERE lock_key = ? AND expires_at < ?",
			CheckQuery:   "SELECT COUNT(*) FROM locks WHERE lock_key = ?",
			InsertQuery:  "INSERT INTO locks (lock_key, expires_at, owner_id) VALUES (?, ?, ?)",
			ScanFunc: func(row *sql.Row) (bool, error) {
				var count int
				err := row.Scan(&count)
				return count > 0, err
			},
		}

		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM locks")).
			WithArgs("test_lock", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM locks")).
			WithArgs("test_lock").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO locks")).
			WithArgs("test_lock", sqlmock.AnyArg(), "owner123").
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()
		err = AcquireTableLock(ctx, db, config, "test_lock", "owner123", 5*time.Second)
		if err != nil {
			t.Errorf("AcquireTableLock() failed: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("returns timeout error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		config := TableLockConfig{
			CleanupQuery: "DELETE FROM locks WHERE lock_key = ? AND expires_at < ?",
			CheckQuery:   "SELECT COUNT(*) FROM locks WHERE lock_key = ?",
			InsertQuery:  "INSERT INTO locks (lock_key, expires_at, owner_id) VALUES (?, ?, ?)",
			ScanFunc: func(row *sql.Row) (bool, error) {
				var count int
				err := row.Scan(&count)
				return count > 0, err
			},
		}

		for i := 0; i < 5; i++ {
			mock.ExpectExec(regexp.QuoteMeta("DELETE FROM locks")).
				WithArgs("test_lock", sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 0))

			mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM locks")).
				WithArgs("test_lock").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		}

		ctx := context.Background()
		err = AcquireTableLock(ctx, db, config, "test_lock", "owner123", 100*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Errorf("AcquireTableLock() error = %v; want ErrLockTimeout", err)
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create mock: %v", err)
		}
		defer func() { _ = db.Close() }()

		config := TableLockConfig{
			CleanupQuery: "DELETE FROM locks WHERE lock_key = ? AND expires_at < ?",
			CheckQuery:   "SELECT COUNT(*) FROM locks WHERE lock_key = ?",
			InsertQuery:  "INSERT INTO locks (lock_key, expires_at, owner_id) VALUES (?, ?, ?)",
			ScanFunc: func(row *sql.Row) (bool, error) {
				var count int
				err := row.Scan(&count)
				return count > 0, err
			},
		}

		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM locks")).
			WithArgs("test_lock", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM locks")).
			WithArgs("test_lock").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = AcquireTableLock(ctx, db, config, "test_lock", "owner123", 5*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("AcquireTableLock() error = %v; want context.Canceled", err)
		}
	})
}
