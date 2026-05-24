// Package clickhouse provides a ClickHouse driver for Queen migrations.
package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
)

// Driver implements the queen.Driver interface for ClickHouse.
type Driver struct {
	base.Driver
	lockTableName string
	lockKey       string
	ownerID       string
}

// New creates a new ClickHouse driver.
func New(db *sql.DB) (*Driver, error) {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new ClickHouse driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) (*Driver, error) {
	ownerID, err := base.GenerateOwnerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate lock owner ID: %w", err)
	}

	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       nil,
			},
		},
		lockTableName: tableName + "_lock",
		lockKey:       "migration_lock",
		ownerID:       ownerID,
	}, nil
}

// Init creates the migrations tracking table and lock table if they don't exist.
func (d *Driver) Init(ctx context.Context) error {
	migrationsQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version      String,
			name         LowCardinality(String),
			applied_at   DateTime64(3)     DEFAULT now64(3),
			checksum     String            DEFAULT '',
			applied_by   String            DEFAULT '',
			duration_ms  Int64             DEFAULT 0,
			hostname     String            DEFAULT '',
			environment  LowCardinality(String) DEFAULT '',
			action       LowCardinality(String) DEFAULT 'apply',
			status       LowCardinality(String) DEFAULT 'success',
			error_message String           DEFAULT ''
		)
		ENGINE = ReplacingMergeTree()
		ORDER BY version
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, migrationsQuery); err != nil {
		return err
	}

	migrations := []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS applied_by String DEFAULT ''`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS duration_ms Int64 DEFAULT 0`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS hostname String DEFAULT ''`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS environment LowCardinality(String) DEFAULT ''`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS action LowCardinality(String) DEFAULT 'apply'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS status LowCardinality(String) DEFAULT 'success'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS error_message String DEFAULT ''`, d.Config.QuoteIdentifier(d.TableName)),
	}

	for _, migration := range migrations {
		_, _ = d.DB.ExecContext(ctx, migration)
	}

	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key    LowCardinality(String),
			acquired_at DateTime64(3)     DEFAULT now64(3),
			expires_at  DateTime64(3),
			owner_id    String
		)
		ENGINE = ReplacingMergeTree()
		ORDER BY lock_key
		TTL expires_at + INTERVAL 10 SECOND DELETE
	`, d.Config.QuoteIdentifier(d.lockTableName))

	_, err := d.DB.ExecContext(ctx, lockQuery)
	return err
}

// Lock uses a ClickHouse table as a best-effort migration guard.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	cfg := base.TableLockConfig{
		CleanupQuery: fmt.Sprintf(
			"ALTER TABLE %s DELETE WHERE lock_key = ? AND expires_at < now64(3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		CleanupArgs: func(lockKey string, _ time.Time) []any {
			return []any{lockKey}
		},
		CheckQuery: fmt.Sprintf(
			"SELECT count(*) FROM %s FINAL WHERE lock_key = ? AND expires_at >= now64(3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		InsertQuery: fmt.Sprintf(
			"INSERT INTO %s (lock_key, expires_at, owner_id) VALUES (?, ?, ?)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		ScanFunc: func(row *sql.Row) (bool, error) {
			var count int64
			if err := row.Scan(&count); err != nil {
				return false, err
			}
			return count > 0, nil
		},
	}

	err := base.AcquireTableLock(ctx, d.DB, cfg, d.lockKey, d.ownerID, timeout)
	if errors.Is(err, queen.ErrLockTimeout) {
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s'",
			queen.ErrLockTimeout, d.lockKey, d.lockTableName)
	}
	return err
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(ctx context.Context) error {
	unlockQuery := fmt.Sprintf(
		"ALTER TABLE %s DELETE WHERE lock_key = ? AND owner_id = ?",
		d.Config.QuoteIdentifier(d.lockTableName),
	)

	_, err := d.DB.ExecContext(ctx, unlockQuery, d.lockKey, d.ownerID)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s': %w",
			d.lockKey, d.TableName, err)
	}
	return err
}
