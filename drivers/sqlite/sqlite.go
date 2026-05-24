// Package sqlite provides a SQLite driver for Queen migrations.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
)

// Driver implements the queen.Driver interface for SQLite.
type Driver struct {
	base.Driver
}

// New creates a new SQLite driver.
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new SQLite driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       base.ParseTimeISO8601,
			},
		},
	}
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT (datetime('now')),
			checksum TEXT NOT NULL,
			applied_by TEXT,
			duration_ms INTEGER,
			hostname TEXT,
			environment TEXT,
			action TEXT DEFAULT 'apply',
			status TEXT DEFAULT 'success',
			error_message TEXT
		) WITHOUT ROWID
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	migrations := []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN applied_by TEXT`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN duration_ms INTEGER`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN hostname TEXT`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN environment TEXT`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN action TEXT DEFAULT 'apply'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN status TEXT DEFAULT 'success'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN error_message TEXT`, d.Config.QuoteIdentifier(d.TableName)),
	}

	for _, migration := range migrations {
		_, _ = d.DB.ExecContext(ctx, migration)
	}

	return nil
}

// Lock uses SQLite's busy timeout and immediate transaction path.
// It is intended for local SQLite use, not distributed coordination.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	_, err := d.DB.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", timeout.Milliseconds()))
	if err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	_, err = d.DB.ExecContext(ctx, "PRAGMA locking_mode = EXCLUSIVE")
	if err != nil {
		return fmt.Errorf("failed to set locking mode: %w", err)
	}

	_, err = d.DB.ExecContext(ctx, "BEGIN IMMEDIATE")
	if err != nil {
		if strings.Contains(err.Error(), "database is locked") {
			return fmt.Errorf("%w: failed to acquire exclusive lock for table '%s'",
				queen.ErrLockTimeout, d.TableName)
		}
		return fmt.Errorf("failed to begin immediate transaction: %w", err)
	}

	_, err = d.DB.ExecContext(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit lock transaction: %w", err)
	}

	return nil
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(ctx context.Context) error {
	_, err := d.DB.ExecContext(ctx, "PRAGMA locking_mode = NORMAL")
	if err != nil {
		return fmt.Errorf("failed to reset locking mode for table '%s': %w",
			d.TableName, err)
	}

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin unlock transaction for table '%s': %w",
			d.TableName, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit unlock transaction for table '%s': %w",
			d.TableName, err)
	}

	return nil
}
