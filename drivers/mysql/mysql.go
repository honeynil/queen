// Package mysql provides a MySQL driver for Queen migrations.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
)

// Driver implements the queen.Driver interface for MySQL.
type Driver struct {
	base.Driver
	lockName string
	conn     *sql.Conn
}

// New creates a new MySQL driver.
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new MySQL driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteBackticks,
				ParseTime:       nil,
			},
		},
		lockName: "queen_lock_" + tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			checksum VARCHAR(64) NOT NULL,
			applied_by VARCHAR(255),
			duration_ms BIGINT,
			hostname VARCHAR(255),
			environment VARCHAR(50),
			action VARCHAR(20) DEFAULT 'apply',
			status VARCHAR(20) DEFAULT 'success',
			error_message TEXT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	migrations := []struct {
		column string
		query  string
	}{
		{"applied_by", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN applied_by VARCHAR(255)`, d.Config.QuoteIdentifier(d.TableName))},
		{"duration_ms", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN duration_ms BIGINT`, d.Config.QuoteIdentifier(d.TableName))},
		{"hostname", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN hostname VARCHAR(255)`, d.Config.QuoteIdentifier(d.TableName))},
		{"environment", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN environment VARCHAR(50)`, d.Config.QuoteIdentifier(d.TableName))},
		{"action", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN action VARCHAR(20) DEFAULT 'apply'`, d.Config.QuoteIdentifier(d.TableName))},
		{"status", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN status VARCHAR(20) DEFAULT 'success'`, d.Config.QuoteIdentifier(d.TableName))},
		{"error_message", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN error_message TEXT`, d.Config.QuoteIdentifier(d.TableName))},
	}

	for _, migration := range migrations {
		var columnExists int
		checkQuery := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`
		if err := d.DB.QueryRowContext(ctx, checkQuery, d.TableName, migration.column).Scan(&columnExists); err != nil {
			continue
		}

		if columnExists == 0 {
			if _, err := d.DB.ExecContext(ctx, migration.query); err != nil {
				continue
			}
		}
	}

	return nil
}

// Lock acquires a named lock to prevent concurrent migrations.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	var result sql.NullInt64
	query := "SELECT GET_LOCK(?, ?)"

	err = conn.QueryRowContext(ctx, query, d.lockName, int(timeout.Seconds())).Scan(&result)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	if !result.Valid || result.Int64 != 1 {
		_ = conn.Close()
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s'",
			queen.ErrLockTimeout, d.lockName, d.TableName)
	}

	d.conn = conn
	return nil
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(ctx context.Context) error {
	if d.conn == nil {
		return nil
	}

	var result sql.NullInt64
	err := d.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", d.lockName).Scan(&result)

	if err != nil {
		return fmt.Errorf("failed to release named lock '%s' for table '%s': %w",
			d.lockName, d.TableName, err)
	}

	_ = d.conn.Close()
	d.conn = nil

	return nil
}
