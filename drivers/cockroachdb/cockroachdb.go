// Package cockroachdb provides a CockroachDB driver for Queen migrations.
package cockroachdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
)

// Driver implements the queen.Driver interface for CockroachDB.
type Driver struct {
	base.Driver
	lockTableName string
	lockKey       string
	ownerID       string
}

// New creates a new CockroachDB driver.
func New(db *sql.DB) (*Driver, error) {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new CockroachDB driver with a custom table name.
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
				Placeholder:     base.PlaceholderDollar,
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
			version		VARCHAR(255) PRIMARY KEY,
			name		VARCHAR(255) NOT NULL,
			applied_at  TIMESTAMP	 DEFAULT CURRENT_TIMESTAMP,
			checksum	VARCHAR(64)  NOT NULL,
			applied_by	VARCHAR(255),
			duration_ms	BIGINT,
			hostname	VARCHAR(255),
			environment	VARCHAR(50),
			action		VARCHAR(20) DEFAULT 'apply',
			status		VARCHAR(20) DEFAULT 'success',
			error_message TEXT
		)
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, migrationsQuery); err != nil {
		return err
	}

	migrations := []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS applied_by VARCHAR(255)`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS duration_ms BIGINT`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS hostname VARCHAR(255)`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS environment VARCHAR(50)`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS action VARCHAR(20) DEFAULT 'apply'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'success'`, d.Config.QuoteIdentifier(d.TableName)),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS error_message TEXT`, d.Config.QuoteIdentifier(d.TableName)),
	}

	for _, migration := range migrations {
		_, _ = d.DB.ExecContext(ctx, migration)
	}

	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key	VARCHAR(255)	PRIMARY KEY,
			acquired_at	TIMESTAMP		DEFAULT CURRENT_TIMESTAMP,
			expires_at	TIMESTAMP		NOT NULL,
			owner_id	VARCHAR(64)		NOT NULL
		)
	`, d.Config.QuoteIdentifier(d.lockTableName))

	_, err := d.DB.ExecContext(ctx, lockQuery)
	return err
}

// Lock uses a table row with expiry as the migration guard.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	cfg := base.TableLockConfig{
		CleanupQuery: fmt.Sprintf(
			"DELETE FROM %s WHERE lock_key = $1 AND expires_at < now()",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		CleanupArgs: func(lockKey string, _ time.Time) []any {
			return []any{lockKey}
		},
		CheckQuery: fmt.Sprintf(
			"SELECT 1 FROM %s WHERE lock_key = $1 AND expires_at >= now() LIMIT 1",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		InsertQuery: fmt.Sprintf(
			"INSERT INTO %s (lock_key, expires_at, owner_id) VALUES ($1, $2, $3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		ScanFunc: func(row *sql.Row) (bool, error) {
			var exists int
			err := row.Scan(&exists)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
			return exists != 0, nil
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
		"DELETE FROM %s WHERE lock_key = $1 AND owner_id = $2",
		d.Config.QuoteIdentifier(d.lockTableName),
	)

	_, err := d.DB.ExecContext(ctx, unlockQuery, d.lockKey, d.ownerID)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s': %w",
			d.lockKey, d.TableName, err)
	}
	return err
}

// QuoteIdentifier quotes a SQL identifier.
func QuoteIdentifier(name string) string {
	return base.QuoteDoubleQuotes(name)
}
