// Package postgres provides a PostgreSQL driver for Queen migrations.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for PostgreSQL.
type Driver struct {
	db        *sql.DB
	tableName string
	lockID    int64
}

// New creates a new PostgreSQL driver.
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new PostgreSQL driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		db:        db,
		tableName: tableName,
		lockID:    hashTableName(tableName), // Unique lock ID based on table name
	}
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			checksum VARCHAR(64) NOT NULL
		)
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// GetApplied returns all applied migrations sorted by applied_at.
func (d *Driver) GetApplied(ctx context.Context) ([]queen.Applied, error) {
	query := fmt.Sprintf(`
		SELECT version, name, applied_at, checksum
		FROM %s
		ORDER BY applied_at ASC
	`, quoteIdentifier(d.tableName))

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var applied []queen.Applied
	for rows.Next() {
		var a queen.Applied
		if err := rows.Scan(&a.Version, &a.Name, &a.AppliedAt, &a.Checksum); err != nil {
			return nil, err
		}
		applied = append(applied, a)
	}

	return applied, rows.Err()
}

// Record marks a migration as applied.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (version, name, checksum)
		VALUES ($1, $2, $3)
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}

// Remove removes a migration record (for rollback).
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = $1
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, version)
	return err
}

// Lock acquires an advisory lock to prevent concurrent migrations.
// PostgreSQL advisory locks are automatically released when the connection closes
// or when explicitly unlocked.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Set lock timeout
	_, err := d.db.ExecContext(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", timeout.Milliseconds()))
	if err != nil {
		return err
	}

	// Try to acquire advisory lock
	var acquired bool
	err = d.db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", d.lockID).Scan(&acquired)
	if err != nil {
		return err
	}

	if !acquired {
		return queen.ErrLockTimeout
	}

	return nil
}

// Unlock releases the advisory lock.
func (d *Driver) Unlock(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", d.lockID)
	return err
}

// Exec executes a function within a transaction.
func (d *Driver) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		// Ignore rollback error, return original error
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close closes the database connection.
func (d *Driver) Close() error {
	return d.db.Close()
}

// hashTableName creates a unique int64 hash from the table name for advisory locks.
// This ensures different migration tables use different locks.
func hashTableName(name string) int64 {
	var hash int64
	for i, c := range name {
		hash = hash*31 + int64(c) + int64(i)
	}
	return hash
}

// quoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
// In PostgreSQL, identifiers are quoted with double quotes.
func quoteIdentifier(name string) string {
	// Replace any existing double quotes with two double quotes (escaping)
	// and wrap the identifier in double quotes
	escaped := ""
	for _, c := range name {
		if c == '"' {
			escaped += "\"\""
		} else {
			escaped += string(c)
		}
	}
	return `"` + escaped + `"`
}
