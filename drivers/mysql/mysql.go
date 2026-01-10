// Package mysql provides a MySQL driver for Queen migrations.
//
// This driver supports MySQL 5.7+ and MariaDB 10.2+. It uses MySQL's GET_LOCK()
// function for distributed locking to prevent concurrent migrations.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/go-sql-driver/mysql"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/mysql"
//	)
//
//	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/dbname?parseTime=true")
//	driver := mysql.New(db)
//	q := queen.New(driver)
//
// # Connection String Requirements
//
// The connection string MUST include parseTime=true to properly handle TIMESTAMP columns:
//
//	"user:password@tcp(localhost:3306)/dbname?parseTime=true"
//
// # Locking Mechanism
//
// MySQL doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// GET_LOCK() which creates a named lock that's automatically released when the
// connection closes or RELEASE_LOCK() is called.
//
// The lock name is derived from the migrations table name to ensure different
// migration tables use different locks.
//
// # Compatibility
//
//   - MySQL 5.7+ (uses GET_LOCK with timeout)
//   - MariaDB 10.2+ (uses GET_LOCK with timeout)
//   - Older versions may work but are not officially supported
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for MySQL.
//
// The driver is thread-safe and can be used concurrently by multiple goroutines.
// However, Queen already handles locking to prevent concurrent migrations.
type Driver struct {
	db        *sql.DB
	tableName string
	lockName  string
}

// New creates a new MySQL driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db?parseTime=true")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := mysql.New(db)
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new MySQL driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name for
// organizational purposes.
//
// Example:
//
//	driver := mysql.NewWithTableName(db, "my_custom_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		db:        db,
		tableName: tableName,
		lockName:  "queen_lock_" + tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version: VARCHAR(255) PRIMARY KEY - unique migration version
//   - name: VARCHAR(255) NOT NULL - human-readable migration name
//   - applied_at: TIMESTAMP - when the migration was applied
//   - checksum: VARCHAR(64) - hash of migration content for validation
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			checksum VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// GetApplied returns all applied migrations sorted by applied_at in ascending order.
//
// This is used by Queen to determine which migrations have already been applied
// and which are pending.
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
	defer func() { _ = rows.Close() }()

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

// Record marks a migration as applied in the database.
//
// This should be called after successfully executing a migration's up function.
// The checksum is automatically computed from the migration content.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (version, name, checksum)
		VALUES (?, ?, ?)
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}

// Remove removes a migration record from the database.
//
// This should be called after successfully rolling back a migration's down function.
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = ?
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, version)
	return err
}

// Lock acquires a named lock to prevent concurrent migrations.
//
// MySQL uses GET_LOCK() which creates a named lock. The lock is automatically
// released when the connection closes or when Unlock() is called.
//
// The lock name is based on the migrations table name, so different migration
// tables will use different locks.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// GET_LOCK returns:
	// 1 if the lock was obtained successfully
	// 0 if the attempt timed out
	// NULL if an error occurred
	var result sql.NullInt64
	query := "SELECT GET_LOCK(?, ?)"
	err := d.db.QueryRowContext(ctx, query, d.lockName, int(timeout.Seconds())).Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	if !result.Valid || result.Int64 != 1 {
		return queen.ErrLockTimeout
	}

	return nil
}

// Unlock releases the migration lock.
//
// This should be called in a defer statement after acquiring the lock.
// It's safe to call even if the lock wasn't acquired.
func (d *Driver) Unlock(ctx context.Context) error {
	// RELEASE_LOCK returns:
	// 1 if the lock was released
	// 0 if the lock was not held by this thread
	// NULL if the named lock did not exist
	var result sql.NullInt64
	query := "SELECT RELEASE_LOCK(?)"
	err := d.db.QueryRowContext(ctx, query, d.lockName).Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	// We don't check the result because RELEASE_LOCK might return 0 or NULL
	// if the lock was already released (e.g., connection closed), which is fine
	return nil
}

// Exec executes a function within a transaction.
//
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
//
// This provides ACID guarantees for migration execution.
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
//
// Any locks held by this connection will be automatically released.
func (d *Driver) Close() error {
	return d.db.Close()
}

// quoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
//
// In MySQL, identifiers are quoted with backticks (`). This function also escapes
// any existing backticks in the identifier by doubling them.
//
// Examples:
//   - users -> `users`
//   - my`table -> `my“table`
func quoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}
