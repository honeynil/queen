// Package sqlite provides a SQLite driver for Queen migrations.
//
// This driver supports SQLite 3.8+ and is ideal for embedded databases,
// development, testing, and single-server applications.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/mattn/go-sqlite3"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/sqlite"
//	)
//
//	db, _ := sql.Open("sqlite3", "myapp.db")
//	driver := sqlite.New(db)
//	q := queen.New(driver)
//
// # Database File
//
// SQLite stores the database in a single file. Common patterns:
//
//   - Persistent: "myapp.db" or "/path/to/database.db"
//   - In-memory: ":memory:" (lost when connection closes)
//   - Temporary: "" (empty string, deleted when closed)
//
// For production use, always use a persistent file path.
//
// # Locking Mechanism
//
// Unlike PostgreSQL and MySQL, SQLite is a file-based database with different
// locking characteristics:
//
//   - SQLite uses database-level locks, not connection-level locks
//   - This driver uses BEGIN EXCLUSIVE transaction for migration locking
//   - The lock is automatically released when the transaction commits/rolls back
//   - Only one writer can access the database at a time (by design)
//
// # WAL Mode (Recommended)
//
// For better concurrent read/write performance, enable WAL (Write-Ahead Logging):
//
//	db, _ := sql.Open("sqlite3", "myapp.db?_journal_mode=WAL")
//
// WAL mode allows readers to access the database while a migration is running,
// though only one migration can run at a time.
//
// # Compatibility
//
//   - SQLite 3.8+ (uses WITHOUT ROWID optimization where available)
//   - Works on all platforms (Linux, macOS, Windows)
//   - Single file, no server required
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for SQLite.
//
// The driver is thread-safe for concurrent reads, but SQLite's database-level
// locking means only one write operation (migration) can occur at a time.
// This is handled automatically by PRAGMA locking_mode=EXCLUSIVE.
type Driver struct {
	db        *sql.DB
	tableName string
}

// New creates a new SQLite driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("sqlite3", "myapp.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := sqlite.New(db)
//
// For better performance with concurrent reads, use WAL mode:
//
//	db, err := sql.Open("sqlite3", "myapp.db?_journal_mode=WAL")
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new SQLite driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database file, or when you want to customize the table name
// for organizational purposes.
//
// Example:
//
//	driver := sqlite.NewWithTableName(db, "my_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		db:        db,
		tableName: tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version: TEXT PRIMARY KEY - unique migration version
//   - name: TEXT NOT NULL - human-readable migration name
//   - applied_at: TEXT - ISO8601 timestamp when migration was applied
//   - checksum: TEXT - hash of migration content for validation
//
// This method is idempotent and safe to call multiple times.
//
// Note: SQLite doesn't have a native TIMESTAMP type. We use TEXT with
// ISO8601 format (YYYY-MM-DD HH:MM:SS) which sorts correctly and is human-readable.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT (datetime('now')),
			checksum TEXT NOT NULL
		) WITHOUT ROWID
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// GetApplied returns all applied migrations sorted by applied_at in ascending order.
//
// This is used by Queen to determine which migrations have already been applied
// and which are pending.
//
// Note: SQLite stores timestamps as TEXT in ISO8601 format. We parse them back
// to time.Time for consistency with other drivers.
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
		var appliedAtStr string
		if err := rows.Scan(&a.Version, &a.Name, &appliedAtStr, &a.Checksum); err != nil {
			return nil, err
		}

		// Parse ISO8601 timestamp
		// SQLite default format: "YYYY-MM-DD HH:MM:SS"
		appliedAt, err := time.Parse("2006-01-02 15:04:05", appliedAtStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse applied_at timestamp: %w", err)
		}
		a.AppliedAt = appliedAt

		applied = append(applied, a)
	}

	return applied, rows.Err()
}

// Record marks a migration as applied in the database.
//
// This should be called after successfully executing a migration's up function.
// The checksum is automatically computed from the migration content.
//
// The timestamp is automatically set by SQLite to the current time.
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

// Lock acquires an exclusive database lock to prevent concurrent migrations.
//
// SQLite uses database-level locking. This driver uses PRAGMA locking_mode=EXCLUSIVE
// to acquire an exclusive lock on the entire database file. This prevents any other
// connections from writing to the database until the lock is released.
//
// The lock is connection-based (similar to PostgreSQL advisory locks) rather than
// transaction-based, allowing individual migration transactions to be created and
// committed independently.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Set busy_timeout for lock acquisition attempts
	_, err := d.db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", timeout.Milliseconds()))
	if err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// Set EXCLUSIVE locking mode - this locks the database file
	// preventing other connections from acquiring locks
	_, err = d.db.ExecContext(ctx, "PRAGMA locking_mode = EXCLUSIVE")
	if err != nil {
		return fmt.Errorf("failed to set locking mode: %w", err)
	}

	// Force the lock to be acquired immediately by starting and committing a write transaction
	// This ensures we actually acquire the lock now, not lazily later
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		if strings.Contains(err.Error(), "database is locked") {
			return queen.ErrLockTimeout
		}
		return fmt.Errorf("failed to begin lock transaction: %w", err)
	}

	// Perform a write operation to force exclusive lock acquisition
	_, err = tx.ExecContext(ctx, "CREATE TEMP TABLE IF NOT EXISTS _queen_lock_test (id INTEGER)")
	if err != nil {
		_ = tx.Rollback()
		if strings.Contains(err.Error(), "database is locked") {
			return queen.ErrLockTimeout
		}
		return fmt.Errorf("failed to acquire exclusive lock: %w", err)
	}

	// Commit the transaction - we don't need to keep it open
	// The EXCLUSIVE locking mode remains in effect for the connection
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit lock transaction: %w", err)
	}

	return nil
}

// Unlock releases the migration lock.
//
// This resets the locking mode to NORMAL, allowing other connections to
// write to the database.
//
// This should be called in a defer statement after acquiring the lock.
// It's safe to call even if the lock wasn't acquired.
func (d *Driver) Unlock(ctx context.Context) error {
	// Reset locking mode to NORMAL
	_, err := d.db.ExecContext(ctx, "PRAGMA locking_mode = NORMAL")
	if err != nil {
		return fmt.Errorf("failed to reset locking mode: %w", err)
	}

	// Execute a transaction to force the locking mode change to take effect
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin unlock transaction: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit unlock transaction: %w", err)
	}

	return nil
}

// Exec executes a function within a transaction.
//
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
//
// This provides ACID guarantees for migration execution.
//
// Note: SQLite supports nested transactions using SAVEPOINT, but this
// driver uses simple transactions for compatibility and simplicity.
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
// If you're using a file-based database (not :memory:), the database file
// persists after closing. For in-memory databases, all data is lost.
func (d *Driver) Close() error {
	return d.db.Close()
}

// quoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
//
// In SQLite, identifiers can be quoted with double quotes ("), square brackets [],
// or backticks `. This function uses double quotes as it's the SQL standard.
//
// Examples:
//   - users -> "users"
//   - my"table -> "my""table"
func quoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}
