package queen

import (
	"context"
	"database/sql"
	"time"
)

// Driver is the interface that database-specific drivers must implement.
// It abstracts away database-specific migration tracking and locking.
type Driver interface {
	// Init initializes the driver and creates the migrations tracking table if needed.
	// This should be called before any other operations.
	Init(ctx context.Context) error

	// GetApplied returns all migrations that have been applied to the database.
	// The returned slice should be sorted by applied time in ascending order.
	GetApplied(ctx context.Context) ([]Applied, error)

	// Record marks a migration as applied in the database.
	// This should be called after successfully executing a migration.
	Record(ctx context.Context, m *Migration) error

	// Remove removes a migration record from the database.
	// This should be called after successfully rolling back a migration.
	Remove(ctx context.Context, version string) error

	// Lock acquires an exclusive lock to prevent concurrent migrations.
	// If the lock cannot be acquired within the specified timeout, it returns ErrLockTimeout.
	// The lock is automatically released when the context is cancelled.
	Lock(ctx context.Context, timeout time.Duration) error

	// Unlock releases the migration lock.
	// This should be called in a defer statement after acquiring the lock.
	Unlock(ctx context.Context) error

	// Exec executes a function within a transaction.
	// If the function returns an error, the transaction is rolled back.
	// Otherwise, the transaction is committed.
	Exec(ctx context.Context, fn func(*sql.Tx) error) error

	// Close closes the database connection.
	Close() error
}

// Applied represents a migration that has been applied to the database.
// This is returned by Driver.GetApplied().
type Applied struct {
	// Version is the unique version identifier of the migration.
	Version string

	// Name is the human-readable name of the migration.
	Name string

	// AppliedAt is when the migration was applied.
	AppliedAt time.Time

	// Checksum is the hash of the migration content at the time it was applied.
	Checksum string
}
