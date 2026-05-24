// Package mock provides an in-memory mock driver for testing Queen without a real database.
//
// This driver uses SQLite in-memory database to support both SQL and Go function migrations.
// SQL migrations are executed but data is not persisted (exists only in memory).
//
// For integration testing with persistent data, use a real database driver instead
// (e.g., postgres, mysql, sqlite with file).
package mock

import (
	"context"
	"database/sql"
	"sort"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
)

// Driver is an in-memory mock implementation of queen.Driver for testing.
type Driver struct {
	mu        sync.Mutex
	db        *sql.DB
	applied   map[string]queen.Applied
	locked    bool
	initErr   error
	lockErr   error
	recordErr error
}

// New creates a new mock driver with an in-memory SQLite database.
//
// The in-memory database allows SQL migrations to be executed for testing,
// but data is not persisted and is lost when the driver is closed.
func New() *Driver {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic("mock driver: failed to create in-memory database: " + err.Error())
	}

	return &Driver{
		db:      db,
		applied: make(map[string]queen.Applied),
		locked:  false,
	}
}

// SetInitError makes Init return the specified error.
func (d *Driver) SetInitError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initErr = err
}

// SetLockError makes Lock return the specified error.
func (d *Driver) SetLockError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lockErr = err
}

// SetRecordError makes Record return the specified error.
func (d *Driver) SetRecordError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordErr = err
}

// Init initializes the mock driver.
func (d *Driver) Init(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.initErr != nil {
		return d.initErr
	}

	return nil
}

// GetApplied returns all applied migrations.
func (d *Driver) GetApplied(ctx context.Context) ([]queen.Applied, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]queen.Applied, 0, len(d.applied))
	for _, a := range d.applied {
		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AppliedAt.Before(result[j].AppliedAt)
	})

	return result, nil
}

// Record marks a migration as applied.
func (d *Driver) Record(ctx context.Context, m *queen.Migration, meta *queen.MigrationMetadata) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.recordErr != nil {
		return d.recordErr
	}

	applied := queen.Applied{
		Version:   m.Version,
		Name:      m.Name,
		AppliedAt: time.Now(),
		Checksum:  m.Checksum(),
	}

	if meta != nil {
		applied.AppliedBy = meta.AppliedBy
		applied.DurationMS = meta.DurationMS
		applied.Hostname = meta.Hostname
		applied.Environment = meta.Environment
		applied.Action = meta.Action
		applied.Status = meta.Status
		applied.ErrorMessage = meta.ErrorMessage
	}

	d.applied[m.Version] = applied

	return nil
}

// Remove removes a migration record.
func (d *Driver) Remove(ctx context.Context, version string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.applied, version)
	return nil
}

// Lock acquires a lock.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.lockErr != nil {
		return d.lockErr
	}

	if d.locked {
		return queen.ErrLockTimeout
	}

	d.locked = true
	return nil
}

// Unlock releases the lock.
func (d *Driver) Unlock(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.locked = false
	return nil
}

// Exec executes a function within a real SQLite transaction with the specified isolation level.
//
// This allows SQL migrations to be executed against the in-memory database.
func (d *Driver) Exec(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(*sql.Tx) error) error {
	txOpts := &sql.TxOptions{
		Isolation: isolationLevel,
	}

	tx, err := d.db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close closes the in-memory database connection.
func (d *Driver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// IsLocked returns whether the driver is currently locked (for testing).
func (d *Driver) IsLocked() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.locked
}

// AppliedCount returns the number of applied migrations (for testing).
func (d *Driver) AppliedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.applied)
}

// HasVersion returns whether a specific version has been applied (for testing).
func (d *Driver) HasVersion(version string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, exists := d.applied[version]
	return exists
}

// Reset clears all applied migrations metadata (for testing).
//
// Note: This only clears the migration tracking metadata. It does NOT
// reset the in-memory database schema or data. To reset the database,
// create a new mock driver instance.
func (d *Driver) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applied = make(map[string]queen.Applied)
	d.locked = false
}
