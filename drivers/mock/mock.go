// Package mock provides an in-memory mock driver for testing Queen without a real database.
//
// IMPORTANT: Mock driver only works with Go function migrations (UpFunc/DownFunc).
// SQL migrations (UpSQL/DownSQL) require a real database connection and will panic
// when used with the mock driver.
//
// For testing SQL migrations, use a real database (e.g., postgres in Docker) or
// use the testcontainers library.
package mock

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/honeynil/queen"
)

// Driver is an in-memory mock implementation of queen.Driver for testing.
type Driver struct {
	mu       sync.Mutex
	applied  map[string]queen.Applied
	locked   bool
	initErr  error
	lockErr  error
	recordErr error
}

// New creates a new mock driver.
func New() *Driver {
	return &Driver{
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

	// Already initialized (applied map exists)
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

	// Sort by applied time
	sort.Slice(result, func(i, j int) bool {
		return result[i].AppliedAt.Before(result[j].AppliedAt)
	})

	return result, nil
}

// Record marks a migration as applied.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.recordErr != nil {
		return d.recordErr
	}

	d.applied[m.Version] = queen.Applied{
		Version:   m.Version,
		Name:      m.Name,
		AppliedAt: time.Now(),
		Checksum:  m.Checksum(),
	}

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

// Exec executes a function (mock doesn't actually use transactions).
func (d *Driver) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	// Mock driver doesn't have real transactions, so we pass nil
	// The function should handle nil tx gracefully in tests
	return fn(nil)
}

// Close closes the mock driver (no-op).
func (d *Driver) Close() error {
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

// Reset clears all applied migrations (for testing).
func (d *Driver) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applied = make(map[string]queen.Applied)
	d.locked = false
}

// simulateTx is a helper that simulates transaction behavior for testing
type simulateTx struct{}

func (tx *simulateTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("mock tx: Exec not implemented")
}

func (tx *simulateTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	// Successful execution (for mock purposes)
	return &mockResult{}, nil
}

type mockResult struct{}

func (r *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r *mockResult) RowsAffected() (int64, error) { return 1, nil }
