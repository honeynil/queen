// Package queen provides a database migration library for Go.
//
// Queen allows you to define migrations in code (not separate files),
// supports both SQL and Go function migrations, and provides excellent
// testing helpers for validating your migrations.
//
// Basic usage:
//
//	db, _ := sql.Open("postgres", "...")
//	driver := postgres.New(db)
//	q := queen.New(driver)
//
//	q.Add(queen.M{
//	    Version: "001",
//	    Name:    "create_users",
//	    UpSQL:   "CREATE TABLE users (id SERIAL PRIMARY KEY)",
//	    DownSQL: "DROP TABLE users",
//	})
//
//	if err := q.Up(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
package queen

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	naturalsort "github.com/honeynil/queen/internal/sort"
)

// Queen is the main migration manager.
// It holds registered migrations and orchestrates their execution.
type Queen struct {
	driver     Driver
	migrations []*Migration
	config     *Config

	// Track which migrations have been applied (cache)
	applied map[string]*Applied
}

// Config holds configuration options for Queen.
type Config struct {
	// TableName is the name of the table used to track migrations.
	// Default: "queen_migrations"
	TableName string

	// LockTimeout is how long to wait for the migration lock.
	// Default: 30 minutes
	LockTimeout time.Duration

	// SkipLock disables migration locking (not recommended for prod env).
	// Default: false
	SkipLock bool
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		TableName:   "queen_migrations",
		LockTimeout: 30 * time.Minute,
		SkipLock:    false,
	}
}

// New creates a new Queen instance with the given driver and default configuration.
func New(driver Driver) *Queen {
	return NewWithConfig(driver, DefaultConfig())
}

// NewWithConfig creates a new Queen instance with custom configuration.
func NewWithConfig(driver Driver, config *Config) *Queen {
	if config == nil {
		config = DefaultConfig()
	}

	// Validate and fix config values
	if config.TableName == "" {
		config.TableName = "queen_migrations"
	}
	if config.LockTimeout <= 0 {
		config.LockTimeout = 30 * time.Minute
	}

	return &Queen{
		driver:     driver,
		migrations: make([]*Migration, 0),
		config:     config,
		applied:    make(map[string]*Applied),
	}
}

// Add registers a new migration.
// If a migration with the same version already exists, it returns ErrVersionConflict.
// The migration is validated before being added.
func (q *Queen) Add(m M) error {
	if err := m.Validate(); err != nil {
		return err
	}

	// Check for version conflict
	for _, existing := range q.migrations {
		if existing.Version == m.Version {
			return fmt.Errorf("%w: %s", ErrVersionConflict, m.Version)
		}
	}

	// Make a copy to avoid external modifications
	migration := m
	q.migrations = append(q.migrations, &migration)

	return nil
}

// MustAdd is like Add but panics on error.
// Useful for migration registration at package init time.
func (q *Queen) MustAdd(m M) {
	if err := q.Add(m); err != nil {
		panic(err)
	}
}

// Up applies all pending migrations in order.
// It acquires a lock, loads applied migrations, and applies any pending ones.
func (q *Queen) Up(ctx context.Context) error {
	return q.UpSteps(ctx, 0) // 0 means "all"
}

// UpSteps applies up to n pending migrations.
// If n is 0 or negative, all pending migrations are applied.
func (q *Queen) UpSteps(ctx context.Context, n int) error {
	if q.driver == nil {
		return ErrNoDriver
	}

	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	// Initialize driver (creates tracking table if needed)
	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	// Acquire lock
	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			// Use background context for unlock to ensure it completes even if ctx is cancelled
			unlockCtx := context.Background()
			q.driver.Unlock(unlockCtx)
		}()
	}

	// Load applied migrations
	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	// Get pending migrations
	pending := q.getPending()
	if len(pending) == 0 {
		return nil // Nothing to do
	}

	// Limit number of migrations if n > 0
	if n > 0 && n < len(pending) {
		pending = pending[:n]
	}

	// Apply pending migrations
	for _, m := range pending {
		if err := q.applyMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, err)
		}
	}

	return nil
}

// Down rolls back the last n migrations.
// If n is 0 or negative, only the last migration is rolled back.
func (q *Queen) Down(ctx context.Context, n int) error {
	if n <= 0 {
		n = 1
	}

	if q.driver == nil {
		return ErrNoDriver
	}

	// Initialize driver
	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	// Acquire lock
	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			// Use background context for unlock to ensure it completes even if ctx is cancelled
			unlockCtx := context.Background()
			q.driver.Unlock(unlockCtx)
		}()
	}

	// Load applied migrations
	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	// Get applied migrations in reverse order
	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil // Nothing to rollback
	}

	// Limit number of migrations
	if n > len(applied) {
		n = len(applied)
	}

	// Reverse the list to rollback from newest to oldest
	toRollback := applied[:n]

	// Rollback migrations
	for _, m := range toRollback {
		if !m.HasRollback() {
			return newMigrationError(m.Version, m.Name, fmt.Errorf("no down migration defined"))
		}

		if err := q.rollbackMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, err)
		}
	}

	return nil
}

// Reset rolls back all applied migrations.
func (q *Queen) Reset(ctx context.Context) error {
	if q.driver == nil {
		return ErrNoDriver
	}

	// Initialize driver
	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	// Acquire lock
	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			// Use background context for unlock to ensure it completes even if ctx is cancelled
			unlockCtx := context.Background()
			q.driver.Unlock(unlockCtx)
		}()
	}

	// Load applied migrations
	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	// Get applied migrations in reverse order
	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil // Nothing to rollback
	}

	// Rollback all migrations (don't call Down to avoid double-locking)
	for _, m := range applied {
		if !m.HasRollback() {
			return newMigrationError(m.Version, m.Name, fmt.Errorf("no down migration defined"))
		}

		if err := q.rollbackMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, err)
		}
	}

	return nil
}

// Status returns the status of all registered migrations.
func (q *Queen) Status(ctx context.Context) ([]MigrationStatus, error) {
	if q.driver == nil {
		return nil, ErrNoDriver
	}

	// Initialize driver
	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	// Load applied migrations
	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

	// Build status for each migration
	statuses := make([]MigrationStatus, len(q.migrations))
	for i, m := range q.migrations {
		status := MigrationStatus{
			Version:     m.Version,
			Name:        m.Name,
			Checksum:    m.Checksum(),
			HasRollback: m.HasRollback(),
			Destructive: m.IsDestructive(),
			Status:      StatusPending,
		}

		if applied, ok := q.applied[m.Version]; ok {
			status.Status = StatusApplied
			status.AppliedAt = &applied.AppliedAt

			// Check for checksum mismatch
			if applied.Checksum != m.Checksum() && m.Checksum() != "no-checksum-go-func" {
				status.Status = StatusModified
			}
		}

		statuses[i] = status
	}

	return statuses, nil
}

// Validate validates all registered migrations.
// It checks for:
// - Duplicate versions
// - Invalid migrations
// - Checksum mismatches with applied migrations
func (q *Queen) Validate(ctx context.Context) error {
	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	// Check for duplicates (shouldn't happen if Add() is used correctly)
	seen := make(map[string]bool)
	for _, m := range q.migrations {
		if seen[m.Version] {
			return fmt.Errorf("%w: duplicate version %s", ErrVersionConflict, m.Version)
		}
		seen[m.Version] = true

		// Validate each migration
		if err := m.Validate(); err != nil {
			return fmt.Errorf("invalid migration %s: %w", m.Version, err)
		}
	}

	// Check checksum mismatches if driver is available
	if q.driver != nil {
		if err := q.driver.Init(ctx); err != nil {
			return err
		}

		if err := q.loadApplied(ctx); err != nil {
			return err
		}

		for _, m := range q.migrations {
			if applied, ok := q.applied[m.Version]; ok {
				if applied.Checksum != m.Checksum() && m.Checksum() != "no-checksum-go-func" {
					return fmt.Errorf("%w: migration %s (expected %s, got %s)",
						ErrChecksumMismatch, m.Version, applied.Checksum, m.Checksum())
				}
			}
		}
	}

	return nil
}

// Close closes the database connection.
func (q *Queen) Close() error {
	if q.driver != nil {
		return q.driver.Close()
	}
	return nil
}

// loadApplied loads applied migrations from the database into memory.
func (q *Queen) loadApplied(ctx context.Context) error {
	applied, err := q.driver.GetApplied(ctx)
	if err != nil {
		return err
	}

	q.applied = make(map[string]*Applied)
	for i := range applied {
		q.applied[applied[i].Version] = &applied[i]
	}

	return nil
}

// getPending returns migrations that haven't been applied yet, sorted by version.
func (q *Queen) getPending() []*Migration {
	pending := make([]*Migration, 0)

	for _, m := range q.migrations {
		if _, applied := q.applied[m.Version]; !applied {
			pending = append(pending, m)
		}
	}

	// Sort by version using natural sort
	sort.Slice(pending, func(i, j int) bool {
		return naturalsort.Compare(pending[i].Version, pending[j].Version) < 0
	})

	return pending
}

// getAppliedMigrations returns applied migrations in reverse order (newest first).
func (q *Queen) getAppliedMigrations() []*Migration {
	applied := make([]*Migration, 0)

	for _, m := range q.migrations {
		if _, ok := q.applied[m.Version]; ok {
			applied = append(applied, m)
		}
	}

	// Sort by version using natural sort, then reverse
	sort.Slice(applied, func(i, j int) bool {
		return naturalsort.Compare(applied[i].Version, applied[j].Version) > 0
	})

	return applied
}

// applyMigration applies a single migration.
func (q *Queen) applyMigration(ctx context.Context, m *Migration) error {
	// Execute migration in transaction
	err := q.driver.Exec(ctx, func(tx *sql.Tx) error {
		return m.executeUp(ctx, tx)
	})
	if err != nil {
		return err
	}

	// Record in database
	if err := q.driver.Record(ctx, m); err != nil {
		return err
	}

	// Update cache
	q.applied[m.Version] = &Applied{
		Version:   m.Version,
		Name:      m.Name,
		AppliedAt: time.Now(),
		Checksum:  m.Checksum(),
	}

	return nil
}

// rollbackMigration rolls back a single migration.
func (q *Queen) rollbackMigration(ctx context.Context, m *Migration) error {
	// Execute rollback in transaction
	err := q.driver.Exec(ctx, func(tx *sql.Tx) error {
		return m.executeDown(ctx, tx)
	})
	if err != nil {
		return err
	}

	// Remove from database
	if err := q.driver.Remove(ctx, m.Version); err != nil {
		return err
	}

	// Update cache
	delete(q.applied, m.Version)

	return nil
}
