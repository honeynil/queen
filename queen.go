// Package queen provides a lightweight database migration library for Go.
//
// Queen follows the principle "migrations are code, not files". Instead of
// managing separate .sql files, you define migrations directly in Go code.
// This approach provides type safety, better IDE support, and easier testing
// compared to traditional file-based migration tools.
//
// # Basic Usage
//
// Create a Queen instance with a database driver and register migrations:
//
//	db, _ := sql.Open("pgx", "postgres://localhost/myapp?sslmode=disable")
//	driver := postgres.New(db)
//	q := queen.New(driver)
//	defer q.Close()
//
//	q.MustAdd(queen.M{
//	    Version: "001",
//	    Name:    "create_users",
//	    UpSQL:   "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))",
//	    DownSQL: "DROP TABLE users",
//	})
//
//	q.MustAdd(queen.M{
//	    Version: "002",
//	    Name:    "add_users_name",
//	    UpSQL:   "ALTER TABLE users ADD COLUMN name VARCHAR(255)",
//	    DownSQL: "ALTER TABLE users DROP COLUMN name",
//	})
//
//	if err := q.Up(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//
// # SQL Migrations
//
// SQL migrations use UpSQL and DownSQL fields:
//
//	q.Add(queen.M{
//	    Version: "001",
//	    Name:    "create_users",
//	    UpSQL:   "CREATE TABLE users (id INT, email VARCHAR(255))",
//	    DownSQL: "DROP TABLE users",
//	})
//
// # Go Function Migrations
//
// For complex logic that can't be expressed in SQL, use UpFunc and DownFunc:
//
//	q.Add(queen.M{
//	    Version:        "002",
//	    Name:           "normalize_emails",
//	    ManualChecksum: "v1",
//	    UpFunc: func(ctx context.Context, tx *sql.Tx) error {
//	        rows, err := tx.QueryContext(ctx, "SELECT id, email FROM users")
//	        if err != nil {
//	            return err
//	        }
//	        defer rows.Close()
//
//	        for rows.Next() {
//	            var id int
//	            var email string
//	            rows.Scan(&id, &email)
//
//	            normalized := strings.ToLower(strings.TrimSpace(email))
//	            tx.ExecContext(ctx, "UPDATE users SET email = $1 WHERE id = $2",
//	                normalized, id)
//	        }
//	        return rows.Err()
//	    },
//	})
//
// When using Go functions, always set ManualChecksum to track changes.
// Update it whenever you modify the function (e.g., "v1" -> "v2").
//
// # Testing
//
// Queen provides built-in testing helpers:
//
//	func TestMigrations(t *testing.T) {
//	    db := setupTestDB(t)
//	    driver := postgres.New(db)
//	    q := queen.NewTest(t, driver) // Auto-cleanup on test end
//
//	    q.MustAdd(queen.M{
//	        Version: "001",
//	        Name:    "create_users",
//	        UpSQL:   "CREATE TABLE users (id INT)",
//	        DownSQL: "DROP TABLE users",
//	    })
//
//	    q.TestUpDown() // Tests both up and down migrations
//	}
//
// # Natural Sorting
//
// Queen uses natural sorting for migration versions, so "1" < "2" < "10" < "100".
// You can use any versioning scheme: sequential numbers ("001", "002"),
// prefixes ("users_001", "posts_001"), or semantic versions ("v1.0.0").
//
// # Migration Operations
//
// Common operations include:
//
//	q.Up(ctx)              // Apply all pending migrations
//	q.UpSteps(ctx, 3)      // Apply next 3 migrations
//	q.Down(ctx, 1)         // Rollback last migration
//	q.Reset(ctx)           // Rollback all migrations
//	statuses, _ := q.Status(ctx)  // Get migration status
//	q.Validate(ctx)        // Validate migrations
package queen

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	naturalsort "github.com/honeynil/queen/internal/sort"
)

// Queen manages database migrations.
type Queen struct {
	driver     Driver
	migrations []*Migration
	config     *Config

	// Track which migrations have been applied (cache)
	applied map[string]*Applied
}

// Config configures Queen behavior.
type Config struct {
	// TableName for migration tracking. Default: "queen_migrations"
	TableName string

	// LockTimeout for acquiring migration lock. Default: 30 minutes
	LockTimeout time.Duration

	// SkipLock disables locking (not recommended for production). Default: false
	SkipLock bool
}

// DefaultConfig returns default settings: "queen_migrations" table, 30min lock timeout.
func DefaultConfig() *Config {
	return &Config{
		TableName:   "queen_migrations",
		LockTimeout: 30 * time.Minute,
		SkipLock:    false,
	}
}

// New creates a Queen instance with default configuration.
func New(driver Driver) *Queen {
	return NewWithConfig(driver, DefaultConfig())
}

// NewWithConfig creates a Queen instance with custom settings.
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

// Add registers a migration after validation.
// Returns ErrVersionConflict if version already exists.
func (q *Queen) Add(m M) error {
	if err := m.Validate(); err != nil {
		return err
	}

	for _, existing := range q.migrations {
		if existing.Version == m.Version {
			return fmt.Errorf("%w: %s", ErrVersionConflict, m.Version)
		}
	}

	// Store pointer to prevent mutation after registration
	migration := m
	q.migrations = append(q.migrations, &migration)

	return nil
}

// MustAdd is like Add but panics on error.
// Use during initialization when registration must succeed.
func (q *Queen) MustAdd(m M) {
	if err := q.Add(m); err != nil {
		panic(err)
	}
}

// Up applies all pending migrations.
// Equivalent to UpSteps(ctx, 0).
func (q *Queen) Up(ctx context.Context) error {
	return q.UpSteps(ctx, 0)
}

// UpSteps applies up to n pending migrations.
// If n <= 0, applies all pending migrations.
func (q *Queen) UpSteps(ctx context.Context, n int) error {
	if q.driver == nil {
		return ErrNoDriver
	}

	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			// Unlock uses background context to complete even if parent context is cancelled.
			// Unlock errors are non-critical and safely ignored.
			_ = q.driver.Unlock(context.Background())
		}()
	}

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	pending := q.getPending()
	if len(pending) == 0 {
		return nil
	}

	if n > 0 && n < len(pending) {
		pending = pending[:n]
	}

	for _, m := range pending {
		if err := q.applyMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, err)
		}
	}

	return nil
}

// Down rolls back the last n migrations.
// If n <= 0, rolls back only the last migration.
func (q *Queen) Down(ctx context.Context, n int) error {
	if n <= 0 {
		n = 1
	}

	if q.driver == nil {
		return ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			_ = q.driver.Unlock(context.Background())
		}()
	}

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil
	}

	if n > len(applied) {
		n = len(applied)
	}

	toRollback := applied[:n]

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

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	if !q.config.SkipLock {
		if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
			return err
		}
		defer func() {
			_ = q.driver.Unlock(context.Background())
		}()
	}

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil
	}

	// Don't call Down() to avoid double-locking
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

	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

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
			if applied.Checksum != m.Checksum() && m.Checksum() != noChecksumMarker {
				status.Status = StatusModified
			}
		}

		statuses[i] = status
	}

	return statuses, nil
}

// Validate checks for duplicate versions, invalid migrations, and checksum mismatches.
func (q *Queen) Validate(ctx context.Context) error {
	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	// Validate prevents race conditions when migrations are registered concurrently
	seen := make(map[string]bool)
	for _, m := range q.migrations {
		if seen[m.Version] {
			return fmt.Errorf("%w: duplicate version %s", ErrVersionConflict, m.Version)
		}
		seen[m.Version] = true

		if err := m.Validate(); err != nil {
			return fmt.Errorf("invalid migration %s: %w", m.Version, err)
		}
	}

	if q.driver != nil {
		if err := q.driver.Init(ctx); err != nil {
			return err
		}

		if err := q.loadApplied(ctx); err != nil {
			return err
		}

		for _, m := range q.migrations {
			if applied, ok := q.applied[m.Version]; ok {
				if applied.Checksum != m.Checksum() && m.Checksum() != noChecksumMarker {
					return fmt.Errorf("%w: migration %s (expected %s, got %s)",
						ErrChecksumMismatch, m.Version, applied.Checksum, m.Checksum())
				}
			}
		}
	}

	return nil
}

// Close releases database resources.
func (q *Queen) Close() error {
	if q.driver != nil {
		return q.driver.Close()
	}
	return nil
}

// loadApplied caches applied migrations from database.
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

// getPending returns unapplied migrations sorted by version.
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

// getAppliedMigrations returns applied migrations sorted newest-first.
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
