package queen

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/honeynil/queen/internal/checksum"
)

// MigrationFunc is a function that executes a migration using a transaction.
// It receives a context and a transaction, and should return an error if the migration fails.
type MigrationFunc func(ctx context.Context, tx *sql.Tx) error

// Migration represents a single database migration.
// It can be defined using SQL strings, Go functions, or both.
//
// For SQL migrations, use UpSQL and DownSQL fields.
// For Go function migrations, use UpFunc and DownFunc fields.
//
// The Version field must be unique across all migrations.
// Queen uses natural sorting, so "001", "002", "010" work correctly.
// You can also use prefixes like "user_001", "post_001" for modular organization.
type Migration struct {
	// Version is a unique identifier for this migration.
	// Examples: "001", "002", "user_001", "v1.0.0"
	Version string

	// Name is a human-readable description of the migration.
	// Examples: "create_users", "add_email_index"
	Name string

	// UpSQL is the SQL statement to apply the migration.
	// Used for simple SQL migrations.
	UpSQL string

	// DownSQL is the SQL statement to rollback the migration.
	// Optional but recommended for safe rollbacks.
	DownSQL string

	// UpFunc is a Go function to apply the migration.
	// Used for complex migrations that need programmatic logic.
	UpFunc MigrationFunc

	// DownFunc is a Go function to rollback the migration.
	// Optional but recommended for safe rollbacks.
	DownFunc MigrationFunc

	// ManualChecksum is an optional manual checksum for Go function migrations.
	// When using UpFunc/DownFunc, set this to track migration changes.
	// Example: "v1", "v2", or descriptive like "normalize-emails-v1"
	// If not set, checksum validation will be skipped for Go functions.
	ManualChecksum string

	// computed checksum cache
	checksum     string
	checksumOnce sync.Once
}

// M is a convenient alias for Migration, used in registration:
//
//	q.Add(queen.M{
//	    Version: "001",
//	    Name: "create_users",
//	    UpSQL: "CREATE TABLE users...",
//	    DownSQL: "DROP TABLE users",
//	})
type M = Migration

// Validate checks if the migration is valid.
// A migration must have either UpSQL or UpFunc defined.
func (m *Migration) Validate() error {
	if m.Version == "" {
		return ErrInvalidMigration
	}

	if m.Name == "" {
		return ErrInvalidMigration
	}

	// Must have at least one Up method
	if m.UpSQL == "" && m.UpFunc == nil {
		return ErrInvalidMigration
	}

	return nil
}

// Checksum returns a unique hash of the migration content.
// For SQL migrations, it hashes UpSQL and DownSQL.
// For Go function migrations with ManualChecksum, it uses that value.
// For Go function migrations without ManualChecksum, it returns a special marker.
func (m *Migration) Checksum() string {
	m.checksumOnce.Do(func() {
		// If manual checksum is provided, use it
		if m.ManualChecksum != "" {
			m.checksum = m.ManualChecksum
			return
		}

		// For SQL migrations, calculate checksum
		if m.UpSQL != "" || m.DownSQL != "" {
			m.checksum = checksum.Calculate(m.UpSQL, m.DownSQL)
			return
		}

		// For Go functions without manual checksum, use special marker
		m.checksum = "no-checksum-go-func"
	})

	return m.checksum
}

// HasRollback returns true if the migration has a down migration.
func (m *Migration) HasRollback() bool {
	return m.DownSQL != "" || m.DownFunc != nil
}

// IsDestructive returns true if the migration contains potentially destructive operations.
// This checks for DROP TABLE, DROP DATABASE, TRUNCATE, etc.
// Only checks DownSQL, as Up migrations are assumed to be constructive.
func (m *Migration) IsDestructive() bool {
	if m.DownSQL == "" {
		return false
	}

	sql := strings.ToUpper(m.DownSQL)

	destructiveKeywords := []string{
		"DROP TABLE",
		"DROP DATABASE",
		"DROP SCHEMA",
		"TRUNCATE",
	}

	for _, keyword := range destructiveKeywords {
		if strings.Contains(sql, keyword) {
			return true
		}
	}

	return false
}

// executeUp executes the migration's Up operation within a transaction.
func (m *Migration) executeUp(ctx context.Context, tx *sql.Tx) error {
	if m.UpFunc != nil {
		return m.UpFunc(ctx, tx)
	}

	if m.UpSQL != "" {
		_, err := tx.ExecContext(ctx, m.UpSQL)
		return err
	}

	return ErrInvalidMigration
}

// executeDown executes the migration's Down operation within a transaction.
func (m *Migration) executeDown(ctx context.Context, tx *sql.Tx) error {
	if m.DownFunc != nil {
		return m.DownFunc(ctx, tx)
	}

	if m.DownSQL != "" {
		_, err := tx.ExecContext(ctx, m.DownSQL)
		return err
	}

	return ErrInvalidMigration
}
