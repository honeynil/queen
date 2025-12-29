package queen

import (
	"errors"
	"fmt"
)

// Common errors that can be returned by Queen operations.
var (
	// ErrNoMigrations is returned when no migrations are registered.
	ErrNoMigrations = errors.New("no migrations registered")

	// ErrVersionConflict is returned when duplicate version is detected.
	ErrVersionConflict = errors.New("version conflict")

	// ErrMigrationNotFound is returned when a migration version doesn't exist.
	ErrMigrationNotFound = errors.New("migration not found")

	// ErrChecksumMismatch is returned when a migration's checksum doesn't match.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// ErrLockTimeout is returned when unable to acquire lock within timeout.
	ErrLockTimeout = errors.New("lock timeout")

	// ErrNoDriver is returned when driver is not initialized.
	ErrNoDriver = errors.New("driver not initialized")

	// ErrInvalidMigration is returned when migration validation fails.
	ErrInvalidMigration = errors.New("invalid migration")

	// ErrAlreadyApplied is returned when trying to apply already applied migration.
	ErrAlreadyApplied = errors.New("migration already applied")
)

// MigrationError wraps an error with migration context.
type MigrationError struct {
	Version string
	Name    string
	Err     error
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("migration %s (%s): %v", e.Version, e.Name, e.Err)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}

// newMigrationError creates a new MigrationError.
func newMigrationError(version, name string, err error) error {
	return &MigrationError{
		Version: version,
		Name:    name,
		Err:     err,
	}
}
