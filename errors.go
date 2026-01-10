package queen

import (
	"errors"
	"fmt"
)

// Common errors returned by Queen operations.
var (
	ErrNoMigrations      = errors.New("no migrations registered")
	ErrVersionConflict   = errors.New("version conflict")
	ErrMigrationNotFound = errors.New("migration not found")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrLockTimeout       = errors.New("lock timeout")
	ErrNoDriver          = errors.New("driver not initialized")
	ErrInvalidMigration  = errors.New("invalid migration")
	ErrAlreadyApplied    = errors.New("migration already applied")
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
