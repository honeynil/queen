package queen

import "time"

// Status represents the current state of a migration.
type Status int

const (
	// StatusPending indicates the migration has not been applied yet.
	StatusPending Status = iota

	// StatusApplied indicates the migration has been successfully applied.
	StatusApplied

	// StatusModified indicates the migration has been applied,
	// but its content has changed (checksum mismatch).
	StatusModified
)

// String returns a human-readable representation of the status.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusApplied:
		return "applied"
	case StatusModified:
		return "modified"
	default:
		return "unknown"
	}
}

// MigrationStatus contains detailed information about a migration's current state.
// This is returned by Queen.Status().
type MigrationStatus struct {
	// Version is the unique version identifier of the migration.
	Version string

	// Name is the human-readable name of the migration.
	Name string

	// Status indicates whether the migration is pending, applied, or modified.
	Status Status

	// AppliedAt is when the migration was applied (nil if not applied).
	AppliedAt *time.Time

	// Checksum is the current checksum of the migration.
	Checksum string

	// HasRollback indicates if the migration has a down migration.
	HasRollback bool

	// Destructive indicates if the down migration contains destructive operations.
	Destructive bool
}
