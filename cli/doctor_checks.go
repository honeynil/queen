package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

// checkDatabaseConnection verifies database connectivity.
func checkDatabaseConnection(ctx context.Context, q *queen.Queen) DoctorResult {
	// loading applied migrations doubles as a connectivity probe
	_, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Database Connection",
			Status:  statusFail,
			Message: "Failed to connect to database",
			Details: err.Error(),
		}
	}

	return DoctorResult{
		Check:   "Database Connection",
		Status:  statusPass,
		Message: "Database is accessible",
	}
}

// checkMigrationTable verifies the migration table exists and is accessible.
func checkMigrationTable(ctx context.Context, q *queen.Queen) DoctorResult {
	applied, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Migration Table",
			Status:  statusFail,
			Message: "Migration table is not accessible",
			Details: err.Error(),
		}
	}

	return DoctorResult{
		Check:   "Migration Table",
		Status:  statusPass,
		Message: fmt.Sprintf("Migration table exists with %d records", len(applied)),
	}
}

// checkChecksums validates that applied migrations haven't been modified.
func checkChecksums(ctx context.Context, q *queen.Queen) DoctorResult {
	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Checksum Validation",
			Status:  statusFail,
			Message: "Failed to validate checksums",
			Details: err.Error(),
		}
	}

	modified := make([]string, 0)
	for _, s := range statuses {
		if s.Status == queen.StatusModified {
			modified = append(modified, s.Version)
		}
	}

	if len(modified) > 0 {
		return DoctorResult{
			Check:    "Checksum Validation",
			Status:   statusFail,
			Message:  fmt.Sprintf("%d migration(s) have been modified after being applied", len(modified)),
			Details:  fmt.Sprintf("Modified versions: %s", strings.Join(modified, ", ")),
			Severity: "error",
		}
	}

	return DoctorResult{
		Check:   "Checksum Validation",
		Status:  statusPass,
		Message: "All applied migrations match their checksums",
	}
}

// checkGaps checks for migration gaps.
func checkGaps(ctx context.Context, q *queen.Queen) DoctorResult {
	gaps, err := q.DetectGaps(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Gap Detection",
			Status:  statusFail,
			Message: "Failed to detect gaps",
			Details: err.Error(),
		}
	}

	if len(gaps) == 0 {
		return DoctorResult{
			Check:   "Gap Detection",
			Status:  statusPass,
			Message: "No gaps detected",
		}
	}

	errors := 0
	warningCount := 0
	for _, gap := range gaps {
		if gap.Severity == "error" {
			errors++
		} else {
			warningCount++
		}
	}

	status := statusWarning
	if errors > 0 {
		status = statusFail
	}

	return DoctorResult{
		Check:   "Gap Detection",
		Status:  status,
		Message: fmt.Sprintf("Found %d gap(s): %d errors, %d warnings", len(gaps), errors, warningCount),
		Details: "Run 'queen gap detect' for details",
	}
}

// checkRegistrationSync checks if code and database are in sync.
func checkRegistrationSync(ctx context.Context, q *queen.Queen) DoctorResult {
	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Registration Sync",
			Status:  statusFail,
			Message: "Failed to check registration sync",
			Details: err.Error(),
		}
	}

	applied, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Registration Sync",
			Status:  statusFail,
			Message: "Failed to get applied migrations",
			Details: err.Error(),
		}
	}

	registered := make(map[string]bool)
	for _, s := range statuses {
		registered[s.Version] = true
	}

	unregistered := make([]string, 0)
	for _, a := range applied {
		if !registered[a.Version] {
			unregistered = append(unregistered, a.Version)
		}
	}

	if len(unregistered) > 0 {
		return DoctorResult{
			Check:    "Registration Sync",
			Status:   statusFail,
			Message:  fmt.Sprintf("%d applied migration(s) are not registered in code", len(unregistered)),
			Details:  fmt.Sprintf("Unregistered: %s", strings.Join(unregistered, ", ")),
			Severity: "error",
		}
	}

	return DoctorResult{
		Check:   "Registration Sync",
		Status:  statusPass,
		Message: "All applied migrations are registered in code",
	}
}
