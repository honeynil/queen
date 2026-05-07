package cli

import (
	"testing"
	"time"

	"github.com/honeynil/queen"
)

func TestCountStatuses(t *testing.T) {
	t.Parallel()

	now := time.Now()
	statuses := []queen.MigrationStatus{
		{Version: "001", Status: queen.StatusApplied, AppliedAt: &now},
		{Version: "002", Status: queen.StatusApplied, AppliedAt: &now},
		{Version: "003", Status: queen.StatusPending},
		{Version: "004", Status: queen.StatusModified, AppliedAt: &now},
		{Version: "005", Status: queen.StatusPending},
	}

	applied, pending, modified := countStatuses(statuses)

	if applied != 2 {
		t.Errorf("applied = %d, want 2", applied)
	}
	if pending != 2 {
		t.Errorf("pending = %d, want 2", pending)
	}
	if modified != 1 {
		t.Errorf("modified = %d, want 1", modified)
	}
}

func TestOutputStatusTable(t *testing.T) {
	t.Parallel()

	app := &App{config: &Config{}}
	now := time.Now()

	statuses := []queen.MigrationStatus{
		{
			Version:     "001",
			Name:        "create_users",
			Status:      queen.StatusApplied,
			AppliedAt:   &now,
			Checksum:    "abc123def456",
			HasRollback: true,
		},
		{
			Version:     "002",
			Name:        "add_email",
			Status:      queen.StatusPending,
			Checksum:    "xyz789",
			HasRollback: false,
		},
	}

	err := app.outputStatusTable(statuses)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOutputStatusJSON(t *testing.T) {
	t.Parallel()

	app := &App{config: &Config{}}
	now := time.Now()

	statuses := []queen.MigrationStatus{
		{
			Version:     "001",
			Name:        "create_users",
			Status:      queen.StatusApplied,
			AppliedAt:   &now,
			Checksum:    "abc123",
			HasRollback: true,
		},
		{
			Version:     "002",
			Name:        "add_email",
			Status:      queen.StatusPending,
			Checksum:    "xyz789",
			HasRollback: false,
		},
	}

	err := app.outputStatusJSON(statuses)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
