package cli

import (
	"testing"

	"github.com/yaop-labs/queen"
)

func TestResolveBaselineTargetDefaultsToLastMigration(t *testing.T) {
	statuses := []queen.MigrationStatus{
		{Version: "001", Name: "create_users"},
		{Version: "002", Name: "create_posts"},
	}

	index, version, err := resolveBaselineTarget(statuses, "")
	if err != nil {
		t.Fatalf("resolveBaselineTarget returned error: %v", err)
	}
	if index != 1 || version != "002" {
		t.Fatalf("target=(%d, %q), want (1, 002)", index, version)
	}
}

func TestPendingStatusesThroughFiltersApplied(t *testing.T) {
	statuses := []queen.MigrationStatus{
		{Version: "001", Status: queen.StatusApplied},
		{Version: "002", Status: queen.StatusPending},
		{Version: "003", Status: queen.StatusPending},
	}

	got := pendingStatusesThrough(statuses, 1)
	if len(got) != 1 || got[0].Version != "002" {
		t.Fatalf("pendingStatusesThrough()=%v, want only 002", got)
	}
}
