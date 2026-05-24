package cli

import (
	"strings"
	"testing"

	"github.com/yaop-labs/queen"
)

func TestOutputExplainTableUsesUserFacingDestructiveWarning(t *testing.T) {
	app := &App{config: &Config{}}
	plan := &queen.MigrationPlan{
		Version:       "002",
		Name:          "drop_users",
		Status:        "pending",
		Type:          queen.MigrationTypeSQL,
		Direction:     queen.DirectionDown,
		SQL:           "DROP TABLE users;",
		HasRollback:   true,
		IsDestructive: true,
		Checksum:      "abc123",
		Warnings:      []string{"Destructive operation"},
	}

	out := captureStdout(t, func() {
		app.outputExplainTable(plan)
	})
	if !strings.Contains(out, "Destructive:   yes") {
		t.Fatalf("output missing destructive flag:\n%s", out)
	}
	if !strings.Contains(out, "WARNING: Warnings:") {
		t.Fatalf("output missing warning heading:\n%s", out)
	}
	if strings.Contains(out, "WARNING: YES") || strings.Contains(out, "WARNING: WARNING") {
		t.Fatalf("output contains placeholder warning text:\n%s", out)
	}
}
