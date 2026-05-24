package demodata

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/sqlite"
)

func TestMainTUIDemoStartsWithGapsAndTapReadyPendingMigration(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:queen_demodata_test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	q := queen.New(sqlite.New(db))
	defer func() { _ = q.Close() }()

	opts := MainTUIOptions()
	opts.StepDelay = 0
	Register(q, opts)

	if err := q.UpSteps(ctx, 3); err != nil {
		t.Fatalf("seed demo migrations: %v", err)
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	wantApplied := map[string]bool{"001": true, "002": true, "004": true}
	firstPending := ""
	for _, status := range statuses {
		if wantApplied[status.Version] && status.Status != queen.StatusApplied {
			t.Fatalf("version %s status=%s, want applied", status.Version, status.Status)
		}
		if firstPending == "" && status.Status == queen.StatusPending {
			firstPending = status.Version
		}
	}
	if firstPending != IntegratedTapStartVersion {
		t.Fatalf("first pending version=%q, want %q", firstPending, IntegratedTapStartVersion)
	}

	gaps, err := q.DetectGaps(ctx)
	if err != nil {
		t.Fatalf("detect gaps: %v", err)
	}
	gotGaps := map[string]queen.GapType{}
	for _, gap := range gaps {
		gotGaps[gap.Version] = gap.Type
	}
	for _, version := range []string{"003", "007"} {
		if gotGaps[version] != queen.GapTypeNumbering {
			t.Fatalf("gap %s=%q, want numbering; all gaps: %#v", version, gotGaps[version], gaps)
		}
	}
}
