package cli

import (
	"context"
	"testing"

	"github.com/yaop-labs/queen"
)

func TestCheckRollbackCycleAppliesRollsBackAndReapplies(t *testing.T) {
	ctx := context.Background()
	q, closeDB := newSQLiteQueen(t)
	defer closeDB()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE users;",
	})

	var summary checkSummary
	checkRollbackCycle(ctx, q, &summary)
	if summary.failed != 0 {
		t.Fatalf("rollback cycle failed: %+v", summary)
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(statuses) != 1 || statuses[0].Status != queen.StatusApplied {
		t.Fatalf("statuses after rollback cycle = %+v, want one applied migration", statuses)
	}
}

func TestCheckRollbackCycleRequiresCleanDatabase(t *testing.T) {
	ctx := context.Background()
	q, closeDB := newSQLiteQueen(t)
	defer closeDB()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE users;",
	})
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	var summary checkSummary
	checkRollbackCycle(ctx, q, &summary)
	if summary.failed != 1 || summary.exitCode != 6 {
		t.Fatalf("rollback cycle summary = %+v, want one failure with exit code 6", summary)
	}
}
