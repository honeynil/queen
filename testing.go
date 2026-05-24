package queen

import (
	"context"
	"testing"
)

// TestHelper provides testing utilities for migrations.
type TestHelper struct {
	*Queen
	t   *testing.T
	ctx context.Context
}

func NewTest(t *testing.T, driver Driver) *TestHelper {
	t.Helper()

	q := New(driver)
	ctx := context.Background()

	if err := q.driver.Init(ctx); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	t.Cleanup(func() {
		_ = q.Close()
	})

	return &TestHelper{
		Queen: q,
		t:     t,
		ctx:   ctx,
	}
}

func (th *TestHelper) TestUpDown() {
	th.t.Helper()

	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to apply migrations: %v", err)
	}

	applied, err := th.driver.GetApplied(th.ctx)
	if err != nil {
		th.t.Fatalf("Failed to get applied migrations: %v", err)
	}

	count := len(applied)
	if count == 0 {
		th.t.Fatal("No migrations were applied")
	}

	if err := th.Reset(th.ctx); err != nil {
		th.t.Fatalf("Failed to rollback migrations: %v", err)
	}

	applied, err = th.driver.GetApplied(th.ctx)
	if err != nil {
		th.t.Fatalf("Failed to get applied migrations after rollback: %v", err)
	}

	if len(applied) != 0 {
		th.t.Fatalf("Expected 0 migrations after rollback, got %d", len(applied))
	}

	th.t.Logf("✓ Successfully applied and rolled back %d migrations", count)
}

func (th *TestHelper) MustUp() {
	th.t.Helper()
	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to apply migrations: %v", err)
	}
}

func (th *TestHelper) MustDown(n int) {
	th.t.Helper()
	if err := th.Down(th.ctx, n); err != nil {
		th.t.Fatalf("Failed to rollback migrations: %v", err)
	}
}

func (th *TestHelper) MustReset() {
	th.t.Helper()
	if err := th.Reset(th.ctx); err != nil {
		th.t.Fatalf("Failed to reset migrations: %v", err)
	}
}

func (th *TestHelper) MustValidate() {
	th.t.Helper()
	if err := th.Validate(th.ctx); err != nil {
		th.t.Fatalf("Migration validation failed: %v", err)
	}
}

func (th *TestHelper) TestRollback() {
	th.t.Helper()

	th.t.Log("Applying all migrations...")
	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to apply migrations: %v", err)
	}

	if err := th.loadApplied(th.ctx); err != nil {
		th.t.Fatalf("Failed to load applied migrations: %v", err)
	}

	applied := th.getAppliedMigrations()
	count := len(applied)
	if count == 0 {
		th.t.Fatal("No migrations were applied")
	}
	th.t.Logf("✓ Applied %d migrations", count)

	th.t.Log("Rolling back migrations one by one...")
	for _, m := range applied {
		th.t.Logf("  Rolling back %s (%s)...", m.Version, m.Name)
		if err := th.Down(th.ctx, 1); err != nil {
			th.t.Fatalf("Failed to rollback migration %s (%s): %v", m.Version, m.Name, err)
		}
	}
	th.t.Logf("✓ Rolled back %d migrations", count)

	if err := th.loadApplied(th.ctx); err != nil {
		th.t.Fatalf("Failed to load applied migrations after rollback: %v", err)
	}
	if len(th.applied) != 0 {
		th.t.Fatalf("Expected 0 migrations after rollback, got %d", len(th.applied))
	}

	th.t.Log("Reapplying all migrations...")
	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to reapply migrations (database not clean after rollback): %v", err)
	}

	th.t.Logf("✓ Successfully completed full migration cycle: Up(%d) -> Down(%d) -> Up(%d)", count, count, count)
}
