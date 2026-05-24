package cli

import (
	"context"
	"fmt"

	"github.com/yaop-labs/queen"
)

type checkOptions struct {
	ci           bool
	noPending    bool
	noGaps       bool
	rollbackTest bool
}

type checkSummary struct {
	passed   int
	failed   int
	exitCode int
}

func runPipelineChecks(ctx context.Context, q *queen.Queen, opts checkOptions) checkSummary {
	var summary checkSummary

	fmt.Print("Validating migrations... ")
	if err := q.Validate(ctx); err != nil {
		fmt.Printf("FAIL\n  %v\n", err)
		summary.fail(3)
	} else {
		fmt.Println("OK")
		summary.pass()
	}

	fmt.Print("Checking for gaps... ")
	gaps, err := q.DetectGaps(ctx)
	if err != nil {
		fmt.Printf("FAIL\n  %v\n", err)
		summary.fail(3)
	} else if len(gaps) > 0 {
		fmt.Printf("WARNING (%d gaps found)\n", len(gaps))
		if opts.noGaps || opts.ci {
			summary.fail(4)
		} else {
			summary.pass()
		}
	} else {
		fmt.Println("OK")
		summary.pass()
	}

	if opts.noPending || opts.ci {
		checkPendingMigrations(ctx, q, &summary)
	}

	if opts.rollbackTest {
		checkRollbackCycle(ctx, q, &summary)
	}

	fmt.Print("Database connectivity... ")
	if _, err := q.Driver().GetApplied(ctx); err != nil {
		fmt.Printf("FAIL\n  %v\n", err)
		summary.fail(3)
	} else {
		fmt.Println("OK")
		summary.pass()
	}

	return summary
}

func checkRollbackCycle(ctx context.Context, q *queen.Queen, summary *checkSummary) {
	fmt.Print("Testing rollback cycle... ")

	statuses, err := q.Status(ctx)
	if err != nil {
		fmt.Printf("FAIL\n  %v\n", err)
		summary.fail(6)
		return
	}

	appliedCount := 0
	for _, s := range statuses {
		if s.Status == queen.StatusApplied || s.Status == queen.StatusModified {
			appliedCount++
		}
	}
	if appliedCount > 0 {
		fmt.Printf("FAIL\n  rollback-test requires a clean test database; found %d applied migration(s)\n", appliedCount)
		summary.fail(6)
		return
	}

	if len(statuses) == 0 {
		fmt.Println("FAIL\n  no migrations registered")
		summary.fail(6)
		return
	}

	if err := q.Up(ctx); err != nil {
		fmt.Printf("FAIL\n  apply failed: %v\n", err)
		summary.fail(6)
		return
	}
	if err := q.Reset(ctx); err != nil {
		fmt.Printf("FAIL\n  rollback failed: %v\n", err)
		summary.fail(6)
		return
	}
	if err := q.Up(ctx); err != nil {
		fmt.Printf("FAIL\n  reapply failed: %v\n", err)
		summary.fail(6)
		return
	}

	fmt.Println("OK")
	summary.pass()
}

func checkPendingMigrations(ctx context.Context, q *queen.Queen, summary *checkSummary) {
	fmt.Print("Checking for pending migrations... ")
	statuses, err := q.Status(ctx)
	if err != nil {
		fmt.Printf("FAIL\n  %v\n", err)
		summary.fail(3)
		return
	}

	pendingCount := 0
	for _, s := range statuses {
		if s.Status == queen.StatusPending {
			pendingCount++
		}
	}
	if pendingCount > 0 {
		fmt.Printf("FAIL (%d pending)\n", pendingCount)
		summary.fail(5)
		return
	}

	fmt.Println("OK")
	summary.pass()
}

func (s *checkSummary) pass() {
	s.passed++
}

func (s *checkSummary) fail(exitCode int) {
	s.failed++
	if s.exitCode == 0 {
		s.exitCode = exitCode
	}
}
