package cli

import (
	"strings"
	"testing"
)

func TestOutputCheckSummaryCountsOnlyExecutedChecks(t *testing.T) {
	out := captureStdout(t, func() {
		outputCheckSummary(checkSummary{passed: 3})
	})
	if !strings.Contains(out, "All checks passed (3/3)") {
		t.Fatalf("output summary mismatch:\n%s", out)
	}
	if strings.Contains(out, "All checks passed (3/0)") {
		t.Fatalf("output used stale denominator:\n%s", out)
	}
}

func TestOutputCheckSummaryReportsFailures(t *testing.T) {
	out := captureStdout(t, func() {
		outputCheckSummary(checkSummary{passed: 2, failed: 1, exitCode: 3})
	})
	if !strings.Contains(out, "Checks: 2 passed, 1 failed") {
		t.Fatalf("output summary mismatch:\n%s", out)
	}
	if strings.Contains(out, "All checks passed") {
		t.Fatalf("failure output claimed success:\n%s", out)
	}
}
