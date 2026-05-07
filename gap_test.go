package queen

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDetectGaps(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no driver", func(t *testing.T) {
		t.Parallel()

		q := &Queen{driver: nil}
		_, err := q.DetectGaps(context.Background())

		if !errors.Is(err, ErrNoDriver) {
			t.Errorf("DetectGaps() error = %v, want %v", err, ErrNoDriver)
		}
	})

	t.Run("detects no gaps with empty migrations", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		gaps, err := q.DetectGaps(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(gaps) != 0 {
			t.Errorf("expected no gaps, got %d", len(gaps))
		}
	})
}

func TestDetectApplicationGaps(t *testing.T) {
	t.Parallel()

	t.Run("detects unregistered migrations", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		q := &Queen{
			migrations: []*Migration{
				{Version: "001", Name: "create_users"},
			},
			applied: map[string]*Applied{
				"001": {Version: "001", Name: "create_users", AppliedAt: now},
				"002": {Version: "002", Name: "add_email", AppliedAt: now},
			},
		}

		appGaps, unregGaps := q.detectApplicationGaps()

		if len(appGaps) != 0 {
			t.Errorf("expected no application gaps, got %d", len(appGaps))
		}

		if len(unregGaps) != 1 {
			t.Fatalf("expected 1 unregistered gap, got %d", len(unregGaps))
		}

		gap := unregGaps[0]
		if gap.Type != GapTypeUnregistered {
			t.Errorf("gap type = %q, want %q", gap.Type, GapTypeUnregistered)
		}
		if gap.Version != "002" {
			t.Errorf("gap version = %q, want %q", gap.Version, "002")
		}
		if gap.Severity != "error" {
			t.Errorf("gap severity = %q, want %q", gap.Severity, "error")
		}
	})

	t.Run("detects application gaps", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		q := &Queen{
			migrations: []*Migration{
				{Version: "001", Name: "create_users"},
				{Version: "002", Name: "add_email"},
				{Version: "003", Name: "add_index"},
			},
			applied: map[string]*Applied{
				"001": {Version: "001", Name: "create_users", AppliedAt: now},
				"003": {Version: "003", Name: "add_index", AppliedAt: now},
			},
		}

		appGaps, unregGaps := q.detectApplicationGaps()

		if len(unregGaps) != 0 {
			t.Errorf("expected no unregistered gaps, got %d", len(unregGaps))
		}

		if len(appGaps) != 1 {
			t.Fatalf("expected 1 application gap, got %d", len(appGaps))
		}

		gap := appGaps[0]
		if gap.Type != GapTypeApplication {
			t.Errorf("gap type = %q, want %q", gap.Type, GapTypeApplication)
		}
		if gap.Version != "002" {
			t.Errorf("gap version = %q, want %q", gap.Version, "002")
		}
	})
}

func TestDetectNumberingGaps(t *testing.T) {
	t.Parallel()

	t.Run("detects numbering gaps in sequential pattern", func(t *testing.T) {
		t.Parallel()

		q := &Queen{
			migrations: []*Migration{
				{Version: "001", Name: "create_users"},
				{Version: "002", Name: "add_email"},
				{Version: "004", Name: "add_index"},
			},
			applied: make(map[string]*Applied),
		}

		gaps := q.detectNumberingGaps()

		if len(gaps) != 1 {
			t.Fatalf("expected 1 numbering gap, got %d", len(gaps))
		}

		gap := gaps[0]
		if gap.Type != GapTypeNumbering {
			t.Errorf("gap type = %q, want %q", gap.Type, GapTypeNumbering)
		}
		if gap.Version != "003" {
			t.Errorf("gap version = %q, want %q", gap.Version, "003")
		}
		if gap.Severity != "warning" {
			t.Errorf("gap severity = %q, want %q", gap.Severity, "warning")
		}
	})

	t.Run("no gaps with sequential migrations", func(t *testing.T) {
		t.Parallel()

		q := &Queen{
			migrations: []*Migration{
				{Version: "001", Name: "create_users"},
				{Version: "002", Name: "add_email"},
				{Version: "003", Name: "add_index"},
			},
			applied: make(map[string]*Applied),
		}

		gaps := q.detectNumberingGaps()

		if len(gaps) != 0 {
			t.Errorf("expected no numbering gaps, got %d", len(gaps))
		}
	})
}

func TestFilterIgnoredGaps(t *testing.T) {
	t.Parallel()

	t.Run("returns all gaps when no queenignore file", func(t *testing.T) {
		t.Parallel()

		q := &Queen{}

		gaps := []Gap{
			{Version: "001", Type: GapTypeNumbering},
			{Version: "002", Type: GapTypeNumbering},
			{Version: "003", Type: GapTypeApplication},
		}

		filtered := q.filterIgnoredGaps(gaps)

		// Without .queenignore file, all gaps should be returned
		if len(filtered) != len(gaps) {
			t.Errorf("expected %d gaps, got %d", len(gaps), len(filtered))
		}
	})
}
