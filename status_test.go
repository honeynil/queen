package queen

import "testing"

func TestStatusString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status Status
		want   string
	}{
		{"pending status", StatusPending, "pending"},
		{"applied status", StatusApplied, "applied"},
		{"modified status", StatusModified, "modified"},
		{"unknown status", Status(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.status.String()
			if got != tt.want {
				t.Errorf("Status.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStatusConstants(t *testing.T) {
	t.Parallel()

	if StatusPending != 0 {
		t.Errorf("StatusPending = %d, want 0", StatusPending)
	}
	if StatusApplied != 1 {
		t.Errorf("StatusApplied = %d, want 1", StatusApplied)
	}
	if StatusModified != 2 {
		t.Errorf("StatusModified = %d, want 2", StatusModified)
	}
}
