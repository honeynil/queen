package live

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/yaop-labs/queen/tap"
)

func TestElapsedFreezesWhenFinished(t *testing.T) {
	m := newModel(tap.NewChannelSink(1), make(chan error), func() {})
	m.startAt = time.Now().Add(-time.Minute)
	m.finished = true
	m.finishedAt = m.startAt.Add(2 * time.Second)

	first := m.elapsed()
	time.Sleep(5 * time.Millisecond)
	second := m.elapsed()

	if first != second {
		t.Fatalf("elapsed changed after finish: %s -> %s", first, second)
	}
}

func TestWideBodyLinesFitScreenWidth(t *testing.T) {
	m := newModel(tap.NewChannelSink(1), make(chan error), func() {})
	m.width = 128
	m.height = 34
	m.migStarted = 1
	m.execs = 1
	m.rows = []row{
		{atRel: time.Millisecond, ev: tap.Event{
			Kind:      tap.KindStart,
			Version:   "001",
			Name:      "migration_with_a_long_name",
			Direction: tap.DirectionUp,
		}},
		{atRel: 2 * time.Millisecond, ev: tap.Event{
			Kind:      tap.KindExec,
			Version:   "001",
			Name:      "migration_with_a_long_name",
			Direction: tap.DirectionUp,
			Duration:  75 * time.Microsecond,
			Operation: "insert",
			SQL:       "INSERT INTO comments (post_id, body) VALUES (?, ?) /* deliberately long SQL to exercise truncation */",
			BoundSQL:  "INSERT INTO comments (post_id, body) VALUES (123, 'a long comment body')",
		}},
	}
	m.selected = 1

	for i, line := range m.renderBody(m.width) {
		if got := lipgloss.Width(line); got != m.width {
			t.Fatalf("body line %d width=%d, want %d:\n%q", i, got, m.width, line)
		}
	}
}

func TestViewRendersDashboardAndDetails(t *testing.T) {
	m := newModel(tap.NewChannelSink(1), make(chan error), func() {})
	m.width = 120
	m.height = 30
	m.rows = []row{
		{atRel: time.Millisecond, ev: tap.Event{
			Kind:      tap.KindStart,
			Version:   "001",
			Name:      "create_users",
			Direction: tap.DirectionUp,
		}},
		{atRel: 2 * time.Millisecond, ev: tap.Event{
			Kind:        tap.KindExec,
			Version:     "001",
			Name:        "create_users",
			Direction:   tap.DirectionUp,
			Duration:    150 * time.Millisecond,
			Operation:   "select",
			SQL:         "SELECT email FROM users WHERE id = ?",
			BoundSQL:    "SELECT email FROM users WHERE id = 42",
			Slow:        true,
			NPlus1:      true,
			NPlus1Count: 5,
		}},
	}
	m.selected = 1
	m.migStarted = 1
	m.execs = 1
	m.slow = 1
	m.nplus1 = 1

	view := m.View()
	for _, want := range []string{
		"queen tap",
		"RUNNING",
		"migrations",
		"details",
		"SLOW",
		"N+1/5",
		"SELECT email FROM users",
		"j/k",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("View() missing %q:\n%s", want, view)
		}
	}
}
