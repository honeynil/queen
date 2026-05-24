package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/tap"
)

func TestCockpitThreePaneLayoutFitsWidth(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "create_posts", Status: queen.StatusPending},
	}
	m.showTap = true
	m.tapEvents = []tap.Event{
		{Kind: tap.KindStart, Version: "002", Name: "create_posts", StartedAt: time.Now()},
		{Kind: tap.KindExec, Version: "002", SQL: "CREATE TABLE posts (id BIGSERIAL PRIMARY KEY)", Duration: 120 * time.Microsecond},
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	for _, want := range []string{"migrations", "details", "tap stream", "create_posts"} {
		if !strings.Contains(view, want) {
			t.Fatalf("View() missing %q:\n%s", want, view)
		}
	}
}

func TestWideTopBarKeepsStatusAndWidth(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "drop_legacy", Status: queen.StatusPending, Destructive: true},
	}
	m.gaps = []queen.Gap{{Version: "003", Severity: "error", Type: queen.GapTypeNumbering}}
	m.cursor = 1

	top := m.renderTopBar(m.width)
	lines := strings.Split(top, "\n")
	if len(lines) != len(queenBannerLines) {
		t.Fatalf("wide topbar height=%d, want %d:\n%s", len(lines), len(queenBannerLines), top)
	}
	assertEveryLineFits(t, top, m.width)
	for _, want := range []string{"██████╗", "╚══▀▀═╝"} {
		if !strings.Contains(top, want) {
			t.Fatalf("topbar missing %q:\n%s", want, top)
		}
	}
	for _, unwanted := range []string{"migration cockpit", "BLOCKED GAPS", "review destructive"} {
		if strings.Contains(top, unwanted) {
			t.Fatalf("topbar contains removed artifact %q:\n%s", unwanted, top)
		}
	}
}

func TestLayoutMatrixKeepsExactGeometry(t *testing.T) {
	tests := []struct {
		name     string
		width    int
		height   int
		mode     ViewMode
		explain  bool
		filter   string
		message  string
		contains []string
	}{
		{
			name:     "migrations-min-three-pane",
			width:    80,
			height:   22,
			mode:     ViewMigrations,
			contains: []string{"migrations", "migration details", "tap stream"},
		},
		{
			name:     "gaps-wide",
			width:    132,
			height:   36,
			mode:     ViewGaps,
			contains: []string{"gaps", "gap details", "enter/f fills"},
		},
		{
			name:     "help-wide",
			width:    100,
			height:   26,
			mode:     ViewHelp,
			contains: []string{"navigation", "actions", "statuses", "risk levels"},
		},
		{
			name:     "explain-with-filter-message",
			width:    118,
			height:   30,
			mode:     ViewMigrations,
			explain:  true,
			filter:   "users",
			message:  "Explain completed in 86us",
			contains: []string{"explain plan", "EXPLAIN SQL", "/ filter", "Explain completed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := populatedModel(tt.width, tt.height)
			m.viewMode = tt.mode
			m.filter = tt.filter
			m.message = tt.message
			m.messageType = MessageSuccess
			if tt.mode == ViewGaps {
				m.focus = FocusList
				m.cursor = 0
			}
			if tt.mode == ViewHelp {
				m.focus = FocusDetail
			}
			if tt.explain {
				m.explainPinned = true
				m.explainEvent = m.tapEvents[0]
				m.explainQuery = m.tapEvents[0].BoundSQL
				m.explainResult = &tap.ExplainResult{
					Mode:     tap.ExplainOnly,
					Plan:     "Index Scan using users_pkey",
					Duration: 86 * time.Microsecond,
				}
			}

			view := m.View()
			assertEveryLineFits(t, view, tt.width)
			assertViewHeight(t, view, tt.height)
			for _, want := range tt.contains {
				if !strings.Contains(view, want) {
					t.Fatalf("view missing %q:\n%s", want, view)
				}
			}
		})
	}
}

func TestHelpViewUsesAvailableBodyHeight(t *testing.T) {
	m := populatedModel(100, 34)
	m.viewMode = ViewHelp
	m.focus = FocusDetail

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	if !strings.Contains(view, "EXPLAIN ANALYZE") || !strings.Contains(view, "risk levels") {
		t.Fatalf("help view did not render the full help content:\n%s", view)
	}
}

func TestUppercaseRefreshStartsReload(t *testing.T) {
	m := NewModel(nil, nil)
	m.loading = false
	m.spinnerActive = false
	m.message = "old message"

	_, cmd := m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'R'}})
	if cmd == nil {
		t.Fatal("uppercase R did not return a refresh command")
	}
	if !m.loading || !m.spinnerActive {
		t.Fatalf("uppercase R did not start refresh: loading=%v spinner=%v", m.loading, m.spinnerActive)
	}
	if m.message != "" {
		t.Fatalf("uppercase R did not clear message: %q", m.message)
	}
}

func TestExplainScreenIsSeparateFromTapStream(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "013", Name: "nplus1_user_lookups", Status: queen.StatusPending},
	}
	m.showTap = true
	m.tapEvents = []tap.Event{
		{Kind: tap.KindExec, Version: "013", SQL: "SELECT email FROM users WHERE id = ?", BoundSQL: "SELECT email FROM users WHERE id = 7", Operation: "select"},
		{Kind: tap.KindExec, Version: "014", SQL: "INSERT INTO audit_events (actor) VALUES (?)", BoundSQL: "INSERT INTO audit_events (actor) VALUES ('system')"},
	}
	m.tapCursor = 0
	m.explainPinned = true
	m.explainEvent = m.tapEvents[0]
	m.explainQuery = m.tapEvents[0].BoundSQL
	m.explainResult = &tap.ExplainResult{
		Mode:     tap.ExplainOnly,
		Plan:     "Index Scan using users_pkey",
		Duration: 350 * time.Microsecond,
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	for _, want := range []string{"explain", "pinned 013", "SELECT email FROM users WHERE id = 7", "Index Scan"} {
		if !strings.Contains(view, want) {
			t.Fatalf("View() missing %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "INSERT INTO audit_events") {
		t.Fatalf("explain screen leaked live stream event:\n%s", view)
	}
}

func TestExplainScreenWrapsLongSQLInsidePanel(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 96
	m.height = 30
	m.loading = false
	m.spinnerActive = false
	m.showTap = true
	m.migrations = []queen.MigrationStatus{
		{Version: "014", Name: "slow_post_counts", Status: queen.StatusPending},
	}
	longSQL := "SELECT COUNT(*) FROM posts WHERE user_id = 42 AND title LIKE '%queen migration demo with a very long string%' AND published_at > '2026-05-24'"
	m.tapEvents = []tap.Event{{Kind: tap.KindExec, Version: "014", SQL: longSQL, BoundSQL: longSQL, Operation: "select"}}
	m.explainPinned = true
	m.explainEvent = m.tapEvents[0]
	m.explainQuery = longSQL
	m.explainResult = &tap.ExplainResult{
		Mode:     tap.ExplainOnly,
		Plan:     "Index Scan using posts_user_id_idx on posts with an intentionally long textual plan that must wrap under renderer control",
		Duration: 86 * time.Microsecond,
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	for _, want := range []string{"EXPLAIN SQL", "EXPLAIN", "Index Scan"} {
		if !strings.Contains(view, want) {
			t.Fatalf("View() missing %q:\n%s", want, view)
		}
	}
}

func TestExplainScreenShowsRequestedModeWhileRunning(t *testing.T) {
	m := NewModel(nil, nil)
	m.explainPinned = true
	m.explainQuery = "SELECT email FROM users WHERE id = 7"
	m.explainMode = tap.ExplainOnly

	explainOnly := m.renderExplainContent(80)
	if !strings.Contains(explainOnly, "EXPLAIN SQL") || strings.Contains(explainOnly, "EXPLAIN ANALYZE SQL") {
		t.Fatalf("EXPLAIN mode rendered ambiguously:\n%s", explainOnly)
	}
	if !strings.Contains(explainOnly, "status: running EXPLAIN") {
		t.Fatalf("EXPLAIN running status missing mode:\n%s", explainOnly)
	}

	m.explainMode = tap.ExplainAnalyze
	analyze := m.renderExplainContent(80)
	if !strings.Contains(analyze, "EXPLAIN ANALYZE SQL") {
		t.Fatalf("EXPLAIN ANALYZE mode was not rendered:\n%s", analyze)
	}
	if !strings.Contains(analyze, "status: running EXPLAIN ANALYZE") {
		t.Fatalf("EXPLAIN ANALYZE running status missing mode:\n%s", analyze)
	}
}

func TestExplainScreenNavigationScrollsExplainViewport(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 100
	m.height = 24
	m.loading = false
	m.spinnerActive = false
	m.focus = FocusDetail
	m.migrations = []queen.MigrationStatus{
		{Version: "014", Name: "slow_post_counts", Status: queen.StatusPending},
		{Version: "015", Name: "next_migration", Status: queen.StatusPending},
	}
	m.cursor = 0
	m.tapEvents = []tap.Event{{Kind: tap.KindExec, Version: "014", SQL: "SELECT COUNT(*) FROM posts", BoundSQL: "SELECT COUNT(*) FROM posts", Operation: "select"}}
	m.explainPinned = true
	m.explainEvent = m.tapEvents[0]
	m.explainQuery = m.tapEvents[0].BoundSQL
	m.explainResult = &tap.ExplainResult{
		Mode:     tap.ExplainOnly,
		Plan:     strings.Repeat("Seq Scan on posts\n", 40),
		Duration: 86 * time.Microsecond,
	}

	_ = m.View()
	startCursor := m.cursor
	startOffset := m.tapVP.YOffset
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	if m.cursor != startCursor {
		t.Fatalf("explain navigation moved list cursor: got %d want %d", m.cursor, startCursor)
	}
	if m.tapVP.YOffset <= startOffset {
		t.Fatalf("explain navigation did not scroll explain viewport: got %d start %d", m.tapVP.YOffset, startOffset)
	}

	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyEsc})
	if m.explainActive() {
		t.Fatalf("esc did not clear isolated explain screen")
	}
}

func TestTapStreamFollowKeepsNewestEventVisibleAfterOverflow(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 110
	m.height = 28
	m.loading = false
	m.spinnerActive = false
	m.showTap = true
	m.focus = FocusTap
	m.tapFollow = true
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "seed_audit_events", Status: queen.StatusPending},
	}

	for i := 0; i < 620; i++ {
		msg := tapEventMsg(tap.Event{
			Kind:     tap.KindExec,
			Version:  "001",
			SQL:      "INSERT INTO audit_events (actor, action) VALUES (?, ?)",
			BoundSQL: fmt.Sprintf("INSERT INTO audit_events (actor, action) VALUES ('system', 'event_%03d')", i),
			Duration: time.Duration(i+1) * time.Microsecond,
		})
		_, _ = m.Update(msg)
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	if len(m.tapEvents) != 500 {
		t.Fatalf("tap event buffer len=%d, want 500", len(m.tapEvents))
	}
	if m.tapCursor != len(m.tapEvents)-1 {
		t.Fatalf("tap cursor=%d, want newest index %d", m.tapCursor, len(m.tapEvents)-1)
	}
	if !strings.Contains(view, "event_619") {
		t.Fatalf("newest tap event is not visible at the bottom:\n%s", view)
	}
	if strings.Contains(view, "event_000") {
		t.Fatalf("overflowed tap event leaked into visible stream:\n%s", view)
	}
}

func TestFocusCyclesIntoVisibleTapPanelAndExplainKeyResponds(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.showTap = false
	m.focus = FocusList
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusPending},
	}

	view := m.View()
	if !strings.Contains(view, "tap stream") {
		t.Fatalf("tap panel should be visible in the three-pane layout:\n%s", view)
	}
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyTab})
	if m.focus != FocusDetail {
		t.Fatalf("first tab focus=%v, want detail", m.focus)
	}
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyTab})
	if m.focus != FocusTap {
		t.Fatalf("second tab focus=%v, want tap", m.focus)
	}
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	if m.messageType != MessageWarning || !strings.Contains(m.message, "Select a SQL event") {
		t.Fatalf("x on empty tap panel did not produce explain guidance: type=%v message=%q", m.messageType, m.message)
	}
	startCursor := m.cursor
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	if m.cursor != startCursor {
		t.Fatalf("empty tap panel navigation moved migration cursor: got %d want %d", m.cursor, startCursor)
	}
}

func TestGlobalTapAndExplainKeysRespondOutsideTapFocus(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.showTap = false
	m.focus = FocusList
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusPending},
	}
	m.tapEvents = []tap.Event{
		{Kind: tap.KindExec, Version: "001", SQL: "SELECT 1", BoundSQL: "SELECT 1", Operation: "select"},
	}

	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'T'}})
	if !m.showTap || m.focus != FocusTap {
		t.Fatalf("T did not focus visible tap panel: showTap=%v focus=%v", m.showTap, m.focus)
	}
	m.focus = FocusList
	_, _ = m.handleKeyPress(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	if m.messageType != MessageWarning || !strings.Contains(m.message, "unavailable") {
		t.Fatalf("global x outside tap focus did not reach explain flow: type=%v message=%q", m.messageType, m.message)
	}
}

func TestMigrationTableAvoidsUnicodeReplacementArtifacts(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 120
	m.height = 34
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "drop_legacy", Status: queen.StatusPending, Destructive: true},
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	if strings.Contains(view, "�") {
		t.Fatalf("view contains replacement-character artifact:\n%s", view)
	}
	for _, want := range []string{"applied", "pending", "dest", "rollback", "<-"} {
		if !strings.Contains(view, want) {
			t.Fatalf("migration list missing compact marker %q:\n%s", want, view)
		}
	}
}

func TestMigrationListUsesCompactColumns(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 96
	m.height = 28
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "create_posts", Status: queen.StatusPending, Destructive: true, HasRollback: true},
		{Version: "003", Name: "alter_users_with_a_long_but_readable_name", Status: queen.StatusModified},
	}

	content := m.renderMigrationListContent(56, 8)
	assertEveryLineMaxWidth(t, content, 56)
	for _, want := range []string{">", "status", "ver", "name", "dest", "rollback", "applied", "pending", "modified", "<-"} {
		if !strings.Contains(content, want) {
			t.Fatalf("compact migration list missing %q:\n%s", want, content)
		}
	}
	for _, unwanted := range []string{"!dest", "down", "flg"} {
		if strings.Contains(content, unwanted) {
			t.Fatalf("compact migration list still contains wide token %q:\n%s", unwanted, content)
		}
	}
}

func TestGapsListShowsSelectionMarker(t *testing.T) {
	m := populatedModel(100, 28)
	m.viewMode = ViewGaps
	m.focus = FocusList
	m.cursor = 0

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	if !strings.Contains(view, ">") {
		t.Fatalf("gaps list missing selection marker:\n%s", view)
	}
}

func TestCompactLayoutFitsTerminalAndUsesSingleListPanel(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 72
	m.height = 20
	m.loading = false
	m.spinnerActive = false
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "create_posts", Status: queen.StatusPending},
	}
	m.showTap = true
	m.tapEvents = []tap.Event{
		{Kind: tap.KindExec, Version: "002", SQL: "CREATE TABLE posts (id BIGSERIAL PRIMARY KEY)", Duration: 120 * time.Microsecond},
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	if !strings.Contains(view, "migrations") {
		t.Fatalf("compact view missing migrations list:\n%s", view)
	}
	if strings.Contains(view, "tap stream") || strings.Contains(view, "migration details") {
		t.Fatalf("compact view should not squeeze secondary panels into a narrow terminal:\n%s", view)
	}
}

func TestFilterAndMessageBarsKeepTerminalHeight(t *testing.T) {
	m := NewModel(nil, nil)
	m.width = 100
	m.height = 24
	m.loading = false
	m.spinnerActive = false
	m.filter = "destructive"
	m.message = "Review destructive migrations before applying."
	m.messageType = MessageWarning
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied},
		{Version: "002", Name: "drop_legacy_users", Status: queen.StatusPending, Destructive: true},
	}

	view := m.View()
	assertEveryLineFits(t, view, m.width)
	assertViewHeight(t, view, m.height)
	for _, want := range []string{"/ filter", "Review destructive migrations", "drop_legacy_users"} {
		if !strings.Contains(view, want) {
			t.Fatalf("filtered/message view missing %q:\n%s", want, view)
		}
	}
}

func populatedModel(width, height int) *Model {
	m := NewModel(nil, nil)
	m.width = width
	m.height = height
	m.loading = false
	m.spinnerActive = false
	m.showTap = true
	m.migrations = []queen.MigrationStatus{
		{Version: "001", Name: "create_users", Status: queen.StatusApplied, HasRollback: true},
		{Version: "002", Name: "seed_users", Status: queen.StatusPending},
		{Version: "003", Name: "drop_legacy_users", Status: queen.StatusPending, Destructive: true},
	}
	m.gaps = []queen.Gap{
		{Version: "004", Name: "missing_reports", Severity: "warning", Type: queen.GapTypeNumbering, Description: "numbering gap between applied and local migrations"},
	}
	m.tapEvents = []tap.Event{
		{Kind: tap.KindExec, Version: "002", SQL: "SELECT email FROM users WHERE id = ?", BoundSQL: "SELECT email FROM users WHERE id = 7", Operation: "select", Duration: 86 * time.Microsecond},
		{Kind: tap.KindTxCommit, Version: "002", Duration: 120 * time.Microsecond},
	}
	m.tapCursor = 0
	return m
}

func assertEveryLineFits(t *testing.T, view string, width int) {
	t.Helper()
	for i, line := range strings.Split(view, "\n") {
		if got := lipgloss.Width(line); got != width {
			t.Fatalf("line %d width=%d, want %d:\n%q\n\n%s", i, got, width, line, view)
		}
	}
}

func assertEveryLineMaxWidth(t *testing.T, view string, width int) {
	t.Helper()
	for i, line := range strings.Split(view, "\n") {
		if got := lipgloss.Width(line); got > width {
			t.Fatalf("line %d width=%d, max %d:\n%q\n\n%s", i, got, width, line, view)
		}
	}
}

func assertViewHeight(t *testing.T, view string, height int) {
	t.Helper()
	if got := len(strings.Split(view, "\n")); got != height {
		t.Fatalf("view height=%d, want %d:\n%s", got, height, view)
	}
}
