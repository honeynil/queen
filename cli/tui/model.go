// Package tui provides a terminal UI for Queen migrations.
package tui

import (
	"context"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/tap"
)

type ViewMode int

const (
	ViewMigrations ViewMode = iota
	ViewGaps
	ViewHelp
)

// Focus represents which panel currently captures j/k input.
type Focus int

const (
	FocusList Focus = iota
	FocusDetail
	FocusTap
)

type Model struct {
	queen       *queen.Queen
	ctx         context.Context
	migrations  []queen.MigrationStatus
	gaps        []queen.Gap
	cursor      int
	viewMode    ViewMode
	message     string
	messageType MessageType
	loading     bool
	width       int
	height      int
	err         error
	quitting    bool

	spinner       spinner.Model
	spinnerActive bool
	filter        string
	filterEditing bool

	tapSink        *tap.ChannelSink
	tapEvents      []tap.Event
	tapCursor      int
	tapFollow      bool
	showTap        bool
	operationLabel string
	explainResult  *tap.ExplainResult
	explainError   string
	explainQuery   string
	explainMode    tap.ExplainMode
	explainEvent   tap.Event
	explainPinned  bool

	// planCache memoizes Explain() lookups so the detail panel can render the
	// migration's SQL with syntax highlighting without re-querying on each draw.
	planCache map[string]*queen.MigrationPlan

	// New bubbles widgets driving the layout.
	migrationsTable table.Model
	gapsTable       table.Model
	detailVP        viewport.Model
	tapVP           viewport.Model
	filterInput     textinput.Model
	helpView        help.Model
	keys            keyMap
	focus           Focus

	// panelsReady tracks whether bubbles widgets have been initialised so
	// callers that construct Model literally (in tests) still get safe defaults.
	panelsReady bool
}

type MessageType int

const (
	MessageInfo MessageType = iota
	MessageSuccess
	MessageWarning
	MessageError
)

func NewModel(q *queen.Queen, ctx context.Context) *Model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(colorPrimary)

	ti := textinput.New()
	ti.Prompt = ""
	ti.Placeholder = "type to filter..."
	ti.CharLimit = 64

	h := help.New()
	h.ShowAll = false
	h.Styles.ShortKey = FooterKeyStyle
	h.Styles.ShortDesc = FooterDescStyle
	h.Styles.ShortSeparator = FooterDescStyle
	h.Styles.FullKey = FooterKeyStyle
	h.Styles.FullDesc = FooterDescStyle

	m := &Model{
		queen:         q,
		ctx:           ctx,
		cursor:        0,
		viewMode:      ViewMigrations,
		loading:       true,
		spinner:       sp,
		spinnerActive: true,
		planCache:     make(map[string]*queen.MigrationPlan),
		filterInput:   ti,
		helpView:      h,
		keys:          newKeyMap(),
		focus:         FocusList,
	}
	m.initPanels()
	return m
}

// ensurePanels lazily initialises bubbles widgets so model literals in tests
// don't crash before they call View().
func (m *Model) ensurePanels() {
	if m.panelsReady {
		return
	}
	if m.keys.Quit.Keys() == nil {
		m.keys = newKeyMap()
	}
	if m.filterInput.Cursor.Style.GetForeground() == nil {
		m.filterInput = textinput.New()
		m.filterInput.Prompt = ""
		m.filterInput.Placeholder = "type to filter..."
		m.filterInput.CharLimit = 64
	}
	if m.planCache == nil {
		m.planCache = make(map[string]*queen.MigrationPlan)
	}
	m.helpView = help.New()
	m.helpView.ShowAll = false
	m.helpView.Styles.ShortKey = FooterKeyStyle
	m.helpView.Styles.ShortDesc = FooterDescStyle
	m.helpView.Styles.ShortSeparator = FooterDescStyle
	m.initPanels()
}

// migrationPlan returns the cached MigrationPlan for the given version, lazily
// loading it via Queen.Explain on first request. Returns nil if Queen is unset
// or Explain fails (callers fall back to metadata-only rendering).
func (m *Model) migrationPlan(version string) *queen.MigrationPlan {
	if m == nil || m.queen == nil || version == "" {
		return nil
	}
	if m.planCache == nil {
		m.planCache = make(map[string]*queen.MigrationPlan)
	}
	if plan, ok := m.planCache[version]; ok {
		return plan
	}
	plan, err := m.queen.Explain(m.ctx, version)
	if err != nil {
		m.planCache[version] = nil
		return nil
	}
	m.planCache[version] = plan
	return plan
}
