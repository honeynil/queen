package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/tap"
)

func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		m.loadMigrations(),
		m.loadGaps(),
		m.spinner.Tick,
	)
}

func (m *Model) loadMigrations() tea.Cmd {
	return func() tea.Msg {
		statuses, err := m.queen.Status(m.ctx)
		if err != nil {
			return errMsg{err}
		}
		return migrationsLoadedMsg{statuses}
	}
}

func (m *Model) loadGaps() tea.Cmd {
	return func() tea.Msg {
		gaps, err := m.queen.DetectGaps(m.ctx)
		if err != nil {
			return errMsg{err}
		}
		return gapsLoadedMsg{gaps}
	}
}

type migrationsLoadedMsg struct {
	migrations []queen.MigrationStatus
}

type gapsLoadedMsg struct {
	gaps []queen.Gap
}

type errMsg struct {
	err error
}

type operationCompleteMsg struct {
	message     string
	messageType MessageType
}

type tapEventMsg tap.Event
type tapClosedMsg struct{}
type explainResultMsg struct {
	result *tap.ExplainResult
	err    error
}
