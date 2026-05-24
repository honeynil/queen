package tui

import (
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/yaop-labs/queen/tap"
)

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.resizePanels()
		m.ensureCursorInRange()
		return m, nil

	case tea.KeyMsg:
		return m.handleKeyPress(msg)

	case spinner.TickMsg:
		if m.spinnerActive {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil

	case migrationsLoadedMsg:
		initialLoad := len(m.migrations) == 0 && m.cursor == 0 && m.filter == "" && m.viewMode == ViewMigrations
		m.migrations = msg.migrations
		if initialLoad {
			m.focusFirstPendingMigration()
		}
		m.loading = false
		m.spinnerActive = false
		m.refreshRows()
		m.ensureCursorInRange()
		return m, nil

	case gapsLoadedMsg:
		m.gaps = msg.gaps
		m.refreshRows()
		m.ensureCursorInRange()
		return m, nil

	case errMsg:
		m.err = msg.err
		m.message = fmt.Sprintf("Error: %v", msg.err)
		m.messageType = MessageError
		m.loading = false
		m.spinnerActive = false
		return m, nil

	case operationCompleteMsg:
		m.message = msg.message
		m.messageType = msg.messageType
		m.loading = false
		m.spinnerActive = false
		return m, tea.Batch(m.loadMigrations(), m.loadGaps())

	case tapEventMsg:
		const maxTapEvents = 500
		wasFollowing := m.tapFollow || len(m.tapEvents) == 0 || m.tapCursor >= len(m.tapEvents)-1
		m.tapEvents = append(m.tapEvents, tap.Event(msg))
		if overflow := len(m.tapEvents) - maxTapEvents; overflow > 0 {
			m.tapEvents = m.tapEvents[overflow:]
			if !wasFollowing {
				m.tapCursor -= overflow
			}
		}
		if wasFollowing {
			m.tapCursor = len(m.tapEvents) - 1
			m.tapFollow = true
		} else {
			m.tapCursor = clamp(m.tapCursor, 0, len(m.tapEvents)-1)
		}
		m.refreshTap()
		return m, m.waitForTapEvent()

	case tapClosedMsg:
		m.tapSink = nil
		return m, nil

	case explainResultMsg:
		m.loading = false
		m.spinnerActive = false
		m.explainResult = msg.result
		m.explainError = ""
		if msg.err != nil {
			m.explainError = msg.err.Error()
			m.message = fmt.Sprintf("Explain failed: %v", msg.err)
			m.messageType = MessageError
		} else if msg.result != nil {
			m.message = fmt.Sprintf("%s completed in %s", msg.result.Mode, formatTapDuration(msg.result.Duration))
			m.messageType = MessageSuccess
		}
		m.refreshTap()
		return m, nil
	}

	return m, nil
}
