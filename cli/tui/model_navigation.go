package tui

import (
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/yaop-labs/queen/tap"
)

//nolint:gocyclo // TUI key routing is centralized so focus/filter/explain states stay ordered.
func (m *Model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	m.ensurePanels()
	switch msg.String() {
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit
	case "q":
		if !m.filterEditing {
			m.quitting = true
			return m, tea.Quit
		}
	case "t", "T":
		if !m.filterEditing && m.canFocusTap() {
			m.showTap = true
			m.focus = FocusTap
			return m, nil
		}
	}

	if m.loading {
		return m, nil
	}

	if m.filterEditing {
		switch msg.String() {
		case "esc":
			m.filterEditing = false
			m.filterInput.Blur()
			if m.filter == "" {
				m.resetCursor()
			}
			return m, nil
		case "enter":
			m.filterEditing = false
			m.filterInput.Blur()
			return m, nil
		case "backspace":
			runes := []rune(m.filter)
			if len(runes) > 0 {
				m.filter = string(runes[:len(runes)-1])
				m.resetCursor()
			}
			return m, nil
		case "ctrl+u":
			m.filter = ""
			m.filterInput.SetValue("")
			m.resetCursor()
			return m, nil
		case "space":
			m.filter += " "
			m.filterInput.SetValue(m.filter)
			m.resetCursor()
			return m, nil
		}
		if runes := msg.Runes; len(runes) > 0 {
			m.filter += string(runes)
			m.filterInput.SetValue(m.filter)
			m.resetCursor()
		}
		return m, nil
	}

	if key.Matches(msg, m.keys.Tab) {
		m.nextFocus()
		return m, nil
	}
	if key.Matches(msg, m.keys.ShiftTab) {
		m.prevFocus()
		return m, nil
	}

	if m.explainActive() {
		switch msg.String() {
		case "esc":
			m.clearExplain()
			return m, nil
		case "up", "k":
			m.tapVP.ScrollUp(1)
			m.tapFollow = false
			return m, nil
		case "down", "j":
			m.tapVP.ScrollDown(1)
			m.tapFollow = false
			return m, nil
		case "g":
			m.tapVP.GotoTop()
			m.tapFollow = false
			return m, nil
		case "G":
			m.tapVP.GotoBottom()
			m.tapFollow = false
			return m, nil
		}
	}

	switch msg.String() {
	case "x":
		return m.runExplain(tap.ExplainOnly)
	case "X":
		return m.runExplain(tap.ExplainAnalyze)
	}

	if m.focus == FocusTap && len(m.tapEvents) > 0 {
		switch msg.String() {
		case "up", "k":
			if m.tapCursor > 0 {
				m.tapCursor--
			}
			m.tapFollow = false
			return m, nil
		case "down", "j":
			if m.tapCursor < len(m.tapEvents)-1 {
				m.tapCursor++
			}
			m.tapFollow = m.tapCursor >= len(m.tapEvents)-1
			return m, nil
		case "g":
			m.tapCursor = 0
			m.tapFollow = false
			return m, nil
		case "G":
			m.tapCursor = len(m.tapEvents) - 1
			m.tapFollow = true
			return m, nil
		}
	}

	switch msg.String() {
	case "/":
		if m.viewMode != ViewHelp {
			m.filterEditing = true
			m.filterInput.SetValue(m.filter)
			m.filterInput.Focus()
			m.message = ""
			return m, nil
		}

	case "esc":
		if m.filter != "" {
			m.clearFilter()
		}

	case "up", "k":
		if m.focus == FocusDetail {
			m.detailVP.ScrollUp(1)
			return m, nil
		}
		if m.focus == FocusTap {
			m.tapVP.ScrollUp(1)
			return m, nil
		}
		if m.cursor > 0 {
			m.cursor--
			m.syncTableCursor()
		}

	case "down", "j":
		if m.focus == FocusDetail {
			m.detailVP.ScrollDown(1)
			return m, nil
		}
		if m.focus == FocusTap {
			m.tapVP.ScrollDown(1)
			return m, nil
		}
		maxCursor := m.currentItemCount() - 1
		if m.cursor < maxCursor {
			m.cursor++
			m.syncTableCursor()
		}

	case "g":
		if m.focus == FocusDetail {
			m.detailVP.GotoTop()
			return m, nil
		}
		if m.focus == FocusTap {
			m.tapVP.GotoTop()
			m.tapFollow = false
			return m, nil
		}
		m.cursor = 0
		m.syncTableCursor()

	case "G":
		if m.focus == FocusDetail {
			m.detailVP.GotoBottom()
			return m, nil
		}
		if m.focus == FocusTap {
			m.tapVP.GotoBottom()
			m.tapFollow = true
			return m, nil
		}
		if count := m.currentItemCount(); count > 0 {
			m.cursor = count - 1
		}
		m.syncTableCursor()

	case "1":
		m.viewMode = ViewMigrations
		m.resetCursor()
		m.message = ""
		m.focus = FocusList

	case "2":
		m.viewMode = ViewGaps
		m.resetCursor()
		m.message = ""
		m.focus = FocusList

	case "3", "?":
		m.viewMode = ViewHelp
		m.resetCursor()
		m.message = ""
		m.focus = FocusDetail

	case "r", "R":
		m.loading = true
		m.spinnerActive = true
		m.message = ""
		return m, tea.Batch(m.loadMigrations(), m.loadGaps(), m.spinner.Tick)

	case "enter":
		return m.handleAction()

	case "u":
		if m.viewMode == ViewMigrations {
			return m.applyMigration()
		}

	case "d":
		if m.viewMode == ViewMigrations {
			return m.rollbackMigration()
		}

	case "f":
		if m.viewMode == ViewGaps {
			return m.fillGap()
		}

	case "i":
		if m.viewMode == ViewGaps {
			return m.ignoreGap()
		}
	}

	return m, nil
}

func (m *Model) nextFocus() {
	switch m.focus {
	case FocusList:
		m.focus = FocusDetail
	case FocusDetail:
		if m.canFocusTap() {
			m.focus = FocusTap
		} else {
			m.focus = FocusList
		}
	default:
		m.focus = FocusList
	}
}

func (m *Model) prevFocus() {
	switch m.focus {
	case FocusTap:
		m.focus = FocusDetail
	case FocusDetail:
		m.focus = FocusList
	default:
		if m.canFocusTap() {
			m.focus = FocusTap
		} else {
			m.focus = FocusDetail
		}
	}
}

func (m *Model) canFocusTap() bool {
	return m.viewMode == ViewMigrations && m.usingThreePane()
}

func (m *Model) syncTableCursor() {
	m.cursor = clamp(m.cursor, 0, maxInt(0, m.currentItemCount()-1))
	switch m.viewMode {
	case ViewMigrations:
		m.migrationsTable.SetCursor(m.cursor)
	case ViewGaps:
		m.gapsTable.SetCursor(m.cursor)
	}
}
