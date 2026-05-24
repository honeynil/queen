package live

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/queen/tap"
)

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.cancel()
			return m, tea.Quit
		case "up", "k":
			m.moveSelection(-1)
		case "down", "j":
			m.moveSelection(1)
		case "pgup", "b":
			m.moveSelection(-m.visibleStreamHeight())
		case "pgdown", "f":
			m.moveSelection(m.visibleStreamHeight())
		case "home", "g":
			m.selected = 0
			m.ensureSelectionVisible()
		case "end", "G":
			m.selected = len(m.rows) - 1
			m.ensureSelectionVisible()
		}
		return m, nil

	case eventMsg:
		ev := tap.Event(msg)
		follow := m.isFollowingTail()
		m.rows = append(m.rows, row{ev: ev, atRel: time.Since(m.startAt)})
		if m.selected < 0 || follow {
			m.selected = len(m.rows) - 1
		}
		m.updateCounters(ev)
		m.ensureSelectionVisible()
		return m, m.waitForEvent()

	case closedMsg:
		return m, nil

	case doneMsg:
		m.finished = true
		m.finishedAt = time.Now()
		m.runErr = msg.err
		return m, nil

	case tickMsg:
		if m.finished {
			return m, nil
		}
		return m, tick()
	}
	return m, nil
}

func (m *model) updateCounters(ev tap.Event) {
	switch ev.Kind {
	case tap.KindStart:
		m.migStarted++
	case tap.KindEnd:
		m.migFinished++
		if ev.Error != "" {
			m.errors++
		}
	case tap.KindExec:
		m.execs++
		if ev.Error != "" {
			m.errors++
		}
		if ev.Slow {
			m.slow++
		}
		if ev.NPlus1 {
			m.nplus1++
		}
	}
}
