package tui

import (
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/yaop-labs/queen/tap"
)

func (m *Model) startTappedOperation(label string, work func() operationCompleteMsg) (tea.Model, tea.Cmd) {
	sink := tap.NewChannelSink(2048)
	cfg := tap.DefaultAnalyzerConfig()
	analyzed := tap.NewAnalyzerSink(sink, cfg)

	m.queen.SetTap(analyzed)
	m.tapSink = sink
	m.tapEvents = nil
	m.tapCursor = 0
	m.tapFollow = true
	m.explainResult = nil
	m.explainError = ""
	m.explainQuery = ""
	m.explainMode = ""
	m.explainEvent = tap.Event{}
	m.explainPinned = false
	m.showTap = true
	m.operationLabel = label
	m.loading = true
	m.spinnerActive = true
	m.message = ""

	return m, tea.Batch(func() tea.Msg {
		defer func() {
			m.queen.SetTap(nil)
			time.Sleep(40 * time.Millisecond)
			sink.Close()
		}()
		return work()
	}, m.waitForTapEvent(), m.spinner.Tick)
}

func (m *Model) waitForTapEvent() tea.Cmd {
	sink := m.tapSink
	if sink == nil {
		return nil
	}
	return func() tea.Msg {
		e, ok := <-sink.Ch
		if !ok {
			return tapClosedMsg{}
		}
		return tapEventMsg(e)
	}
}

func (m *Model) runExplain(mode tap.ExplainMode) (tea.Model, tea.Cmd) {
	m.ensurePanels()
	ev, index, ok := m.selectedTapExec()
	if !ok {
		m.message = "Select a SQL event to run explain"
		m.messageType = MessageWarning
		return m, nil
	}
	query := ev.SQL
	if query == "" {
		m.message = "Selected tap event has no SQL"
		m.messageType = MessageWarning
		return m, nil
	}
	displayQuery := ev.BoundSQL
	if displayQuery == "" {
		displayQuery = query
	}
	operation := ev.Operation
	if operation == "" {
		operation = tap.OperationOf(query)
	}
	if mode == tap.ExplainAnalyze && operation != "select" {
		m.message = "EXPLAIN ANALYZE is only enabled for SELECT events"
		m.messageType = MessageWarning
		return m, nil
	}
	if m.queen == nil {
		m.message = "Explain is unavailable without a Queen instance"
		m.messageType = MessageWarning
		return m, nil
	}

	m.loading = true
	m.spinnerActive = true
	m.tapCursor = index
	m.tapFollow = false
	m.explainResult = nil
	m.explainError = ""
	m.explainQuery = displayQuery
	m.explainMode = mode
	m.explainEvent = ev
	m.explainPinned = true
	m.tapVP.GotoTop()
	m.message = fmt.Sprintf("Running %s...", mode)
	m.messageType = MessageInfo
	return m, tea.Batch(func() tea.Msg {
		result, err := tap.RunExplain(m.ctx, m.queen.SQLDB(), m.queen.DriverName(), mode, query, ev.Args)
		return explainResultMsg{result: result, err: err}
	}, m.spinner.Tick)
}

func (m *Model) clearExplain() {
	m.explainResult = nil
	m.explainError = ""
	m.explainQuery = ""
	m.explainMode = ""
	m.explainEvent = tap.Event{}
	m.explainPinned = false
}

func (m *Model) selectedTapExec() (tap.Event, int, bool) {
	if len(m.tapEvents) == 0 {
		return tap.Event{}, 0, false
	}
	if m.tapCursor < 0 {
		m.tapCursor = 0
	}
	if m.tapCursor >= len(m.tapEvents) {
		m.tapCursor = len(m.tapEvents) - 1
	}
	for i := m.tapCursor; i >= 0; i-- {
		if m.tapEvents[i].Kind == tap.KindExec {
			return m.tapEvents[i], i, true
		}
	}
	for i := m.tapCursor + 1; i < len(m.tapEvents); i++ {
		if m.tapEvents[i].Kind == tap.KindExec {
			return m.tapEvents[i], i, true
		}
	}
	return tap.Event{}, 0, false
}
