package live

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/queen/tap"
)

type (
	eventMsg  tap.Event
	closedMsg struct{}
	doneMsg   struct{ err error }
	tickMsg   time.Time
)

type row struct {
	ev    tap.Event
	atRel time.Duration
}

type viewRow struct {
	index int
	row   row
	line  string
}

type model struct {
	sink   *tap.ChannelSink
	doneCh <-chan error
	cancel context.CancelFunc

	rows       []row
	startAt    time.Time
	finishedAt time.Time
	finished   bool
	runErr     error

	width, height int
	scroll        int
	selected      int

	migStarted  int
	migFinished int
	execs       int
	errors      int
	slow        int
	nplus1      int
}

func newModel(sink *tap.ChannelSink, doneCh <-chan error, cancel context.CancelFunc) *model {
	return &model{
		sink:     sink,
		doneCh:   doneCh,
		cancel:   cancel,
		startAt:  time.Now(),
		width:    100,
		height:   24,
		selected: -1,
	}
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(m.waitForEvent(), m.waitForDone(), tick())
}

func (m *model) waitForEvent() tea.Cmd {
	return func() tea.Msg {
		e, ok := <-m.sink.Ch
		if !ok {
			return closedMsg{}
		}
		return eventMsg(e)
	}
}

func (m *model) waitForDone() tea.Cmd {
	return func() tea.Msg {
		err := <-m.doneCh
		return doneMsg{err: err}
	}
}

func tick() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}
