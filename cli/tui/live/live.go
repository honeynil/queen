// Package live renders a Bubble Tea live-view of queen migration tap events.
package live

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/queen/tap"
)

// Runner is the function that performs the migration work in a goroutine
// while the TUI runs in the foreground. It receives the sink to install
// on its Queen instance and is expected to return when migrations finish
// (or fail). The error is surfaced in the TUI footer.
type Runner func(ctx context.Context, sink tap.Sink) error

// Run launches the live-view TUI and the Runner concurrently. It blocks
// until the user quits the TUI (q / ctrl+c) or the Runner finishes and
// the user dismisses the screen.
func Run(ctx context.Context, runner Runner) error {
	sink := tap.NewChannelSink(1024)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	doneCh := make(chan error, 1)
	go func() {
		err := runner(runCtx, sink)
		// Allow the TUI to drain the channel before closing.
		time.Sleep(50 * time.Millisecond)
		sink.Close()
		doneCh <- err
	}()

	m := newModel(sink, doneCh, cancel)
	p := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return err
	}
	if m, ok := finalModel.(*model); ok && m.finished {
		return m.runErr
	}
	// Wait for runner to settle.
	select {
	case rerr := <-doneCh:
		return rerr
	case <-time.After(2 * time.Second):
		return nil
	}
}
