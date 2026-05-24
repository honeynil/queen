package cli

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/cli/tui"
)

func (app *App) tuiCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tui",
		Short: "Launch interactive Terminal UI",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				model := tui.NewModel(q, ctx)
				p := tea.NewProgram(
					model,
					tea.WithAltScreen(),
					tea.WithMouseCellMotion(),
				)

				if _, err := p.Run(); err != nil {
					return fmt.Errorf("failed to start TUI: %w", err)
				}

				return nil
			})
		},
	}

	return cmd
}
