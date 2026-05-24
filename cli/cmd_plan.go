package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) planCmd() *cobra.Command {
	var direction string
	var limit int

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show migration execution plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				plans, err := q.DryRun(ctx, direction, limit)
				if err != nil {
					return fmt.Errorf("failed to generate migration plan: %w", err)
				}

				if app.config.JSON {
					return app.outputPlanJSON(plans, direction)
				}
				return app.outputPlanTable(plans, direction)
			})
		},
	}

	cmd.Flags().StringVar(&direction, "direction", "up", "Migration direction: up or down")
	cmd.Flags().IntVar(&limit, "limit", 0, "Limit number of migrations to show (0 = all)")

	return cmd
}
