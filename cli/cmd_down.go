package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) downCmd() *cobra.Command {
	var (
		steps int
		to    string
	)

	cmd := &cobra.Command{
		Use:   "down",
		Short: "Rollback migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			if steps > 0 && to != "" {
				return fmt.Errorf("cannot use both --steps and --to")
			}

			if steps == 0 && to == "" {
				steps = 1
			}

			operation := fmt.Sprintf("rollback %d migration(s)", steps)
			if to != "" {
				operation = fmt.Sprintf("rollback to version %s", to)
			}
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				if to != "" {
					return migrateDownToVersion(ctx, q, to, app.config.Yes)
				}

				if err := q.Down(ctx, steps); err != nil {
					return fmt.Errorf("failed to rollback migrations: %w", err)
				}

				fmt.Printf("Rolled back %d migration(s)\n", steps)
				return nil
			})
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to rollback (default: 1)")
	cmd.Flags().StringVar(&to, "to", "", "Rollback to specific version (exclusive - keeps the target)")

	return cmd
}
