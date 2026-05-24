package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) resetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Rollback all migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			operation := "RESET ALL MIGRATIONS (WARNING: DESTRUCTIVE)"
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			if !app.config.Yes {
				if !confirm("WARNING: This will rollback ALL migrations. Are you absolutely sure?") {
					return fmt.Errorf("operation canceled")
				}
			}

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				if err := q.Reset(ctx); err != nil {
					return fmt.Errorf("failed to reset migrations: %w", err)
				}

				fmt.Println("All migrations have been rolled back")
				return nil
			})
		},
	}
}
