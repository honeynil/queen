package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) resetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Rollback all migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			operation := "RESET ALL MIGRATIONS (WARNING: DESTRUCTIVE)"
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			if !app.config.Yes {
				if !confirm("WARNING: This will rollback ALL migrations. Are you absolutely sure?") {
					return fmt.Errorf("operation canceled")
				}
			}

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			if err := q.Reset(ctx); err != nil {
				return fmt.Errorf("failed to reset migrations: %w", err)
			}

			fmt.Println("All migrations have been rolled back")
			return nil
		},
	}
}
