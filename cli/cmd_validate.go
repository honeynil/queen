package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			if err := q.Validate(ctx); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}

			fmt.Println("All migrations are valid")
			return nil
		},
	}
}
