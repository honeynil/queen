package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				if err := q.Validate(ctx); err != nil {
					return fmt.Errorf("validation failed: %w", err)
				}

				fmt.Println("All migrations are valid")
				return nil
			})
		},
	}
}
