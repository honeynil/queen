package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show current migration version",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				statuses, err := q.Status(ctx)
				if err != nil {
					return fmt.Errorf("failed to get migration status: %w", err)
				}

				var latestVersion string
				var latestName string
				for _, s := range statuses {
					if s.Status == queen.StatusApplied {
						latestVersion = s.Version
						latestName = s.Name
					}
				}

				if latestVersion == "" {
					fmt.Println("No migrations have been applied yet")
				} else {
					fmt.Printf("Current version: %s (%s)\n", latestVersion, latestName)
				}

				return nil
			})
		},
	}
}
