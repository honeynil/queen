package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) gotoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "goto VERSION",
		Short: "Migrate to specific version (up or down)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetVersion := args[0]

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				if targetVersion == "latest" {
					return q.Up(ctx)
				}

				statuses, err := q.Status(ctx)
				if err != nil {
					return fmt.Errorf("failed to get migration status: %w", err)
				}

				targetIndex := -1
				var targetMigration *queen.MigrationStatus
				for i, s := range statuses {
					if s.Version == targetVersion {
						targetIndex = i
						targetMigration = &s
						break
					}
				}

				if targetMigration == nil {
					return fmt.Errorf("migration version not found: %s", targetVersion)
				}

				if targetMigration.Status == queen.StatusPending {
					return app.migrateUpTo(ctx, q, statuses, targetIndex)
				}
				return app.migrateDownTo(ctx, q, statuses, targetIndex)
			})
		},
	}

	return cmd
}
