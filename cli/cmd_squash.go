package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) squashCmd() *cobra.Command {
	var (
		into    string
		fromVer string
		toVer   string
		dryRun  bool
	)

	cmd := &cobra.Command{
		Use:   "squash VERSIONS...",
		Short: "Combine multiple migrations into one",
		RunE: func(cmd *cobra.Command, args []string) error {
			if into == "" {
				return fmt.Errorf("--into flag is required")
			}
			if !queen.IsValidMigrationName(into) {
				return fmt.Errorf("invalid squash name: must contain only lowercase letters, numbers, and underscores")
			}

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				versions, err := resolveSquashVersions(ctx, q, args, fromVer, toVer)
				if err != nil {
					return err
				}
				return app.runSquash(q, squashOptions{
					versions: versions,
					into:     into,
					dryRun:   dryRun,
				})
			})
		},
	}

	cmd.Flags().StringVar(&into, "into", "", "Name for the squashed migration (required)")
	cmd.Flags().StringVar(&fromVer, "from", "", "Start version for range squash")
	cmd.Flags().StringVar(&toVer, "to", "", "End version for range squash")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview squash without making changes")

	return cmd
}
