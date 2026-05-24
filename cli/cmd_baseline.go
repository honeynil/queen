package cli

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) baselineCmd() *cobra.Command {
	var (
		name    string
		at      string
		version string
		dryRun  bool
	)

	cmd := &cobra.Command{
		Use:   "baseline",
		Short: "Mark existing schema migrations as applied",
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				name = "baseline"
			}
			if version != "" {
				at = version
			}

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				return app.runBaseline(ctx, q, baselineOptions{
					name:   name,
					at:     at,
					dryRun: dryRun,
				})
			})
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Label stored in baseline metadata")
	cmd.Flags().StringVar(&at, "at", "", "Mark migrations up to this version as applied")
	cmd.Flags().StringVar(&version, "version", "", "Alias for --at")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview baseline without making changes")

	return cmd
}
