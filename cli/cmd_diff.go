package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) diffCmd() *cobra.Command {
	var showSQL bool

	cmd := &cobra.Command{
		Use:   "diff VERSION1 VERSION2",
		Short: "Compare two migration versions",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			version1 := args[0]
			version2 := args[1]

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				statuses, err := q.Status(ctx)
				if err != nil {
					return fmt.Errorf("failed to get migration status: %w", err)
				}

				v1, err := resolveVersion(version1, statuses)
				if err != nil {
					return fmt.Errorf("invalid version1: %w", err)
				}

				v2, err := resolveVersion(version2, statuses)
				if err != nil {
					return fmt.Errorf("invalid version2: %w", err)
				}

				if strings.HasPrefix(version2, "+") || strings.HasPrefix(version2, "-") {
					v2, err = resolveRelativeVersion(v1, version2, statuses)
					if err != nil {
						return fmt.Errorf("invalid relative version: %w", err)
					}
				}

				migrations, direction, err := getMigrationsBetween(statuses, v1, v2)
				if err != nil {
					return err
				}

				if len(migrations) == 0 {
					fmt.Println("No migrations between versions")
					return nil
				}

				if app.config.JSON {
					return outputDiffJSON(migrations, v1, v2, direction)
				}

				outputDiffTable(migrations, v1, v2, direction, showSQL)
				return nil
			})
		},
	}

	cmd.Flags().BoolVar(&showSQL, "show-sql", false, "Show SQL statements for each migration")

	return cmd
}
