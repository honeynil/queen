package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) explainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "explain <version>",
		Short: "Explain a specific migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			version := args[0]

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			plan, err := q.Explain(ctx, version)
			if err != nil {
				return fmt.Errorf("failed to explain migration: %w", err)
			}

			if app.config.JSON {
				return app.outputExplainJSON(plan)
			}
			app.outputExplainTable(plan)
			return nil
		},
	}
}

func (app *App) outputExplainTable(plan *queen.MigrationPlan) {
	fmt.Printf("Migration: %s\n", plan.Version)
	fmt.Println(strings.Repeat("━", 60))
	fmt.Println()

	fmt.Printf("Name:          %s\n", plan.Name)
	fmt.Printf("Status:        %s\n", plan.Status)
	fmt.Printf("Type:          %s\n", plan.Type)
	fmt.Printf("Direction:     %s\n", plan.Direction)
	fmt.Printf("Has Rollback:  %v\n", plan.HasRollback)
	fmt.Printf("Checksum:      %s\n", plan.Checksum)

	if plan.IsDestructive {
		fmt.Printf("Destructive:   WARNING: YES\n")
	}

	if len(plan.Warnings) > 0 {
		fmt.Println()
		fmt.Println("WARNING: Warnings:")
		for _, warning := range plan.Warnings {
			fmt.Printf("  - %s\n", warning)
		}
	}

	if plan.SQL != "" {
		fmt.Println()
		fmt.Printf("%s SQL:\n", strings.ToUpper(plan.Direction))
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println(plan.SQL)
		fmt.Println(strings.Repeat("-", 60))
	} else if plan.Type == queen.MigrationTypeGoFunc {
		fmt.Println()
		fmt.Printf("%s: Go function (code not shown)\n", strings.ToUpper(plan.Direction))
	}
}

func (app *App) outputExplainJSON(plan *queen.MigrationPlan) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(plan)
}
