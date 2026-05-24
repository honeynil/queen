package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func (app *App) checkCmd() *cobra.Command {
	var (
		ci           bool
		noPending    bool
		noGaps       bool
		rollbackTest bool
	)

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Quick validation for CI/CD pipelines",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := commandContext(cmd)
			q, err := app.setupQueen(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
				os.Exit(2)
			}
			defer func() { _ = q.Close() }()

			summary := runPipelineChecks(ctx, q, checkOptions{
				ci:           ci,
				noPending:    noPending,
				noGaps:       noGaps,
				rollbackTest: rollbackTest,
			})

			outputCheckSummary(summary)
			if summary.failed > 0 {
				os.Exit(summary.exitCode)
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&ci, "ci", false, "CI mode (strict validation, fails on warnings)")
	cmd.Flags().BoolVar(&noPending, "no-pending", false, "Fail if pending migrations exist")
	cmd.Flags().BoolVar(&noGaps, "no-gaps", false, "Fail if gaps are detected")
	cmd.Flags().BoolVar(&rollbackTest, "rollback-test", false, "Run an up/reset/up rollback cycle against a clean test database")

	return cmd
}

func outputCheckSummary(summary checkSummary) {
	fmt.Println()
	if summary.failed > 0 {
		fmt.Printf("Checks: %d passed, %d failed\n", summary.passed, summary.failed)
		return
	}

	total := summary.passed + summary.failed
	fmt.Printf("All checks passed (%d/%d)\n", summary.passed, total)
}
