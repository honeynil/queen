package cli

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

const (
	statusPass    = "pass"
	statusWarning = "warning"
	statusFail    = "fail"
)

// DoctorResult represents a health check result.
type DoctorResult struct {
	Check    string `json:"check"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	Details  string `json:"details,omitempty"`
	Severity string `json:"severity,omitempty"`
}

type doctorOptions struct {
	deep bool
	gaps bool
	fix  bool
}

func (app *App) doctorCmd() *cobra.Command {
	var (
		deep bool
		gaps bool
		fix  bool
	)

	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run migration health checks and diagnostics",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				results := collectDoctorResults(ctx, q, doctorOptions{
					deep: deep,
					gaps: gaps,
					fix:  fix,
				})

				if app.config.JSON {
					return outputDoctorJSON(results)
				}
				return outputDoctorTable(results)
			})
		},
	}

	cmd.Flags().BoolVar(&deep, "deep", false, "Deep schema validation (slower)")
	cmd.Flags().BoolVar(&gaps, "gaps", false, "Only check for gaps")
	cmd.Flags().BoolVar(&fix, "fix", false, "Attempt to auto-fix issues")

	return cmd
}

func collectDoctorResults(ctx context.Context, q *queen.Queen, opts doctorOptions) []DoctorResult {
	results := make([]DoctorResult, 0)

	if opts.gaps {
		results = append(results, checkGaps(ctx, q))
		if opts.fix {
			results = append(results, attemptAutoFix(ctx, q, results)...)
		}
		return results
	}

	results = append(results, checkDatabaseConnection(ctx, q))
	results = append(results, checkMigrationTable(ctx, q))
	results = append(results, checkChecksums(ctx, q))

	if !opts.deep {
		results = append(results, checkGaps(ctx, q))
	}

	results = append(results, checkRegistrationSync(ctx, q))

	if opts.deep {
		results = append(results, checkSchemaConsistency(ctx, q))
	}

	if opts.fix {
		results = append(results, attemptAutoFix(ctx, q, results)...)
	}

	return results
}
