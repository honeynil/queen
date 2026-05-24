package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

// attemptAutoFix provides actionable recommendations for detected issues.
func attemptAutoFix(_ context.Context, _ *queen.Queen, results []DoctorResult) []DoctorResult {
	fixes := make([]DoctorResult, 0)

	for _, result := range results {
		if result.Status != statusFail && result.Status != statusWarning {
			continue
		}

		switch result.Check {
		case "Gap Detection":
			commands := make([]string, 0, 2)
			commands = append(commands, "queen gap detect    # see gap details")
			commands = append(commands, "queen gap fill      # fill detected gaps")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Gaps",
				Status:  statusWarning,
				Message: "Run the following commands to resolve gaps",
				Details: strings.Join(commands, "\n"),
			})

		case "Checksum Validation":
			versions := extractVersionsFromDetails(result.Details)
			var commands []string
			if len(versions) > 0 {
				for _, v := range versions {
					commands = append(commands, fmt.Sprintf("queen baseline --version %s    # accept current version of migration %s", v, v))
				}
			}
			commands = append(commands, "# WARNING: Only use baseline if you are sure the migration code matches the applied schema")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Checksums",
				Status:  statusWarning,
				Message: "Checksum mismatches detected — migrations were modified after being applied",
				Details: strings.Join(commands, "\n"),
			})

		case "Registration Sync":
			versions := extractVersionsFromDetails(result.Details)
			var commands []string
			if len(versions) > 0 {
				for _, v := range versions {
					commands = append(commands, fmt.Sprintf("queen baseline --version %s    # register migration %s as applied", v, v))
				}
			} else {
				commands = append(commands, "queen status    # review current state")
			}
			commands = append(commands, "# Or add the missing migration code to your application")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Registration",
				Status:  statusWarning,
				Message: "Applied migrations not found in code — add them or mark as baseline",
				Details: strings.Join(commands, "\n"),
			})

		case "Schema Consistency":
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Schema",
				Status:  statusWarning,
				Message: "Review the schema issues above and fix the migration SQL manually",
				Details: "queen explain <version>    # inspect a specific migration",
			})
		}
	}

	if len(fixes) == 0 {
		fixes = append(fixes, DoctorResult{
			Check:   "Auto-Fix",
			Status:  statusPass,
			Message: "No issues require fixing",
		})
	}

	return fixes
}

// extractVersionsFromDetails parses version numbers from doctor result details.
func extractVersionsFromDetails(details string) []string {
	// expected forms: "Modified versions: 001, 002, 003" or "Unregistered: 001, 002"
	parts := strings.SplitN(details, ": ", 2)
	if len(parts) < 2 {
		return nil
	}

	versionStr := strings.TrimSpace(parts[1])
	if versionStr == "" {
		return nil
	}

	versions := strings.Split(versionStr, ", ")
	result := make([]string, 0, len(versions))
	for _, v := range versions {
		v = strings.TrimSpace(v)
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}
