package cli

import (
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

func outputGapsTable(gaps []queen.Gap) {
	errors := make([]queen.Gap, 0)
	warnings := make([]queen.Gap, 0)

	for _, gap := range gaps {
		if gap.Severity == "error" {
			errors = append(errors, gap)
		} else {
			warnings = append(warnings, gap)
		}
	}

	if len(errors) > 0 {
		outputWarning("Errors (%d):", len(errors))
		outputBlank()
		for _, gap := range errors {
			fmt.Printf("  [%s] %s\n", gap.Type, gap.Version)
			fmt.Printf("    %s\n", gap.Description)
			if gap.AppliedAt != nil {
				fmt.Printf("    Applied at: %s\n", *gap.AppliedAt)
			}
			outputBlank()
		}
	}

	if len(warnings) > 0 {
		outputWarning("Warnings (%d):", len(warnings))
		outputBlank()
		for _, gap := range warnings {
			fmt.Printf("  [%s] %s", gap.Type, gap.Version)
			if gap.Name != "" {
				fmt.Printf(" - %s", gap.Name)
			}
			fmt.Println()
			fmt.Printf("    %s\n", gap.Description)
			if len(gap.BlockedBy) > 0 {
				fmt.Printf("    Blocking: %s\n", strings.Join(gap.BlockedBy, ", "))
			}
			outputBlank()
		}
	}

	fmt.Printf("Total gaps: %d (%d errors, %d warnings)\n", len(gaps), len(errors), len(warnings))
	fmt.Println("\nRun 'queen gap analyze' for more details")
}

func outputGapsJSON(gaps []queen.Gap) error {
	return outputJSON(gaps)
}

func analyzeGaps(gaps []queen.Gap) {
	fmt.Println("Gap Analysis")
	outputRule("=", 50)
	outputBlank()

	byType := make(map[queen.GapType][]queen.Gap)
	for _, gap := range gaps {
		byType[gap.Type] = append(byType[gap.Type], gap)
	}

	if numGaps, ok := byType[queen.GapTypeNumbering]; ok && len(numGaps) > 0 {
		fmt.Printf("Numbering Gaps (%d):\n", len(numGaps))
		fmt.Println("These are missing version numbers in your sequence.")
		fmt.Println("Impact: Low - These gaps are usually intentional (deleted migrations).")
		outputBlank()
		for _, gap := range numGaps {
			fmt.Printf("  • %s\n", gap.Description)
		}
		outputBlank()
	}

	if appGaps, ok := byType[queen.GapTypeApplication]; ok && len(appGaps) > 0 {
		fmt.Printf("Application Gaps (%d):\n", len(appGaps))
		fmt.Println("These migrations were skipped but later ones were applied.")
		fmt.Println("Impact: Medium - May cause inconsistencies.")
		fmt.Println("Recommendation: Run 'queen gap fill' to apply missing migrations.")
		outputBlank()
		for _, gap := range appGaps {
			fmt.Printf("  • %s - %s\n", gap.Version, gap.Name)
			if len(gap.BlockedBy) > 0 {
				fmt.Printf("    Blocks: %s\n", strings.Join(gap.BlockedBy, ", "))
			}
		}
		outputBlank()
	}

	if unregGaps, ok := byType[queen.GapTypeUnregistered]; ok && len(unregGaps) > 0 {
		fmt.Printf("Unregistered Migrations (%d):\n", len(unregGaps))
		fmt.Println("These migrations exist in the database but not in code.")
		fmt.Println("Impact: High - Code and database are out of sync.")
		fmt.Println("Recommendation: Add these migrations to your code or investigate.")
		outputBlank()
		for _, gap := range unregGaps {
			fmt.Printf("  • %s - %s", gap.Version, gap.Name)
			if gap.AppliedAt != nil {
				fmt.Printf(" (applied: %s)", *gap.AppliedAt)
			}
			fmt.Println()
		}
		outputBlank()
	}
}
