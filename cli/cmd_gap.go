package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os/user"
	"slices"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) gapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gap",
		Short: "Detect and manage migration gaps",
	}

	cmd.AddCommand(
		app.gapDetectCmd(),
		app.gapAnalyzeCmd(),
		app.gapFillCmd(),
		app.gapIgnoreCmd(),
		app.gapListIgnoredCmd(),
		app.gapUnignoreCmd(),
	)

	return cmd
}

func (app *App) gapDetectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "detect",
		Short: "Detect migration gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			gaps, err := q.DetectGaps(ctx)
			if err != nil {
				return fmt.Errorf("failed to detect gaps: %w", err)
			}

			if len(gaps) == 0 {
				if !app.config.JSON {
					fmt.Println("No gaps detected")
				} else {
					fmt.Println("[]")
				}
				return nil
			}

			// Output gaps
			if app.config.JSON {
				return outputGapsJSON(gaps)
			}

			outputGapsTable(gaps)
			return nil
		},
	}

	return cmd
}

func (app *App) gapAnalyzeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze gap dependencies and impact",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			gaps, err := q.DetectGaps(ctx)
			if err != nil {
				return fmt.Errorf("failed to detect gaps: %w", err)
			}

			if len(gaps) == 0 {
				fmt.Println("No gaps detected")
				return nil
			}

			// Analyze and display dependencies
			analyzeGaps(gaps)
			return nil
		},
	}

	return cmd
}

func (app *App) gapFillCmd() *cobra.Command {
	var markApplied bool

	cmd := &cobra.Command{
		Use:   "fill [versions...]",
		Short: "Fill detected gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			gaps, err := q.DetectGaps(ctx)
			if err != nil {
				return fmt.Errorf("failed to detect gaps: %w", err)
			}

			if len(gaps) == 0 {
				fmt.Println("No gaps detected")
				return nil
			}

			// Filter application gaps only
			applicationGaps := make([]queen.Gap, 0)
			for _, gap := range gaps {
				if gap.Type == queen.GapTypeApplication {
					if len(args) == 0 || slices.Contains(args, gap.Version) {
						applicationGaps = append(applicationGaps, gap)
					}
				}
			}

			if len(applicationGaps) == 0 {
				fmt.Println("No application gaps to fill")
				return nil
			}

			// Show what will be done
			if markApplied {
				fmt.Println("WARNING: Marking migrations as applied without executing them")
				fmt.Println("This should only be used if migrations were manually applied.")
			}

			fmt.Printf("Will fill %d gap(s):\n", len(applicationGaps))
			for _, gap := range applicationGaps {
				fmt.Printf("  %s - %s\n", gap.Version, gap.Name)
			}
			fmt.Println()

			if !app.config.Yes {
				action := "apply"
				if markApplied {
					action = "mark as applied"
				}
				if !confirm(fmt.Sprintf("Proceed to %s these migrations?", action)) {
					fmt.Println("Canceled")
					return nil
				}
			}

			// Fill gaps
			if markApplied {
				return fillGapsByMarking(ctx, q, applicationGaps)
			}

			// Apply migrations for each gap
			return fillGapsByApplying(ctx, q, applicationGaps)
		},
	}

	cmd.Flags().BoolVar(&markApplied, "mark-applied", false, "Mark migrations as applied without executing")

	return cmd
}

func (app *App) gapIgnoreCmd() *cobra.Command {
	var reason string

	cmd := &cobra.Command{
		Use:   "ignore VERSION",
		Short: "Ignore a specific gap",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]

			// Load or create .queenignore
			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			// Check if already ignored
			if qi.IsIgnored(version) {
				fmt.Printf("WARNING: Version %s is already ignored\n", version)
				if existingReason := qi.GetReason(version); existingReason != "" {
					fmt.Printf("   Existing reason: %s\n", existingReason)
				}
				return nil
			}

			// Get current user for IgnoredBy field
			ignoredBy := "unknown"
			if currentUser, userErr := user.Current(); userErr == nil {
				ignoredBy = currentUser.Username
			}

			// Add to ignore list
			if err := qi.AddIgnore(version, reason, ignoredBy); err != nil {
				return fmt.Errorf("failed to save .queenignore: %w", err)
			}

			fmt.Printf("Added version %s to .queenignore\n", version)
			if reason != "" {
				fmt.Printf("  Reason: %s\n", reason)
			}
			fmt.Println()
			fmt.Println("This gap will now be ignored by 'queen gap detect' and 'queen doctor'")

			return nil
		},
	}

	cmd.Flags().StringVar(&reason, "reason", "", "Reason for ignoring this gap")

	return cmd
}

func (app *App) gapListIgnoredCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-ignored",
		Short: "List all ignored gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load .queenignore
			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			ignored := qi.ListIgnored()
			if len(ignored) == 0 {
				fmt.Println("No ignored gaps")
				return nil
			}

			fmt.Printf("Ignored gaps (%d):\n\n", len(ignored))
			for _, gap := range ignored {
				fmt.Printf("  %s", gap.Version)
				if gap.Reason != "" {
					fmt.Printf(" - %s", gap.Reason)
				}
				fmt.Println()
			}

			return nil
		},
	}

	return cmd
}

func (app *App) gapUnignoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unignore VERSION",
		Short: "Remove a gap from ignore list",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]

			// Load .queenignore
			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			// Check if it's ignored
			if !qi.IsIgnored(version) {
				fmt.Printf("WARNING: Version %s is not in .queenignore\n", version)
				return nil
			}

			// Remove from ignore list
			if err := qi.RemoveIgnore(version); err != nil {
				return fmt.Errorf("failed to save .queenignore: %w", err)
			}

			fmt.Printf("Removed version %s from .queenignore\n", version)
			fmt.Println()
			fmt.Println("This gap will now be detected by 'queen gap detect' and 'queen doctor'")

			return nil
		},
	}

	return cmd
}

// outputGapsTable prints gaps in a formatted table.
func outputGapsTable(gaps []queen.Gap) {
	// Group by severity
	errors := make([]queen.Gap, 0)
	warnings := make([]queen.Gap, 0)

	for _, gap := range gaps {
		if gap.Severity == "error" {
			errors = append(errors, gap)
		} else {
			warnings = append(warnings, gap)
		}
	}

	// Print errors first
	if len(errors) > 0 {
		fmt.Printf("WARNING: Errors (%d):\n\n", len(errors))
		for _, gap := range errors {
			fmt.Printf("  [%s] %s\n", gap.Type, gap.Version)
			fmt.Printf("    %s\n", gap.Description)
			if gap.AppliedAt != nil {
				fmt.Printf("    Applied at: %s\n", *gap.AppliedAt)
			}
			fmt.Println()
		}
	}

	// Print warnings
	if len(warnings) > 0 {
		fmt.Printf("WARNING: Warnings (%d):\n\n", len(warnings))
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
			fmt.Println()
		}
	}

	fmt.Printf("Total gaps: %d (%d errors, %d warnings)\n", len(gaps), len(errors), len(warnings))
	fmt.Println("\nRun 'queen gap analyze' for more details")
}

// outputGapsJSON prints gaps in JSON format.
func outputGapsJSON(gaps []queen.Gap) error {
	data, err := json.MarshalIndent(gaps, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

// analyzeGaps performs detailed gap analysis.
func analyzeGaps(gaps []queen.Gap) {
	fmt.Println("Gap Analysis")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	byType := make(map[queen.GapType][]queen.Gap)
	for _, gap := range gaps {
		byType[gap.Type] = append(byType[gap.Type], gap)
	}

	if numGaps, ok := byType[queen.GapTypeNumbering]; ok && len(numGaps) > 0 {
		fmt.Printf("Numbering Gaps (%d):\n", len(numGaps))
		fmt.Println("These are missing version numbers in your sequence.")
		fmt.Println("Impact: Low - These gaps are usually intentional (deleted migrations).")
		fmt.Println()
		for _, gap := range numGaps {
			fmt.Printf("  • %s\n", gap.Description)
		}
		fmt.Println()
	}

	if appGaps, ok := byType[queen.GapTypeApplication]; ok && len(appGaps) > 0 {
		fmt.Printf("Application Gaps (%d):\n", len(appGaps))
		fmt.Println("These migrations were skipped but later ones were applied.")
		fmt.Println("Impact: Medium - May cause inconsistencies.")
		fmt.Println("Recommendation: Run 'queen gap fill' to apply missing migrations.")
		fmt.Println()
		for _, gap := range appGaps {
			fmt.Printf("  • %s - %s\n", gap.Version, gap.Name)
			if len(gap.BlockedBy) > 0 {
				fmt.Printf("    Blocks: %s\n", strings.Join(gap.BlockedBy, ", "))
			}
		}
		fmt.Println()
	}

	if unregGaps, ok := byType[queen.GapTypeUnregistered]; ok && len(unregGaps) > 0 {
		fmt.Printf("Unregistered Migrations (%d):\n", len(unregGaps))
		fmt.Println("These migrations exist in the database but not in code.")
		fmt.Println("Impact: High - Code and database are out of sync.")
		fmt.Println("Recommendation: Add these migrations to your code or investigate.")
		fmt.Println()
		for _, gap := range unregGaps {
			fmt.Printf("  • %s - %s", gap.Version, gap.Name)
			if gap.AppliedAt != nil {
				fmt.Printf(" (applied: %s)", *gap.AppliedAt)
			}
			fmt.Println()
		}
		fmt.Println()
	}
}

// fillGapsByApplying applies the gap migrations one by one.
func fillGapsByApplying(ctx context.Context, q *queen.Queen, gaps []queen.Gap) error {
	// Get all statuses to find migration objects
	statuses, err := q.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	// Build a map of version -> index for quick lookup
	statusMap := make(map[string]int)
	for i, s := range statuses {
		statusMap[s.Version] = i
	}

	// Apply each gap migration
	successCount := 0
	for _, gap := range gaps {
		fmt.Printf("Applying %s - %s... ", gap.Version, gap.Name)

		// Find the index in statuses
		targetIndex, exists := statusMap[gap.Version]
		if !exists {
			fmt.Printf("SKIP (not found)\n")
			continue
		}

		// Count how many migrations we need to apply to reach this one
		stepsToApply := 0
		for i := 0; i <= targetIndex; i++ {
			if statuses[i].Status == queen.StatusPending {
				stepsToApply++
			}
		}

		if stepsToApply == 0 {
			fmt.Printf("SKIP (already applied)\n")
			continue
		}

		// Apply up to this migration
		if err := q.UpSteps(ctx, stepsToApply); err != nil {
			fmt.Printf("FAILED\n")
			return fmt.Errorf("failed to apply migration %s: %w", gap.Version, err)
		}

		fmt.Printf("OK\n")
		successCount++

		// Reload statuses after applying
		statuses, err = q.Status(ctx)
		if err != nil {
			return fmt.Errorf("failed to reload status: %w", err)
		}
	}

	fmt.Println()
	fmt.Printf("Successfully filled %d gap(s)\n", successCount)
	return nil
}

// fillGapsByMarking marks migrations as applied without executing them.
func fillGapsByMarking(ctx context.Context, q *queen.Queen, gaps []queen.Gap) error {
	fmt.Println("WARNING: Marking migrations as applied without executing SQL (dangerous operation)")
	fmt.Println()

	currentUser := "unknown"
	if u, err := user.Current(); err == nil {
		currentUser = u.Username
	}

	successCount := 0
	for _, gap := range gaps {
		fmt.Printf("Marking %s - %s... ", gap.Version, gap.Name)

		m := q.FindMigration(gap.Version)
		if m == nil {
			fmt.Printf("SKIP (not registered in code)\n")
			continue
		}

		meta := &queen.MigrationMetadata{
			AppliedBy: currentUser,
			Action:    "mark-applied",
			Status:    "success",
		}

		if err := q.Driver().Record(ctx, m, meta); err != nil {
			fmt.Printf("FAILED: %v\n", err)
			return fmt.Errorf("failed to mark migration %s: %w", gap.Version, err)
		}

		fmt.Printf("OK\n")
		successCount++
	}

	fmt.Println()
	fmt.Printf("Successfully marked %d migration(s) as applied\n", successCount)
	return nil
}
