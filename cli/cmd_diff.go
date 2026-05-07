package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
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

			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			// Get migration status
			statuses, err := q.Status(ctx)
			if err != nil {
				return fmt.Errorf("failed to get migration status: %w", err)
			}

			// Resolve version keywords
			v1, err := resolveVersion(version1, statuses)
			if err != nil {
				return fmt.Errorf("invalid version1: %w", err)
			}

			v2, err := resolveVersion(version2, statuses)
			if err != nil {
				return fmt.Errorf("invalid version2: %w", err)
			}

			// Handle relative versions (+N, -N)
			if strings.HasPrefix(version2, "+") || strings.HasPrefix(version2, "-") {
				v2, err = resolveRelativeVersion(v1, version2, statuses)
				if err != nil {
					return fmt.Errorf("invalid relative version: %w", err)
				}
			}

			// Get migrations between versions
			migrations, direction, err := getMigrationsBetween(statuses, v1, v2)
			if err != nil {
				return err
			}

			if len(migrations) == 0 {
				fmt.Println("No migrations between versions")
				return nil
			}

			// Output
			if app.config.JSON {
				return outputDiffJSON(migrations, v1, v2, direction)
			}

			outputDiffTable(migrations, v1, v2, direction, showSQL)
			return nil
		},
	}

	cmd.Flags().BoolVar(&showSQL, "show-sql", false, "Show SQL statements for each migration")

	return cmd
}

// resolveVersion resolves special version keywords to actual versions.
func resolveVersion(version string, statuses []queen.MigrationStatus) (string, error) {
	switch version {
	case "current":
		// Find last applied migration
		for i := len(statuses) - 1; i >= 0; i-- {
			if statuses[i].Status == queen.StatusApplied {
				return statuses[i].Version, nil
			}
		}
		return "", fmt.Errorf("no migrations applied yet")

	case "latest":
		// Return last registered migration
		if len(statuses) > 0 {
			return statuses[len(statuses)-1].Version, nil
		}
		return "", fmt.Errorf("no migrations registered")

	default:
		// Return as-is (literal version or relative like +3)
		return version, nil
	}
}

// resolveRelativeVersion resolves relative versions like +3 or -2.
func resolveRelativeVersion(baseVersion string, relative string, statuses []queen.MigrationStatus) (string, error) {
	// Find base version index
	baseIndex := -1
	for i, s := range statuses {
		if s.Version == baseVersion {
			baseIndex = i
			break
		}
	}

	if baseIndex == -1 {
		return "", fmt.Errorf("base version not found: %s", baseVersion)
	}

	// Parse relative offset
	var offset int
	if strings.HasPrefix(relative, "+") {
		if _, err := fmt.Sscanf(relative, "+%d", &offset); err != nil {
			return "", fmt.Errorf("invalid relative offset format: %s", relative)
		}
	} else if strings.HasPrefix(relative, "-") {
		if _, err := fmt.Sscanf(relative, "-%d", &offset); err != nil {
			return "", fmt.Errorf("invalid relative offset format: %s", relative)
		}
		offset = -offset
	}

	targetIndex := baseIndex + offset
	if targetIndex < 0 || targetIndex >= len(statuses) {
		return "", fmt.Errorf("relative version out of range")
	}

	return statuses[targetIndex].Version, nil
}

// getMigrationsBetween returns migrations between two versions.
func getMigrationsBetween(statuses []queen.MigrationStatus, v1, v2 string) ([]queen.MigrationStatus, string, error) {
	// Find indices
	idx1, idx2 := -1, -1
	for i, s := range statuses {
		if s.Version == v1 {
			idx1 = i
		}
		if s.Version == v2 {
			idx2 = i
		}
	}

	if idx1 == -1 {
		return nil, "", fmt.Errorf("version not found: %s", v1)
	}
	if idx2 == -1 {
		return nil, "", fmt.Errorf("version not found: %s", v2)
	}

	// Ensure idx1 < idx2
	direction := queen.DirectionUp
	if idx1 > idx2 {
		idx1, idx2 = idx2, idx1
		direction = queen.DirectionDown
	}

	// Extract migrations (exclusive of endpoints)
	migrations := make([]queen.MigrationStatus, 0)
	for i := idx1 + 1; i <= idx2; i++ {
		migrations = append(migrations, statuses[i])
	}

	return migrations, direction, nil
}

// outputDiffTable prints the diff in a formatted table.
func outputDiffTable(migrations []queen.MigrationStatus, v1, v2, direction string, showSQL bool) {
	arrow := "→"
	if direction == queen.DirectionDown {
		arrow = "←"
	}

	fmt.Printf("Difference: %s %s %s\n", v1, arrow, v2)
	fmt.Printf("Direction: %s\n", direction)
	fmt.Printf("Migrations: %d\n\n", len(migrations))

	// Determine column widths
	maxVersion := len("VERSION")
	maxName := len("NAME")
	for _, m := range migrations {
		if len(m.Version) > maxVersion {
			maxVersion = len(m.Version)
		}
		if len(m.Name) > maxName {
			maxName = len(m.Name)
		}
	}

	// Print header
	header := fmt.Sprintf("%-*s  %-*s  %s", maxVersion, "VERSION", maxName, "NAME", "STATUS")
	fmt.Println(header)
	fmt.Println(strings.Repeat("-", len(header)+10))

	// Print migrations
	for _, m := range migrations {
		status := m.Status.String()
		if m.Status == queen.StatusModified {
			status += " (WARNING: modified)"
		}
		if m.Destructive {
			status += " (WARNING: destructive)"
		}

		fmt.Printf("%-*s  %-*s  %s\n", maxVersion, m.Version, maxName, m.Name, status)

		// Show SQL if requested
		if showSQL {
			// Note: MigrationStatus doesn't have SQL, need to get it from Migration
			fmt.Println("    (SQL display requires access to Migration object)")
		}
	}

	// Summary
	fmt.Println()
	applied := 0
	pending := 0
	for _, m := range migrations {
		if m.Status == queen.StatusApplied {
			applied++
		} else {
			pending++
		}
	}

	fmt.Printf("Summary: %d applied, %d pending\n", applied, pending)

	if direction == queen.DirectionDown && applied > 0 {
		fmt.Println("\nNOTE: Going from current to target requires rollback")
	}
}

// outputDiffJSON prints the diff in JSON format.
func outputDiffJSON(migrations []queen.MigrationStatus, v1, v2, direction string) error {
	result := map[string]interface{}{
		"from":       v1,
		"to":         v2,
		"direction":  direction,
		"count":      len(migrations),
		"migrations": migrations,
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	fmt.Println(string(data))
	return nil
}
