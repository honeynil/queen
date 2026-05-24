package cli

import (
	"fmt"

	"github.com/yaop-labs/queen"
)

func outputDiffTable(migrations []queen.MigrationStatus, v1, v2, direction string, showSQL bool) {
	arrow := "→"
	if direction == queen.DirectionDown {
		arrow = "←"
	}

	fmt.Printf("Difference: %s %s %s\n", v1, arrow, v2)
	fmt.Printf("Direction: %s\n", direction)
	fmt.Printf("Migrations: %d\n\n", len(migrations))

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

	header := fmt.Sprintf("%-*s  %-*s  %s", maxVersion, "VERSION", maxName, "NAME", "STATUS")
	fmt.Println(header)
	outputRule("-", len(header)+10)

	for _, m := range migrations {
		status := m.Status.String()
		if m.Status == queen.StatusModified {
			status += " (WARNING: modified)"
		}
		if m.Destructive {
			status += " (WARNING: destructive)"
		}

		fmt.Printf("%-*s  %-*s  %s\n", maxVersion, m.Version, maxName, m.Name, status)

		if showSQL {
			fmt.Println("    (SQL display requires access to Migration object)")
		}
	}

	outputBlank()
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

func outputDiffJSON(migrations []queen.MigrationStatus, v1, v2, direction string) error {
	result := map[string]interface{}{
		"from":       v1,
		"to":         v2,
		"direction":  direction,
		"count":      len(migrations),
		"migrations": migrations,
	}

	return outputJSON(result)
}
