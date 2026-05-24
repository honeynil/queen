package cli

import (
	"fmt"

	"github.com/yaop-labs/queen"
)

func outputLogTable(applied []queen.Applied, withDuration, withMeta bool) {
	maxVersion := len("VERSION")
	maxName := len("NAME")
	for _, a := range applied {
		if len(a.Version) > maxVersion {
			maxVersion = len(a.Version)
		}
		if len(a.Name) > maxName {
			maxName = len(a.Name)
		}
	}

	header := fmt.Sprintf("%-*s  %-*s  %s", maxVersion, "VERSION", maxName, "NAME", "APPLIED AT")
	if withDuration {
		header += "  DURATION"
	}
	if withMeta {
		header += "  APPLIED BY  HOSTNAME  ENV"
	}
	fmt.Println(header)
	outputRule("-", len(header)+20)

	for _, a := range applied {
		appliedAt := a.AppliedAt.Format("2006-01-02 15:04:05")
		row := fmt.Sprintf("%-*s  %-*s  %s", maxVersion, a.Version, maxName, a.Name, appliedAt)

		if withDuration {
			if a.DurationMS > 0 {
				row += fmt.Sprintf("  %dms", a.DurationMS)
			} else {
				row += "  -"
			}
		}

		if withMeta {
			appliedBy := a.AppliedBy
			if appliedBy == "" {
				appliedBy = "-"
			}
			hostname := a.Hostname
			if hostname == "" {
				hostname = "-"
			}
			env := a.Environment
			if env == "" {
				env = "-"
			}
			row += fmt.Sprintf("  %s  %s  %s", appliedBy, hostname, env)
		}

		fmt.Println(row)
	}

	fmt.Printf("\nTotal: %d migrations\n", len(applied))
}

func outputLogJSON(applied []queen.Applied) error {
	return outputJSON(applied)
}
