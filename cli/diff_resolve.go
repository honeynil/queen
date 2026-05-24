package cli

import (
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

// resolveVersion resolves special version keywords to actual versions.
func resolveVersion(version string, statuses []queen.MigrationStatus) (string, error) {
	switch version {
	case "current":
		for i := len(statuses) - 1; i >= 0; i-- {
			if statuses[i].Status == queen.StatusApplied {
				return statuses[i].Version, nil
			}
		}
		return "", fmt.Errorf("no migrations applied yet")

	case "latest":
		if len(statuses) > 0 {
			return statuses[len(statuses)-1].Version, nil
		}
		return "", fmt.Errorf("no migrations registered")

	default:
		return version, nil
	}
}

// resolveRelativeVersion resolves relative versions like +3 or -2.
func resolveRelativeVersion(baseVersion string, relative string, statuses []queen.MigrationStatus) (string, error) {
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

	direction := queen.DirectionUp
	if idx1 > idx2 {
		idx1, idx2 = idx2, idx1
		direction = queen.DirectionDown
	}

	migrations := make([]queen.MigrationStatus, 0)
	for i := idx1 + 1; i <= idx2; i++ {
		migrations = append(migrations, statuses[i])
	}

	return migrations, direction, nil
}
