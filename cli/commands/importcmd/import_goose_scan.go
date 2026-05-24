package importcmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func scanGooseMigrations(sourcePath string, dryRun bool) ([]gooseMigration, error) {
	files, err := filepath.Glob(filepath.Join(sourcePath, "*.sql"))
	if err != nil {
		return nil, fmt.Errorf("failed to scan files: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no goose migration files found in %s", sourcePath)
	}

	fmt.Printf("Found %d migration file(s)\n\n", len(files))

	migrations := make([]gooseMigration, 0, len(files))
	for _, file := range files {
		migration, ok := readGooseMigration(file)
		if !ok {
			continue
		}

		migrations = append(migrations, migration)

		basename := filepath.Base(file)
		fmt.Printf("  %s\n", basename)
		if dryRun {
			fmt.Printf("    Version: %s, Name: %s\n", migration.version, migration.name)
		}
	}

	return migrations, nil
}

func readGooseMigration(file string) (gooseMigration, bool) {
	basename := filepath.Base(file)

	version, name, ok := parseGooseFilename(basename)
	if !ok {
		fmt.Printf("  WARNING: Skipping %s (invalid format)\n", basename)
		return gooseMigration{}, false
	}

	content, err := os.ReadFile(file)
	if err != nil {
		fmt.Printf("  WARNING: Skipping %s (failed to read)\n", basename)
		return gooseMigration{}, false
	}

	upSQL, downSQL := parseGooseSQL(string(content))
	return gooseMigration{
		file:    file,
		version: version,
		name:    name,
		upSQL:   upSQL,
		downSQL: downSQL,
	}, true
}

func parseGooseFilename(basename string) (version, name string, ok bool) {
	parts := strings.SplitN(basename, "_", 2)
	if len(parts) < 2 {
		return "", "", false
	}
	return parts[0], strings.TrimSuffix(parts[1], ".sql"), true
}

func parseGooseSQL(content string) (upSQL, downSQL string) {
	lines := strings.Split(content, "\n")
	inUp := false
	inDown := false
	upLines := make([]string, 0)
	downLines := make([]string, 0)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "-- +goose Up") {
			inUp = true
			inDown = false
			continue
		}

		if strings.HasPrefix(trimmed, "-- +goose Down") {
			inUp = false
			inDown = true
			continue
		}

		if inUp {
			upLines = append(upLines, line)
		} else if inDown {
			downLines = append(downLines, line)
		}
	}

	upSQL = strings.TrimSpace(strings.Join(upLines, "\n"))
	downSQL = strings.TrimSpace(strings.Join(downLines, "\n"))
	return
}
