package importcmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func writeQueenMigrations(output string, migrations []gooseMigration) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	fmt.Println()
	fmt.Println("Converting to Queen format...")

	if err := checkOutputConflicts(output, migrations); err != nil {
		return err
	}

	registrationCalls := make([]string, 0, len(migrations))
	for _, m := range migrations {
		call, err := writeQueenMigration(output, m)
		if err != nil {
			return err
		}
		registrationCalls = append(registrationCalls, call)
	}

	registrationFile := filepath.Join(output, "migrations.go")
	registrationContent := generateRegistrationFile(strings.Join(registrationCalls, "\n"))

	if err := writeFileExclusive(registrationFile, []byte(registrationContent)); err != nil {
		return fmt.Errorf("failed to write migrations.go: %w", err)
	}

	fmt.Printf("  Created migrations.go\n")
	return nil
}

func writeQueenMigration(output string, migration gooseMigration) (string, error) {
	queenVersion := migration.version
	filename := fmt.Sprintf("%s_%s.go", queenVersion, migration.name)
	funcName := gooseRegisterFuncName(queenVersion, migration.name)

	goContent := generateQueenMigrationFile(queenVersion, migration.name, funcName, migration.upSQL, migration.downSQL)
	outputFile := filepath.Join(output, filename)

	if err := writeFileExclusive(outputFile, []byte(goContent)); err != nil {
		return "", fmt.Errorf("failed to write %s: %w", filename, err)
	}

	fmt.Printf("  Created %s\n", filename)
	return fmt.Sprintf("\t%s(q)", funcName), nil
}

func checkOutputConflicts(output string, migrations []gooseMigration) error {
	seen := map[string]struct{}{
		filepath.Join(output, "migrations.go"): {},
	}

	if err := ensureDoesNotExist(filepath.Join(output, "migrations.go")); err != nil {
		return err
	}

	for _, migration := range migrations {
		filename := fmt.Sprintf("%s_%s.go", migration.version, migration.name)
		path := filepath.Join(output, filename)

		if _, exists := seen[path]; exists {
			return fmt.Errorf("duplicate generated output path: %s", path)
		}
		seen[path] = struct{}{}

		if err := ensureDoesNotExist(path); err != nil {
			return err
		}
	}

	return nil
}

func ensureDoesNotExist(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("file already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check output file %s: %w", path, err)
	}
	return nil
}

func writeFileExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("file already exists: %s", path)
		}
		return err
	}
	defer func() { _ = file.Close() }()

	n, err := file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short write: wrote %d of %d bytes", n, len(data))
	}
	return nil
}
