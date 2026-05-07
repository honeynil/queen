package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func (app *App) importCmd() *cobra.Command {
	var (
		fromTool string
		dryRun   bool
		output   string
	)

	cmd := &cobra.Command{
		Use:   "import PATH",
		Short: "Import migrations from other tools",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourcePath := args[0]

			if fromTool == "" {
				return fmt.Errorf("--from flag is required (goose)")
			}

			if output == "" {
				output = "migrations"
			}

			if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
				return fmt.Errorf("source path does not exist: %s", sourcePath)
			}

			if dryRun {
				fmt.Println("Dry run mode - no changes will be made")
				fmt.Println()
			}

			fmt.Printf("Importing migrations from %s\n", fromTool)
			fmt.Printf("Source: %s\n", sourcePath)
			fmt.Printf("Output: %s\n", output)
			fmt.Println()

			switch fromTool {
			case "goose":
				return importFromGoose(sourcePath, output, dryRun)
			default:
				return fmt.Errorf("unsupported tool: %s (supported: goose)", fromTool)
			}
		},
	}

	cmd.Flags().StringVar(&fromTool, "from", "", "Migration tool to import from (goose)")
	cmd.Flags().StringVar(&output, "output", "", "Output directory for Queen migrations (default: migrations)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview import without making changes")

	return cmd
}

func importFromGoose(sourcePath, output string, dryRun bool) error {
	fmt.Println("Scanning goose migration files...")

	files, err := filepath.Glob(filepath.Join(sourcePath, "*.sql"))
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no goose migration files found in %s", sourcePath)
	}

	fmt.Printf("Found %d migration file(s)\n\n", len(files))

	type gooseMigration struct {
		file    string
		version string
		name    string
		upSQL   string
		downSQL string
	}

	migrations := make([]gooseMigration, 0)

	for _, file := range files {
		basename := filepath.Base(file)

		parts := strings.SplitN(basename, "_", 2)
		if len(parts) < 2 {
			fmt.Printf("  WARNING: Skipping %s (invalid format)\n", basename)
			continue
		}

		version := parts[0]
		namePart := strings.TrimSuffix(parts[1], ".sql")

		content, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("  WARNING: Skipping %s (failed to read)\n", basename)
			continue
		}

		upSQL, downSQL := parseGooseSQL(string(content))

		migrations = append(migrations, gooseMigration{
			file:    file,
			version: version,
			name:    namePart,
			upSQL:   upSQL,
			downSQL: downSQL,
		})

		fmt.Printf("  %s\n", basename)
		if dryRun {
			fmt.Printf("    Version: %s, Name: %s\n", version, namePart)
		}
	}

	if len(migrations) == 0 {
		return fmt.Errorf("no valid goose migrations found")
	}

	if dryRun {
		fmt.Println()
		fmt.Printf("Would create %d Queen migration file(s) in %s/\n", len(migrations), output)
		fmt.Println()
		fmt.Println("Run without --dry-run to execute conversion")
		return nil
	}

	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	fmt.Println()
	fmt.Println("Converting to Queen format...")

	registrationCalls := make([]string, 0)
	for i, m := range migrations {
		queenVersion := fmt.Sprintf("%03d", i+1)
		filename := fmt.Sprintf("%s_%s.go", queenVersion, m.name)
		funcName := "Register" + queenVersion
		capitalizeNext := true
		for _, r := range m.name {
			if r == '_' || r == '-' {
				capitalizeNext = true
				continue
			}
			if capitalizeNext && r >= 'a' && r <= 'z' {
				funcName += string(r - 32) // to uppercase
				capitalizeNext = false
			} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				funcName += string(r)
				capitalizeNext = false
			}
		}

		goContent := generateQueenMigrationFile(queenVersion, m.name, funcName, m.upSQL, m.downSQL)
		outputFile := filepath.Join(output, filename)

		if err := os.WriteFile(outputFile, []byte(goContent), 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", filename, err)
		}

		fmt.Printf("  Created %s\n", filename)
		registrationCalls = append(registrationCalls, fmt.Sprintf("\t%s(q)", funcName))
	}

	registrationFile := filepath.Join(output, "migrations.go")
	registrationContent := generateRegistrationFile(strings.Join(registrationCalls, "\n"))

	if err := os.WriteFile(registrationFile, []byte(registrationContent), 0644); err != nil {
		return fmt.Errorf("failed to write migrations.go: %w", err)
	}

	fmt.Printf("  Created migrations.go\n")
	fmt.Println()
	fmt.Printf("Successfully imported %d migration(s) from goose\n", len(migrations))
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Review the generated files in", output+"/")
	fmt.Println("  2. Update import paths if needed")
	fmt.Println("  3. Build and run your migration CLI")

	return nil
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

func generateQueenMigrationFile(version, name, funcName, upSQL, downSQL string) string {
	upSQL = strings.ReplaceAll(upSQL, "`", "` + \"`\" + `")
	downSQL = strings.ReplaceAll(downSQL, "`", "` + \"`\" + `")

	return fmt.Sprintf(`package migrations

import (
	"github.com/honeynil/queen"
)

// %s registers the %s migration.
func %s(q *queen.Queen) {
	q.MustAdd(queen.M{
		Version: "%s",
		Name:    "%s",
		UpSQL: `+"`"+`
%s
		`+"`"+`,
		DownSQL: `+"`"+`
%s
		`+"`"+`,
	})
}
`, funcName, name, funcName, version, name, upSQL, downSQL)
}

func generateRegistrationFile(registrationCalls string) string {
	return fmt.Sprintf(`package migrations

import (
	"github.com/honeynil/queen"
)

// Register registers all migrations with Queen.
func Register(q *queen.Queen) {
%s
}
`, registrationCalls)
}
