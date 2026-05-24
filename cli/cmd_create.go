package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) createCmd() *cobra.Command {
	var migrationType string

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			if !queen.IsValidMigrationName(name) {
				return fmt.Errorf("invalid migration name: must contain only lowercase letters, numbers, and underscores")
			}

			if err := app.loadConfigFile(); err != nil && !os.IsNotExist(err) {
				if err.Error() != "config file not found: .queen.yaml (use --use-config only when config file exists)" {
					return fmt.Errorf("failed to load config: %w", err)
				}
			}

			nextVersion, err := app.findNextVersion()
			if err != nil {
				return err
			}

			filename := migrationFilename(nextVersion, name)
			variableName := migrationVariableName(nextVersion, name)

			var content string
			switch migrationType {
			case "sql":
				content = generateSQLTemplate(nextVersion, name, variableName)
			case "go":
				content = generateGoTemplate(nextVersion, name, variableName)
			default:
				return fmt.Errorf("invalid migration type: %s (must be 'sql' or 'go')", migrationType)
			}

			if err := os.MkdirAll("migrations", 0755); err != nil {
				return fmt.Errorf("failed to create migrations directory: %w", err)
			}

			if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
				return fmt.Errorf("failed to create migration file: %w", err)
			}

			fmt.Printf("Created migration file: %s\n\n", filename)
			fmt.Println("Next steps:")
			fmt.Printf("1. Edit %s and add your migration logic\n", filename)
			fmt.Println("2. Add this line to migrations/register.go:")
			fmt.Printf("\n   q.MustAdd(%s)\n\n", variableName)

			return nil
		},
	}

	cmd.Flags().StringVar(&migrationType, "type", "sql", "Migration type: sql or go")

	return cmd
}

func (app *App) findNextVersion() (string, error) {
	namingConfig := app.getNamingConfig()

	if namingConfig == nil {
		namingConfig = &queen.NamingConfig{
			Pattern: queen.NamingPatternSequentialPadded,
			Padding: 3,
			Enforce: true,
		}
	}

	existingVersions, err := app.getExistingVersions()
	if err != nil {
		return "", err
	}

	if len(existingVersions) == 0 {
		switch namingConfig.Pattern {
		case queen.NamingPatternSequential:
			return "1", nil
		case queen.NamingPatternSequentialPadded:
			padding := namingConfig.Padding
			if padding <= 0 {
				padding = 3
			}
			return fmt.Sprintf("%0*d", padding, 1), nil
		case queen.NamingPatternSemver:
			return "", fmt.Errorf("semver pattern requires a manual version; create does not support semver auto-versioning yet")
		default:
			return "", fmt.Errorf("unknown naming pattern: %s", namingConfig.Pattern)
		}
	}

	return namingConfig.FindNextVersion(existingVersions)
}

func (app *App) getExistingVersions() ([]string, error) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	versions := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}

		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			continue
		}

		versions = append(versions, parts[0])
	}

	return versions, nil
}
