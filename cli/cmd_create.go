package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
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

			filename := fmt.Sprintf("migrations/%s_%s.go", nextVersion, name)
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
			return "", fmt.Errorf("semver pattern requires manual version specification, use --version flag")
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

func migrationVariableName(version, name string) string {
	return fmt.Sprintf("Migration%s%s", version, toPascalCase(name))
}

func generateSQLTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")

	return fmt.Sprintf(`package migrations

import "github.com/honeynil/queen"

// %s %s
var %s = queen.M{
	Version: "%s",
	Name:    "%s",
	UpSQL: `+"`"+`
		-- Write your migration here
		-- Example: CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));
	`+"`"+`,
	DownSQL: `+"`"+`
		-- Write your rollback here
		-- Example: DROP TABLE users;
	`+"`"+`,
}
`, variableName, description, variableName, version, name)
}

func generateGoTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")
	upFuncName := fmt.Sprintf("up%s%s", version, toPascalCase(name))
	downFuncName := fmt.Sprintf("down%s%s", version, toPascalCase(name))

	return fmt.Sprintf(`package migrations

import (
	"context"
	"database/sql"

	"github.com/honeynil/queen"
)

// %s %s
var %s = queen.M{
	Version:        "%s",
	Name:           "%s",
	ManualChecksum: "v1", // Update this when you change the function
	UpFunc:         %s,
	DownFunc:       %s,
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your migration logic
	// Example:
	// rows, err := tx.QueryContext(ctx, "SELECT id, name FROM users")
	// if err != nil {
	//     return err
	// }
	// defer rows.Close()
	//
	// for rows.Next() {
	//     var id int
	//     var name string
	//     if err := rows.Scan(&id, &name); err != nil {
	//         return err
	//     }
	//     // Process data...
	// }
	//
	// return rows.Err()
	return nil
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your rollback logic
	return nil
}
`, variableName, description, variableName, version, name, upFuncName, downFuncName, upFuncName, downFuncName)
}

func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}
