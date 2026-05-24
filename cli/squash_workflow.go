package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/yaop-labs/queen"
)

type squashOptions struct {
	versions []string
	into     string
	dryRun   bool
}

func (app *App) runSquash(q *queen.Queen, opts squashOptions) error {
	if len(opts.versions) < 2 {
		return fmt.Errorf("need at least 2 migrations to squash")
	}

	migrations, err := collectSquashMigrations(q, opts.versions)
	if err != nil {
		return err
	}

	upSQL, downSQL, err := combineSQLMigrations(migrations)
	if err != nil {
		return err
	}

	nextVersion, err := app.findNextVersion()
	if err != nil {
		return err
	}

	filename := migrationFilename(nextVersion, opts.into)
	variableName := migrationVariableName(nextVersion, opts.into)
	content := generateSquashedMigrationTemplate(nextVersion, opts.into, variableName, upSQL, downSQL)

	if opts.dryRun {
		fmt.Println("Dry run mode - no changes will be made")
		outputBlank()
	}

	fmt.Printf("Squashing %d migrations into '%s':\n", len(migrations), opts.into)
	for _, migration := range migrations {
		fmt.Printf("  • %s - %s\n", migration.Version, migration.Name)
	}
	outputBlank()
	fmt.Printf("Output file: %s\n", filename)

	if opts.dryRun {
		outputBlank()
		fmt.Println("Run without --dry-run to create the squashed migration file")
		return nil
	}

	if err := os.MkdirAll("migrations", 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("migration file already exists: %s", filename)
		}
		return fmt.Errorf("failed to create squashed migration file: %w", err)
	}
	defer func() { _ = file.Close() }()

	if _, err := file.WriteString(content); err != nil {
		return fmt.Errorf("failed to write squashed migration file: %w", err)
	}

	fmt.Printf("Created squashed migration file: %s\n", filename)
	fmt.Println("Review the generated migration before removing old files.")
	return nil
}

func resolveSquashVersions(ctx context.Context, q *queen.Queen, args []string, fromVer, toVer string) ([]string, error) {
	if len(args) > 0 {
		return parseSquashVersionArgs(args), nil
	}
	if fromVer == "" || toVer == "" {
		return nil, fmt.Errorf("provide either VERSIONS or use --from/--to flags")
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}

	versions := make([]string, 0)
	inRange := false
	for _, s := range statuses {
		if s.Version == fromVer {
			inRange = true
		}
		if inRange {
			versions = append(versions, s.Version)
		}
		if s.Version == toVer {
			break
		}
	}
	if len(versions) == 0 || versions[len(versions)-1] != toVer {
		return nil, fmt.Errorf("invalid squash range: %s..%s", fromVer, toVer)
	}
	return versions, nil
}

func parseSquashVersionArgs(args []string) []string {
	versions := make([]string, 0, len(args))
	for _, arg := range args {
		for _, part := range strings.Split(arg, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				versions = append(versions, part)
			}
		}
	}
	return versions
}

func collectSquashMigrations(q *queen.Queen, versions []string) ([]*queen.Migration, error) {
	migrations := make([]*queen.Migration, 0, len(versions))
	seen := make(map[string]bool, len(versions))
	for _, version := range versions {
		if seen[version] {
			continue
		}
		seen[version] = true

		migration := q.FindMigration(version)
		if migration == nil {
			return nil, fmt.Errorf("migration not registered: %s", version)
		}
		migrations = append(migrations, migration)
	}
	return migrations, nil
}

func combineSQLMigrations(migrations []*queen.Migration) (string, string, error) {
	var up strings.Builder
	var down strings.Builder

	for _, migration := range migrations {
		if migration.UpSQL == "" {
			return "", "", fmt.Errorf("migration %s is not a SQL migration and cannot be squashed", migration.Version)
		}
		writeSquashSQLBlock(&up, migration.Version, migration.Name, migration.UpSQL)
	}

	for i := len(migrations) - 1; i >= 0; i-- {
		migration := migrations[i]
		if migration.DownSQL == "" {
			return "", "", fmt.Errorf("migration %s has no SQL rollback and cannot be safely squashed", migration.Version)
		}
		writeSquashSQLBlock(&down, migration.Version, migration.Name, migration.DownSQL)
	}

	return strings.TrimSpace(up.String()), strings.TrimSpace(down.String()), nil
}

func writeSquashSQLBlock(b *strings.Builder, version, name, sql string) {
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	fmt.Fprintf(b, "-- %s %s\n", version, name)
	b.WriteString(strings.TrimSpace(sql))
}

func generateSquashedMigrationTemplate(version, name, variableName, upSQL, downSQL string) string {
	return fmt.Sprintf(`package migrations

import "github.com/yaop-labs/queen"

var %s = queen.M{
	Version: "%s",
	Name:    "%s",
	UpSQL: `+"`"+`
%s
	`+"`"+`,
	DownSQL: `+"`"+`
%s
	`+"`"+`,
}
`, variableName, version, name, indentSQL(upSQL), indentSQL(downSQL))
}

func indentSQL(sql string) string {
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	for i, line := range lines {
		lines[i] = "\t\t" + line
	}
	return strings.Join(lines, "\n")
}
