package importcmd

import "fmt"

type gooseMigration struct {
	file    string
	version string
	name    string
	upSQL   string
	downSQL string
}

func importFromGoose(sourcePath, output string, dryRun bool) error {
	fmt.Println("Scanning goose migration files...")

	migrations, err := scanGooseMigrations(sourcePath, dryRun)
	if err != nil {
		return err
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

	if err := writeQueenMigrations(output, migrations); err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("Successfully imported %d migration(s) from goose\n", len(migrations))
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Review the generated files in", output+"/")
	fmt.Println("  2. Update import paths if needed")
	fmt.Println("  3. Build and run your migration CLI")

	return nil
}
