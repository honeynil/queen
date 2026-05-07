package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

const (
	statusPass    = "pass"
	statusWarning = "warning"
	statusFail    = "fail"
)

// DoctorResult represents a health check result.
type DoctorResult struct {
	Check    string `json:"check"`
	Status   string `json:"status"` // "statusPass", "statusWarning", "statusFail"
	Message  string `json:"message"`
	Details  string `json:"details,omitempty"`
	Severity string `json:"severity,omitempty"`
}

type doctorOptions struct {
	deep bool
	gaps bool
	fix  bool
}

func (app *App) doctorCmd() *cobra.Command {
	var (
		deep bool
		gaps bool
		fix  bool
	)

	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run migration health checks and diagnostics",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			results := collectDoctorResults(ctx, q, doctorOptions{
				deep: deep,
				gaps: gaps,
				fix:  fix,
			})

			// Output results
			if app.config.JSON {
				return outputDoctorJSON(results)
			}

			return outputDoctorTable(results)
		},
	}

	cmd.Flags().BoolVar(&deep, "deep", false, "Deep schema validation (slower)")
	cmd.Flags().BoolVar(&gaps, "gaps", false, "Only check for gaps")
	cmd.Flags().BoolVar(&fix, "fix", false, "Attempt to auto-fix issues")

	return cmd
}

func collectDoctorResults(ctx context.Context, q *queen.Queen, opts doctorOptions) []DoctorResult {
	results := make([]DoctorResult, 0)

	if opts.gaps {
		results = append(results, checkGaps(ctx, q))
		if opts.fix {
			results = append(results, attemptAutoFix(ctx, q, results)...)
		}
		return results
	}

	// Run health checks
	results = append(results, checkDatabaseConnection(ctx, q))
	results = append(results, checkMigrationTable(ctx, q))
	results = append(results, checkChecksums(ctx, q))

	if !opts.deep {
		results = append(results, checkGaps(ctx, q))
	}

	results = append(results, checkRegistrationSync(ctx, q))

	if opts.deep {
		results = append(results, checkSchemaConsistency(ctx, q))
	}

	if opts.fix {
		results = append(results, attemptAutoFix(ctx, q, results)...)
	}

	return results
}

// checkDatabaseConnection verifies database connectivity.
func checkDatabaseConnection(ctx context.Context, q *queen.Queen) DoctorResult {
	// Try to load applied migrations as a connectivity test
	_, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Database Connection",
			Status:  statusFail,
			Message: "Failed to connect to database",
			Details: err.Error(),
		}
	}

	return DoctorResult{
		Check:   "Database Connection",
		Status:  statusPass,
		Message: "Database is accessible",
	}
}

// checkMigrationTable verifies the migration table exists and is accessible.
func checkMigrationTable(ctx context.Context, q *queen.Queen) DoctorResult {
	applied, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Migration Table",
			Status:  statusFail,
			Message: "Migration table is not accessible",
			Details: err.Error(),
		}
	}

	return DoctorResult{
		Check:   "Migration Table",
		Status:  statusPass,
		Message: fmt.Sprintf("Migration table exists with %d records", len(applied)),
	}
}

// checkChecksums validates that applied migrations haven't been modified.
func checkChecksums(ctx context.Context, q *queen.Queen) DoctorResult {
	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Checksum Validation",
			Status:  statusFail,
			Message: "Failed to validate checksums",
			Details: err.Error(),
		}
	}

	modified := make([]string, 0)
	for _, s := range statuses {
		if s.Status == queen.StatusModified {
			modified = append(modified, s.Version)
		}
	}

	if len(modified) > 0 {
		return DoctorResult{
			Check:    "Checksum Validation",
			Status:   statusFail,
			Message:  fmt.Sprintf("%d migration(s) have been modified after being applied", len(modified)),
			Details:  fmt.Sprintf("Modified versions: %s", strings.Join(modified, ", ")),
			Severity: "error",
		}
	}

	return DoctorResult{
		Check:   "Checksum Validation",
		Status:  statusPass,
		Message: "All applied migrations match their checksums",
	}
}

// checkGaps checks for migration gaps.
func checkGaps(ctx context.Context, q *queen.Queen) DoctorResult {
	gaps, err := q.DetectGaps(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Gap Detection",
			Status:  statusFail,
			Message: "Failed to detect gaps",
			Details: err.Error(),
		}
	}

	if len(gaps) == 0 {
		return DoctorResult{
			Check:   "Gap Detection",
			Status:  statusPass,
			Message: "No gaps detected",
		}
	}

	// Count by severity
	errors := 0
	statusWarnings := 0
	for _, gap := range gaps {
		if gap.Severity == "error" {
			errors++
		} else {
			statusWarnings++
		}
	}

	status := statusWarning
	if errors > 0 {
		status = statusFail
	}

	return DoctorResult{
		Check:   "Gap Detection",
		Status:  status,
		Message: fmt.Sprintf("Found %d gap(s): %d errors, %d statusWarnings", len(gaps), errors, statusWarnings),
		Details: "Run 'queen gap detect' for details",
	}
}

// checkRegistrationSync checks if code and database are in sync.
func checkRegistrationSync(ctx context.Context, q *queen.Queen) DoctorResult {
	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Registration Sync",
			Status:  statusFail,
			Message: "Failed to check registration sync",
			Details: err.Error(),
		}
	}

	applied, err := q.Driver().GetApplied(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Registration Sync",
			Status:  statusFail,
			Message: "Failed to get applied migrations",
			Details: err.Error(),
		}
	}

	registered := make(map[string]bool)
	for _, s := range statuses {
		registered[s.Version] = true
	}

	unregistered := make([]string, 0)
	for _, a := range applied {
		if !registered[a.Version] {
			unregistered = append(unregistered, a.Version)
		}
	}

	if len(unregistered) > 0 {
		return DoctorResult{
			Check:    "Registration Sync",
			Status:   statusFail,
			Message:  fmt.Sprintf("%d applied migration(s) are not registered in code", len(unregistered)),
			Details:  fmt.Sprintf("Unregistered: %s", strings.Join(unregistered, ", ")),
			Severity: "error",
		}
	}

	return DoctorResult{
		Check:   "Registration Sync",
		Status:  statusPass,
		Message: "All applied migrations are registered in code",
	}
}

// checkSchemaConsistency performs deep schema validation by analyzing SQL from all migrations.
func checkSchemaConsistency(ctx context.Context, q *queen.Queen) DoctorResult {
	upPlans, err := q.DryRun(ctx, queen.DirectionUp, 0)
	if err != nil {
		// If no pending migrations, that's fine — still analyze down plans
		upPlans = nil
	}

	downPlans, err := q.DryRun(ctx, queen.DirectionDown, 0)
	if err != nil {
		downPlans = nil
	}

	// Also get statuses to check all migrations
	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Schema Consistency",
			Status:  statusFail,
			Message: "Failed to get migration statuses",
			Details: err.Error(),
		}
	}

	var issues []string

	// Collect all SQL by version (up direction)
	upTables := make(map[string]string)   // table -> version that creates it
	downTables := make(map[string]string) // table -> version that drops it

	// Analyze up SQL from pending migrations
	for _, plan := range upPlans {
		analyzeSQL(plan.SQL, plan.Version, true, upTables, &issues)
	}

	// Analyze down SQL from applied migrations
	for _, plan := range downPlans {
		analyzeSQL(plan.SQL, plan.Version, false, downTables, &issues)
	}

	// Check for migrations without rollback
	noRollbackCount := 0
	for _, s := range statuses {
		if !s.HasRollback {
			noRollbackCount++
		}
	}
	if noRollbackCount > 0 {
		issues = append(issues, fmt.Sprintf("%d migration(s) have no rollback defined", noRollbackCount))
	}

	// Check for duplicate table creations in up SQL
	tableCreators := make(map[string][]string) // table -> list of versions
	for _, plan := range upPlans {
		for _, table := range extractTables(plan.SQL, "CREATE TABLE") {
			tableCreators[table] = append(tableCreators[table], plan.Version)
		}
	}
	for table, versions := range tableCreators {
		if len(versions) > 1 {
			issues = append(issues, fmt.Sprintf("Table %q created in multiple migrations: %s", table, strings.Join(versions, ", ")))
		}
	}

	if len(issues) == 0 {
		return DoctorResult{
			Check:   "Schema Consistency",
			Status:  statusPass,
			Message: "No schema consistency issues found",
		}
	}

	status := statusWarning
	return DoctorResult{
		Check:   "Schema Consistency",
		Status:  status,
		Message: fmt.Sprintf("Found %d schema consistency issue(s)", len(issues)),
		Details: strings.Join(issues, "\n"),
	}
}

// analyzeSQL checks a single migration's SQL for common issues.
func analyzeSQL(sql string, version string, isUp bool, tables map[string]string, issues *[]string) {
	if sql == "" {
		return
	}

	upper := strings.ToUpper(sql)

	if isUp {
		destructive := []string{"DROP TABLE", "DROP DATABASE", "DROP SCHEMA", "TRUNCATE"}
		for _, keyword := range destructive {
			if strings.Contains(upper, keyword) {
				*issues = append(*issues, fmt.Sprintf("Migration %s: destructive operation %q in up SQL", version, keyword))
			}
		}
	}

	// Track table operations
	for _, table := range extractTables(sql, "CREATE TABLE") {
		if existing, ok := tables[table]; ok && isUp {
			*issues = append(*issues, fmt.Sprintf("Migration %s: table %q already created in %s", version, table, existing))
		}
		tables[table] = version
	}
}

// extractTables extracts table names from SQL for a given operation (e.g. "CREATE TABLE").
func extractTables(sql string, operation string) []string {
	upper := strings.ToUpper(sql)
	op := strings.ToUpper(operation)
	var tables []string

	for {
		idx := strings.Index(upper, op)
		if idx == -1 {
			break
		}

		rest := strings.TrimSpace(sql[idx+len(op):])
		upper = upper[idx+len(op):]

		// Skip "IF NOT EXISTS" / "IF EXISTS"
		restUpper := strings.ToUpper(rest)
		if strings.HasPrefix(restUpper, "IF ") {
			spaceIdx := strings.IndexAny(rest[3:], " \t\n")
			if spaceIdx != -1 {
				// skip "IF NOT EXISTS" or "IF EXISTS"
				afterIf := strings.TrimSpace(rest[3+spaceIdx:])
				if strings.HasPrefix(strings.ToUpper(afterIf), "EXISTS") {
					spaceIdx2 := strings.IndexAny(afterIf[6:], " \t\n")
					if spaceIdx2 != -1 {
						rest = strings.TrimSpace(afterIf[6+spaceIdx2:])
					}
				} else {
					rest = afterIf
				}
			}
		}

		// Extract table name (handle schema.table and quoted names)
		tableName := extractIdentifier(rest)
		if tableName != "" {
			tables = append(tables, strings.ToLower(tableName))
		}
	}

	return tables
}

// extractIdentifier extracts a SQL identifier (table name) from the beginning of a string.
func extractIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	var name strings.Builder
	for _, r := range s {
		if r == '(' || r == ' ' || r == '\t' || r == '\n' || r == ';' {
			break
		}
		name.WriteRune(r)
	}

	result := name.String()
	// Remove surrounding quotes
	result = strings.Trim(result, "\"'`")
	return result
}

// attemptAutoFix provides actionable recommendations for detected issues.
func attemptAutoFix(_ context.Context, _ *queen.Queen, results []DoctorResult) []DoctorResult {
	fixes := make([]DoctorResult, 0)

	for _, result := range results {
		if result.Status != statusFail && result.Status != statusWarning {
			continue
		}

		switch result.Check {
		case "Gap Detection":
			commands := make([]string, 0, 2)
			commands = append(commands, "queen gap detect    # see gap details")
			commands = append(commands, "queen gap fill      # fill detected gaps")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Gaps",
				Status:  statusWarning,
				Message: "Run the following commands to resolve gaps",
				Details: strings.Join(commands, "\n"),
			})

		case "Checksum Validation":
			versions := extractVersionsFromDetails(result.Details)
			var commands []string
			if len(versions) > 0 {
				for _, v := range versions {
					commands = append(commands, fmt.Sprintf("queen baseline --version %s    # accept current version of migration %s", v, v))
				}
			}
			commands = append(commands, "# WARNING: Only use baseline if you are sure the migration code matches the applied schema")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Checksums",
				Status:  statusWarning,
				Message: "Checksum mismatches detected — migrations were modified after being applied",
				Details: strings.Join(commands, "\n"),
			})

		case "Registration Sync":
			versions := extractVersionsFromDetails(result.Details)
			var commands []string
			if len(versions) > 0 {
				for _, v := range versions {
					commands = append(commands, fmt.Sprintf("queen baseline --version %s    # register migration %s as applied", v, v))
				}
			} else {
				commands = append(commands, "queen status    # review current state")
			}
			commands = append(commands, "# Or add the missing migration code to your application")
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Registration",
				Status:  statusWarning,
				Message: "Applied migrations not found in code — add them or mark as baseline",
				Details: strings.Join(commands, "\n"),
			})

		case "Schema Consistency":
			fixes = append(fixes, DoctorResult{
				Check:   "Fix: Schema",
				Status:  statusWarning,
				Message: "Review the schema issues above and fix the migration SQL manually",
				Details: "queen explain <version>    # inspect a specific migration",
			})
		}
	}

	if len(fixes) == 0 {
		fixes = append(fixes, DoctorResult{
			Check:   "Auto-Fix",
			Status:  statusPass,
			Message: "No issues require fixing",
		})
	}

	return fixes
}

// extractVersionsFromDetails parses version numbers from doctor result details.
func extractVersionsFromDetails(details string) []string {
	// Details format examples:
	// "Modified versions: 001, 002, 003"
	// "Unregistered: 001, 002"
	parts := strings.SplitN(details, ": ", 2)
	if len(parts) < 2 {
		return nil
	}

	versionStr := strings.TrimSpace(parts[1])
	if versionStr == "" {
		return nil
	}

	versions := strings.Split(versionStr, ", ")
	result := make([]string, 0, len(versions))
	for _, v := range versions {
		v = strings.TrimSpace(v)
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}

// outputDoctorTable prints doctor results in a formatted table.
func outputDoctorTable(results []DoctorResult) error {
	fmt.Println("Queen Migration Health Check")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	statusPassed := 0
	statusWarnings := 0
	statusFailed := 0

	for _, result := range results {
		var icon string
		switch result.Status {
		case statusPass:
			icon = "✓"
			statusPassed++
		case statusWarning:
			icon = "⚠"
			statusWarnings++
		case statusFail:
			icon = "✗"
			statusFailed++
		}

		fmt.Printf("%s %s\n", icon, result.Check)
		fmt.Printf("  %s\n", result.Message)
		if result.Details != "" {
			fmt.Printf("  %s\n", result.Details)
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Summary: %d statusPassed, %d statusWarnings, %d statusFailed\n", statusPassed, statusWarnings, statusFailed)

	if statusFailed > 0 {
		fmt.Println("\nWARNING: Some checks statusFailed. Review the issues above.")
		return fmt.Errorf("health check statusFailed")
	}

	if statusWarnings > 0 {
		fmt.Println("\nWARNING: Some checks have statusWarnings. Review recommended.")
	} else {
		fmt.Println("\nAll checks statusPassed!")
	}

	return nil
}

// outputDoctorJSON prints doctor results in JSON format.
func outputDoctorJSON(results []DoctorResult) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("statusFailed to marshal JSON: %w", err)
	}
	fmt.Println(string(data))
	return nil
}
