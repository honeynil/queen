package cli

import (
	"fmt"
)

func outputDoctorTable(results []DoctorResult) error {
	fmt.Println("Queen Migration Health Check")
	outputRule("=", 50)
	outputBlank()

	passedCount := 0
	warningCount := 0
	failedCount := 0

	for _, result := range results {
		var icon string
		switch result.Status {
		case statusPass:
			icon = "✓"
			passedCount++
		case statusWarning:
			icon = "⚠"
			warningCount++
		case statusFail:
			icon = "✗"
			failedCount++
		}

		fmt.Printf("%s %s\n", icon, result.Check)
		fmt.Printf("  %s\n", result.Message)
		if result.Details != "" {
			fmt.Printf("  %s\n", result.Details)
		}
		outputBlank()
	}

	outputRule("=", 50)
	fmt.Printf("Summary: %d passed, %d warnings, %d failed\n", passedCount, warningCount, failedCount)

	if failedCount > 0 {
		outputBlank()
		outputWarning("Some checks failed. Review the issues above.")
		return fmt.Errorf("health check failed")
	}

	if warningCount > 0 {
		outputBlank()
		outputWarning("Some checks have warnings. Review recommended.")
	} else {
		fmt.Println("\nAll checks passed!")
	}

	return nil
}

func outputDoctorJSON(results []DoctorResult) error {
	return outputJSON(results)
}
