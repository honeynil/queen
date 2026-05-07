package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func confirm(message string) bool {
	fmt.Printf("%s (yes/no): ", message)

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.TrimSpace(strings.ToLower(response))
	return response == "yes" || response == "y"
}

func confirmExact(message, expected string) bool {
	fmt.Printf("%s\nType '%s' to confirm: ", message, expected)

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.TrimSpace(response)
	return response == expected
}

func (app *App) checkConfirmation(operation string) error {
	if !app.requiresConfirmation() {
		return nil
	}

	env := app.getEnvironmentName()
	message := fmt.Sprintf("WARNING: WARNING: You are about to %s on %s environment\nDatabase: %s",
		operation, strings.ToUpper(env), app.config.DSN)

	if env == "production" {
		if !confirmExact(message, "production") {
			return fmt.Errorf("operation canceled")
		}
	} else {
		if !confirm(message + "\nContinue?") {
			return fmt.Errorf("operation canceled")
		}
	}

	return nil
}
