package cli

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

var passwordKeyValueRE = regexp.MustCompile(`(?i)(password|pwd)=([^;\s]+)`)

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
	message := fmt.Sprintf("WARNING: You are about to %s on %s environment\nDatabase: %s",
		operation, strings.ToUpper(env), redactDSN(app.config.DSN))

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

func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}

	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" && u.User != nil {
		username := u.User.Username()
		if username == "" {
			u.User = url.UserPassword("redacted", "redacted")
		} else if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(username, "redacted")
		}
		return u.String()
	}

	if at := strings.Index(dsn, "@"); at > 0 {
		prefix := dsn[:at]
		if colon := strings.Index(prefix, ":"); colon >= 0 {
			return prefix[:colon+1] + "redacted" + dsn[at:]
		}
	}

	return passwordKeyValueRE.ReplaceAllString(dsn, "$1=redacted")
}
