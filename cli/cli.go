// Package cli provides a command-line interface for Queen migrations.
package cli

// Run starts the CLI with the given migration registration function.
func Run(register RegisterFunc) {
	RunWithDB(register, nil)
}

// RunWithDB starts the CLI with a custom database opener.
func RunWithDB(register RegisterFunc, dbOpener DBOpener) {
	newApp(register, dbOpener).execute()
}
