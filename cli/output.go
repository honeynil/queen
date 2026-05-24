package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func outputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func outputTable(headers ...string) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.Header(headers)
	return table
}

func outputRule(char string, width int) {
	fmt.Println(strings.Repeat(char, width))
}

func outputBlank() {
	fmt.Println()
}

func outputWarning(format string, args ...any) {
	fmt.Printf("WARNING: "+format+"\n", args...)
}
