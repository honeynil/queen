package tui

import (
	"strings"
	"sync"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/glamour/ansi"
)

var (
	glamourMu       sync.Mutex
	glamourRenderer *glamour.TermRenderer
	glamourWidth    int
)

func uintPtr(v uint) *uint { return &v }

// tuiStyleConfig returns a glamour StyleConfig tuned for Queen's TUI palette.
func tuiStyleConfig() ansi.StyleConfig {
	primary := "#60A5FA"
	accent := "#38BDF8"
	muted := "#7C8491"
	text := "#E5E7EB"
	amber := "#F59E0B"
	red := "#F87171"
	bold := true

	return ansi.StyleConfig{
		Document: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &text},
			Margin:         uintPtr(0),
		},
		BlockQuote: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &muted, Italic: &bold},
			Indent:         uintPtr(1),
		},
		Paragraph: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &text},
		},
		Heading: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &primary, Bold: &bold},
		},
		H1: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &primary, Bold: &bold},
		},
		H2: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &accent, Bold: &bold},
		},
		H3: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &accent},
		},
		Text: ansi.StylePrimitive{Color: &text},
		Strong: ansi.StylePrimitive{
			Bold:  &bold,
			Color: &primary,
		},
		Emph: ansi.StylePrimitive{Color: &accent, Italic: &bold},
		Code: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: &accent},
		},
		CodeBlock: ansi.StyleCodeBlock{
			StyleBlock: ansi.StyleBlock{
				StylePrimitive: ansi.StylePrimitive{Color: &text},
				Margin:         uintPtr(0),
			},
			Theme: "monokai",
			Chroma: &ansi.Chroma{
				Text:          ansi.StylePrimitive{Color: &text},
				Keyword:       ansi.StylePrimitive{Color: &primary, Bold: &bold},
				NameFunction:  ansi.StylePrimitive{Color: &accent},
				LiteralString: ansi.StylePrimitive{Color: &amber},
				Comment:       ansi.StylePrimitive{Color: &muted, Italic: &bold},
				Error:         ansi.StylePrimitive{Color: &red},
			},
		},
		List: ansi.StyleList{
			StyleBlock: ansi.StyleBlock{
				StylePrimitive: ansi.StylePrimitive{Color: &text},
			},
			LevelIndent: 2,
		},
		Item: ansi.StylePrimitive{Color: &text},
		Link: ansi.StylePrimitive{Color: &accent, Underline: &bold},
		HorizontalRule: ansi.StylePrimitive{
			Color:  &muted,
			Format: "─",
		},
	}
}

func getGlamourRenderer(width int) *glamour.TermRenderer {
	if width <= 0 {
		width = 80
	}
	glamourMu.Lock()
	defer glamourMu.Unlock()
	if glamourRenderer != nil && glamourWidth == width {
		return glamourRenderer
	}
	r, err := glamour.NewTermRenderer(
		glamour.WithStyles(tuiStyleConfig()),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		return nil
	}
	glamourRenderer = r
	glamourWidth = width
	return r
}

// renderMarkdown renders the given markdown with the TUI theme.
// Falls back to raw on failure.
func renderMarkdown(md string, width int) string {
	r := getGlamourRenderer(width)
	if r == nil {
		return md
	}
	out, err := r.Render(md)
	if err != nil {
		return md
	}
	return strings.TrimRight(out, "\n")
}

// renderSQLBlock wraps SQL in a fenced markdown code block and renders it.
func renderSQLBlock(sql string, width int) string {
	if strings.TrimSpace(sql) == "" {
		return ""
	}
	md := "```sql\n" + sql + "\n```"
	return renderMarkdown(md, width)
}
