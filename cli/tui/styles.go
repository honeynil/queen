// Package tui provides a terminal UI for Queen migrations.
package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Neutral terminal palette with amber reserved for warnings and pending work.
var (
	colorBg      = lipgloss.Color("#101113") // near-black
	colorPanelBg = lipgloss.Color("#181A1F") // panel background
	colorText    = lipgloss.Color("#E5E7EB") // off-white text
	colorMuted   = lipgloss.Color("#7C8491") // dim grey
	colorRed     = lipgloss.Color("#F87171") // error / destructive
	colorGreen   = lipgloss.Color("#22C55E") // applied
	colorAmber   = lipgloss.Color("#F59E0B") // pending / warning

	colorPrimary    = lipgloss.Color("#60A5FA") // primary UI chrome
	colorPrimaryDim = lipgloss.Color("#334155") // borders / dividers
	colorAccent     = lipgloss.Color("#38BDF8") // keys / secondary emphasis

	selectedBg = lipgloss.Color("#30343B")

	// Legacy names kept local so older TUI code remains readable while the
	// palette itself is no longer yellow-first.
	colorYellow     = colorPrimary
	colorYellowWarm = colorAccent
	colorYellowDim  = colorPrimaryDim

	// Header
	HeaderStyle = lipgloss.NewStyle().
			Foreground(colorText).
			Padding(0, 2)

	AppTitleStyle = lipgloss.NewStyle().
			Foreground(colorYellow).
			Bold(true)

	AppSubtitleStyle = lipgloss.NewStyle().
				Foreground(colorYellowDim)

	StatusInfoStyle = lipgloss.NewStyle().
			Foreground(colorYellowWarm)

	StatusPillStyle = lipgloss.NewStyle().
			Background(colorAmber).
			Foreground(colorBg).
			Bold(true).
			Padding(0, 1)

	StatusDarkPillStyle = lipgloss.NewStyle().
				Background(selectedBg).
				Foreground(colorYellow).
				Bold(true).
				Padding(0, 1)

	HelpHintStyle = lipgloss.NewStyle().
			Foreground(colorMuted)

	// Tab bar
	ActiveTabStyle = lipgloss.NewStyle().
			Background(colorPrimary).
			Foreground(colorBg).
			Bold(true).
			Padding(0, 1)

	InactiveTabStyle = lipgloss.NewStyle().
				Foreground(colorMuted).
				Padding(0, 1)

	TabBadgeStyle = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	// Footer
	FooterStyle = lipgloss.NewStyle().
			Foreground(colorYellowDim).
			Padding(0, 2)

	FooterKeyStyle = lipgloss.NewStyle().
			Foreground(colorYellowWarm).
			Bold(true)

	FooterDescStyle = lipgloss.NewStyle().
			Foreground(colorMuted)

	// Content
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorYellow).
			MarginBottom(1)

	AppliedStyle = lipgloss.NewStyle().
			Foreground(colorGreen).
			Bold(true)

	PendingStyle = lipgloss.NewStyle().
			Foreground(colorAmber).
			Bold(true)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	SelectedStyle = lipgloss.NewStyle().
			Background(selectedBg).
			Foreground(colorText).
			Bold(true)

	SelectedMarkerStyle = lipgloss.NewStyle().
				Foreground(colorAmber).
				Bold(true)

	NormalStyle = lipgloss.NewStyle()

	VersionStyle = lipgloss.NewStyle().
			Foreground(colorYellowWarm)

	NameStyle = lipgloss.NewStyle().
			Foreground(colorText)

	DetailStyle = lipgloss.NewStyle().
			Foreground(colorMuted)

	ProgressFilledStyle = lipgloss.NewStyle().
				Foreground(colorYellow)

	ProgressEmptyStyle = lipgloss.NewStyle().
				Foreground(colorYellowDim)

	ScrollIndicatorStyle = lipgloss.NewStyle().
				Foreground(colorMuted).
				Italic(true)

	SuccessMsgStyle = lipgloss.NewStyle().
			Foreground(colorGreen).
			Bold(true)

	WarningMsgStyle = lipgloss.NewStyle().
			Foreground(colorAmber)

	ErrorMsgStyle = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	InfoBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorYellowWarm).
			Padding(1, 3)

	DetailPanelStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorYellowDim).
				Padding(0, 1)

	PanelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorYellowDim).
			Foreground(colorText)

	PanelActiveStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorYellow).
				Foreground(colorText)

	PanelTitleStyle = lipgloss.NewStyle().
			Foreground(colorYellow).
			Bold(true).
			Padding(0, 1)

	PanelSubtitleStyle = lipgloss.NewStyle().
				Foreground(colorMuted)

	GapTypeBadgeStyle = lipgloss.NewStyle().
				Foreground(colorMuted)

	DestructiveBadgeStyle = lipgloss.NewStyle().
				Foreground(colorRed).
				Bold(true)

	RollbackBadgeStyle = lipgloss.NewStyle().
				Foreground(colorGreen)

	SeparatorStyle = lipgloss.NewStyle().
			Foreground(colorYellowDim)

	HelpKeyStyle = lipgloss.NewStyle().
			Foreground(colorYellowWarm).
			Bold(true)

	HelpDescStyle = lipgloss.NewStyle().
			Foreground(colorText)

	HelpSectionStyle = lipgloss.NewStyle().
				Foreground(colorYellow).
				Bold(true)
)

// UI symbols
const (
	iconCursor   = "> "
	iconSelected = "*"
	iconEmpty    = "o"
	keyDown      = "down"
	keyEnter     = "enter"
)

func separator(width int) string {
	if width <= 0 {
		return ""
	}
	return SeparatorStyle.Render(strings.Repeat("─", width))
}

// ANSI-shadow QUEEN banner for wide terminals.
var queenBannerLines = []string{
	` ██████╗ ██╗   ██╗███████╗███████╗███╗   ██╗`,
	`██╔═══██╗██║   ██║██╔════╝██╔════╝████╗  ██║`,
	`██║   ██║██║   ██║█████╗  █████╗  ██╔██╗ ██║`,
	`██║▄▄ ██║██║   ██║██╔══╝  ██╔══╝  ██║╚██╗██║`,
	`╚██████╔╝╚██████╔╝███████╗███████╗██║ ╚████║`,
	` ╚══▀▀═╝  ╚═════╝ ╚══════╝╚══════╝╚═╝  ╚═══╝`,
}

var bannerGradientStops = []struct{ r, g, b int }{
	{0x93, 0xC5, 0xFD}, // #93C5FD
	{0x60, 0xA5, 0xFA}, // #60A5FA
	{0x38, 0xBD, 0xF8}, // #38BDF8
}

func lerp(a, b int, t float64) int {
	return int(float64(a) + (float64(b)-float64(a))*t)
}

func gradientColorAt(t float64) lipgloss.Color {
	if t <= 0 {
		s := bannerGradientStops[0]
		return lipgloss.Color(fmt.Sprintf("#%02X%02X%02X", s.r, s.g, s.b))
	}
	if t >= 1 {
		s := bannerGradientStops[len(bannerGradientStops)-1]
		return lipgloss.Color(fmt.Sprintf("#%02X%02X%02X", s.r, s.g, s.b))
	}
	segments := float64(len(bannerGradientStops) - 1)
	pos := t * segments
	idx := int(pos)
	local := pos - float64(idx)
	a := bannerGradientStops[idx]
	b := bannerGradientStops[idx+1]
	return lipgloss.Color(fmt.Sprintf("#%02X%02X%02X",
		lerp(a.r, b.r, local), lerp(a.g, b.g, local), lerp(a.b, b.b, local)))
}

// RenderBanner returns the wide QUEEN banner.
func RenderBanner() string {
	width := 0
	for _, l := range queenBannerLines {
		if w := lipgloss.Width(l); w > width {
			width = w
		}
	}
	out := make([]string, 0, len(queenBannerLines))
	for _, line := range queenBannerLines {
		runes := []rune(line)
		var b strings.Builder
		for i, r := range runes {
			if r == ' ' {
				b.WriteRune(r)
				continue
			}
			t := 0.0
			if width > 1 {
				t = float64(i) / float64(width-1)
			}
			b.WriteString(lipgloss.NewStyle().Foreground(gradientColorAt(t)).Bold(true).Render(string(r)))
		}
		out = append(out, b.String())
	}
	return strings.Join(out, "\n")
}
