// Package tui provides a terminal UI for Queen migrations.
package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	// Color palette — softer, Tailwind-inspired
	primaryColor   = lipgloss.Color("#8B5CF6")
	accentColor    = lipgloss.Color("#A78BFA")
	successColor   = lipgloss.Color("#10B981")
	warningColor   = lipgloss.Color("#F59E0B")
	errorColor     = lipgloss.Color("#EF4444")
	dimColor       = lipgloss.Color("#6B7280")
	subtleBg       = lipgloss.Color("#1E1E2E")
	borderColor    = lipgloss.Color("#4B5563")
	textColor      = lipgloss.Color("#F9FAFB")
	mutedTextColor = lipgloss.Color("#9CA3AF")
	selectedBg     = lipgloss.Color("#2D2B55")

	// Header
	HeaderStyle = lipgloss.NewStyle().
			Background(subtleBg).
			Foreground(textColor).
			Padding(0, 2)

	AppTitleStyle = lipgloss.NewStyle().
			Foreground(primaryColor).
			Bold(true)

	AppSubtitleStyle = lipgloss.NewStyle().
				Foreground(mutedTextColor)

	// Tab bar
	ActiveTabStyle = lipgloss.NewStyle().
			Background(primaryColor).
			Foreground(textColor).
			Bold(true).
			Padding(0, 1)

	InactiveTabStyle = lipgloss.NewStyle().
				Foreground(mutedTextColor).
				Padding(0, 1)

	TabBadgeStyle = lipgloss.NewStyle().
			Foreground(warningColor).
			Bold(true)

	// Footer
	FooterStyle = lipgloss.NewStyle().
			Background(subtleBg).
			Foreground(dimColor).
			Padding(0, 2)

	FooterKeyStyle = lipgloss.NewStyle().
			Foreground(accentColor).
			Bold(true)

	FooterDescStyle = lipgloss.NewStyle().
			Foreground(dimColor)

	// Content styles
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			MarginBottom(1)

	AppliedStyle = lipgloss.NewStyle().
			Foreground(successColor).
			Bold(true)

	PendingStyle = lipgloss.NewStyle().
			Foreground(warningColor).
			Bold(true)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(errorColor).
			Bold(true)

	// List items
	SelectedStyle = lipgloss.NewStyle().
			Background(selectedBg).
			Bold(true)

	NormalStyle = lipgloss.NewStyle()

	VersionStyle = lipgloss.NewStyle().
			Foreground(accentColor)

	NameStyle = lipgloss.NewStyle().
			Foreground(mutedTextColor)

	DetailStyle = lipgloss.NewStyle().
			Foreground(dimColor)

	// Progress bar
	ProgressFilledStyle = lipgloss.NewStyle().
				Foreground(successColor)

	ProgressEmptyStyle = lipgloss.NewStyle().
				Foreground(borderColor)

	// Scroll indicators
	ScrollIndicatorStyle = lipgloss.NewStyle().
				Foreground(dimColor).
				Italic(true)

	// Messages
	SuccessMsgStyle = lipgloss.NewStyle().
			Foreground(successColor).
			Bold(true)

	WarningMsgStyle = lipgloss.NewStyle().
			Foreground(warningColor)

	ErrorMsgStyle = lipgloss.NewStyle().
			Foreground(errorColor).
			Bold(true)

	// Info box (used in help view)
	InfoBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Padding(1, 3)

	// Gap type badges
	GapTypeBadgeStyle = lipgloss.NewStyle().
				Foreground(dimColor)

	// Destructive badge
	DestructiveBadgeStyle = lipgloss.NewStyle().
				Foreground(errorColor).
				Bold(true)

	// Rollback badge
	RollbackBadgeStyle = lipgloss.NewStyle().
				Foreground(successColor)

	// Separator
	SeparatorStyle = lipgloss.NewStyle().
			Foreground(borderColor)

	// Help key style
	HelpKeyStyle = lipgloss.NewStyle().
			Foreground(accentColor).
			Bold(true)

	HelpDescStyle = lipgloss.NewStyle().
			Foreground(mutedTextColor)

	HelpSectionStyle = lipgloss.NewStyle().
				Foreground(primaryColor).
				Bold(true)
)

// UI symbols
const (
	iconCursor   = "❯ "
	iconSelected = "●"
	iconEmpty    = "○"
	keyDown      = "down"
	keyEnter     = "enter"
)

func separator(width int) string {
	return SeparatorStyle.Render(strings.Repeat("─", width))
}
