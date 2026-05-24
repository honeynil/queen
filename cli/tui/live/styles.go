package live

import "github.com/charmbracelet/lipgloss"

var (
	titleStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#F8FAFC"))
	dimStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("#64748B"))
	mutedStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#94A3B8"))
	okStyle        = lipgloss.NewStyle().Foreground(lipgloss.Color("#10B981")).Bold(true)
	errStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("#EF4444")).Bold(true)
	warnStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("#F59E0B")).Bold(true)
	execStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("#38BDF8"))
	nplusStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#C084FC")).Bold(true)
	kindStyle      = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#E2E8F0"))
	selectedStyle  = lipgloss.NewStyle().Background(lipgloss.Color("#1F2937")).Foreground(lipgloss.Color("#F8FAFC")).Bold(true)
	markerStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#60A5FA")).Bold(true)
	panelStyle     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#334155")).Padding(0, 1)
	footerStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#94A3B8"))
	footerKeyStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#E2E8F0")).Bold(true)
)
