package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/tap"
)

// initPanels constructs the bubbles widgets once. Sizes are placeholder until
// the first WindowSizeMsg arrives, after which resizePanels takes over.
func (m *Model) initPanels() {
	if m.panelsReady {
		return
	}

	tblStyles := table.DefaultStyles()
	tblStyles.Header = tblStyles.Header.
		Foreground(colorYellow).
		BorderForeground(colorYellowDim).
		Bold(true)
	tblStyles.Selected = tblStyles.Selected.
		Foreground(colorText).
		Background(selectedBg).
		Bold(true)
	tblStyles.Cell = tblStyles.Cell.Foreground(colorText)

	mt := table.New(
		table.WithColumns([]table.Column{
			{Title: "status", Width: 10},
			{Title: "version", Width: 10},
			{Title: "name", Width: 24},
			{Title: "flags", Width: 12},
		}),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	mt.SetStyles(tblStyles)

	gt := table.New(
		table.WithColumns([]table.Column{
			{Title: "", Width: 2},
			{Title: "severity", Width: 10},
			{Title: "type", Width: 14},
			{Title: "version", Width: 10},
			{Title: "name", Width: 20},
		}),
		table.WithFocused(false),
		table.WithHeight(10),
	)
	gt.SetStyles(tblStyles)

	dvp := viewport.New(40, 10)
	tvp := viewport.New(40, 8)

	m.migrationsTable = mt
	m.gapsTable = gt
	m.detailVP = dvp
	m.tapVP = tvp
	m.panelsReady = true
}

// usingThreePane reports whether the current geometry can host the layered layout.
func (m *Model) usingThreePane() bool {
	w := m.width
	h := m.height
	if w <= 0 {
		w = 80
	}
	if h <= 0 {
		h = 24
	}
	return w >= 80 && h >= 22
}

// effectiveSize returns sanitised width/height for layout math.
func (m *Model) effectiveSize() (int, int) {
	w := m.width
	h := m.height
	if w <= 0 {
		w = 80
	}
	if h <= 0 {
		h = 24
	}
	return w, h
}

// resizePanels recomputes panel widths/heights from m.width / m.height.
func (m *Model) resizePanels() {
	m.ensurePanels()
	w, h := m.effectiveSize()

	headerH := topBarHeight(w)
	tabsH := 1
	footerH := 1
	extras := 0
	if m.filterVisible() {
		extras++
	}
	if m.message != "" {
		extras++
	}
	bodyH := h - headerH - tabsH - footerH - extras
	if bodyH < 6 {
		bodyH = 6
	}

	if m.viewMode == ViewHelp {
		m.detailVP.Width = w - 2
		m.detailVP.Height = bodyH - 2
		m.tapVP.Width = w - 2
		m.tapVP.Height = bodyH - 2
		m.filterInput.Width = w - 24
		if m.filterInput.Width < 12 {
			m.filterInput.Width = 12
		}
		m.helpView.Width = w
		return
	}

	// inner = box - 2 per axis to leave room for the panel border.
	if m.usingThreePane() {
		leftW, _ := m.panelSplitWidths(w)
		if leftW < 28 {
			leftW = 28
		}
		if leftW > w-32 {
			leftW = w - 32
		}
		rightW := w - leftW
		detailH := bodyH * 55 / 100
		if m.usesSQLPreview(w) {
			detailH = bodyH * 46 / 100
		}
		if detailH < 6 {
			detailH = 6
		}
		tapH := bodyH - detailH
		if tapH < 4 {
			tapH = 4
			detailH = bodyH - tapH
		}
		if m.explainActive() {
			detailH = 0
			tapH = bodyH
		}

		m.migrationsTable.SetWidth(leftW - 2)
		m.migrationsTable.SetHeight(bodyH - 3) // 2 border + 1 header
		m.gapsTable.SetWidth(leftW - 2)
		m.gapsTable.SetHeight(bodyH - 3)
		m.resizeTableColumns(leftW - 4)

		detailVPW := rightW - 2
		if m.usesSQLPreview(w) && !m.explainActive() {
			detailVPW = rightW*44/100 - 2
		}
		m.detailVP.Width = detailVPW
		m.detailVP.Height = maxInt(1, detailH-2)
		m.tapVP.Width = rightW - 2
		m.tapVP.Height = tapH - 2
	} else {
		m.migrationsTable.SetWidth(w - 2)
		m.migrationsTable.SetHeight(bodyH - 3)
		m.gapsTable.SetWidth(w - 2)
		m.gapsTable.SetHeight(bodyH - 3)
		m.resizeTableColumns(w - 4)

		m.detailVP.Width = w - 2
		m.detailVP.Height = bodyH - 2
		m.tapVP.Width = w - 2
		m.tapVP.Height = bodyH - 2
	}

	m.filterInput.Width = w - 24
	if m.filterInput.Width < 12 {
		m.filterInput.Width = 12
	}
	m.helpView.Width = w
}

func (m *Model) panelSplitWidths(width int) (leftW, rightW int) {
	if m.viewMode == ViewMigrations {
		leftW = width * 52 / 100
	} else {
		leftW = width * 35 / 100
	}
	leftW = clamp(leftW, 28, maxInt(28, width-32))
	return leftW, width - leftW
}

func (m *Model) usesSQLPreview(width int) bool {
	return m.viewMode == ViewMigrations && width >= 118
}

func (m *Model) resizeTableColumns(inner int) {
	if inner < 24 {
		inner = 24
	}
	mCols := m.migrationsTable.Columns()
	if len(mCols) == 4 {
		status := 10
		version := 10
		flags := 12
		name := inner - status - version - flags - 3
		if name < 8 {
			name = 8
		}
		m.migrationsTable.SetColumns([]table.Column{
			{Title: "status", Width: status},
			{Title: "version", Width: version},
			{Title: "name", Width: name},
			{Title: "flags", Width: flags},
		})
	}
	gCols := m.gapsTable.Columns()
	if len(gCols) == 5 {
		marker := 2
		sev := 10
		typ := 14
		ver := 10
		name := inner - marker - sev - typ - ver - 4
		if name < 8 {
			name = 8
		}
		m.gapsTable.SetColumns([]table.Column{
			{Title: "", Width: marker},
			{Title: "severity", Width: sev},
			{Title: "type", Width: typ},
			{Title: "version", Width: ver},
			{Title: "name", Width: name},
		})
	}
}

// refreshMigrationRows syncs the migrations table rows from filtered indexes.
func (m *Model) refreshMigrationRows() {
	m.ensurePanels()
	indexes := m.filteredMigrationIndexes()
	rows := make([]table.Row, 0, len(indexes))
	for _, idx := range indexes {
		mig := m.migrations[idx]
		status := migrationStatusCell(mig.Status)
		flags := []string{}
		if mig.Destructive {
			flags = append(flags, "!dest")
		}
		if mig.HasRollback {
			flags = append(flags, "down")
		}
		rows = append(rows, table.Row{
			status,
			mig.Version,
			mig.Name,
			strings.Join(flags, " "),
		})
	}
	m.migrationsTable.SetRows(rows)
	if len(rows) == 0 {
		m.migrationsTable.SetCursor(0)
		m.cursor = 0
	} else {
		m.cursor = clamp(m.cursor, 0, len(rows)-1)
		m.migrationsTable.SetCursor(m.cursor)
	}
}

// refreshGapRows syncs the gaps table rows from filtered indexes.
func (m *Model) refreshGapRows() {
	m.ensurePanels()
	indexes := m.filteredGapIndexes()
	rows := make([]table.Row, 0, len(indexes))
	for pos, idx := range indexes {
		g := m.gaps[idx]
		sev := "warn"
		if g.Severity == "error" {
			sev = "error"
		}
		name := g.Name
		if name == "" {
			name = truncate(g.Description, 32)
		}
		marker := ""
		if pos == m.cursor {
			marker = iconCursor
		}
		rows = append(rows, table.Row{
			marker,
			sev,
			gapTypeLabel(g.Type),
			g.Version,
			name,
		})
	}
	m.gapsTable.SetRows(rows)
	if len(rows) == 0 {
		m.gapsTable.SetCursor(0)
		m.cursor = 0
	} else {
		m.cursor = clamp(m.cursor, 0, len(rows)-1)
		m.gapsTable.SetCursor(m.cursor)
	}
}

func (m *Model) refreshRows() {
	switch m.viewMode {
	case ViewGaps:
		m.refreshGapRows()
	default:
		m.refreshMigrationRows()
	}
}

func (m *Model) renderGapListContent(width, height int) string {
	if len(m.gaps) == 0 {
		return AppliedStyle.Render("No gaps detected.")
	}
	errors := 0
	for _, gap := range m.gaps {
		if strings.EqualFold(gap.Severity, "error") {
			errors++
		}
	}
	summary := DetailStyle.Render(fmt.Sprintf("total %d  |  errors %d  |  warnings %d", len(m.gaps), errors, len(m.gaps)-errors))
	if height <= 1 {
		return truncate(summary, width)
	}
	tableView := m.gapsTable.View()
	return strings.Join(fitBlock(summary+"\n"+tableView, width, height), "\n")
}

func (m *Model) renderGapsBody(width, height int) string {
	if width < 72 || height < 15 {
		return m.renderListPanel(width, height)
	}
	introH := 1
	gap := 1
	tableH := height * 42 / 100
	tableH = clamp(tableH, 7, maxInt(7, height-8))
	bottomH := height - introH - tableH - gap
	if bottomH < 6 {
		bottomH = 6
		tableH = height - introH - bottomH - gap
	}
	intro := padRight(DetailStyle.Render("Gaps are missing migration versions in the target."), width)
	table := m.renderPanel("gaps", m.renderGapTableContent(maxInt(1, width-2), maxInt(1, tableH-2)), width, tableH, m.focus == FocusList)
	leftW := width * 45 / 100
	rightW := width - leftW
	summary := m.renderPanel("summary", m.renderGapSummaryContent(), leftW, bottomH, false)
	detail := m.renderPanel("gap details", m.renderGapDetailContent(maxInt(1, rightW-2)), rightW, bottomH, m.focus == FocusDetail)
	bottom := lipgloss.JoinHorizontal(lipgloss.Top, summary, detail)
	return lipgloss.JoinVertical(lipgloss.Left, intro, table, strings.Repeat(" ", width), bottom)
}

func (m *Model) renderGapTableContent(width, height int) string {
	if len(m.gaps) == 0 {
		return AppliedStyle.Render("No gaps detected.")
	}
	markerW := 1
	verW := 7
	beforeW := 8
	afterW := 8
	countW := 5
	severityW := 9
	separatorW := 2 * 6
	recommendW := width - markerW - verW - beforeW - afterW - countW - severityW - separatorW
	if recommendW < 18 {
		recommendW = 18
	}
	row := func(marker, ver, before, after, count, severity, recommendation string) string {
		line := padRight(truncate(marker, markerW), markerW) +
			"  " + padRight(truncate(ver, verW), verW) +
			"  " + padRight(truncate(before, beforeW), beforeW) +
			"  " + padRight(truncate(after, afterW), afterW) +
			"  " + padRight(truncate(count, countW), countW) +
			"  " + padRight(truncate(severity, severityW), severityW) +
			"  " + padRight(truncate(recommendation, recommendW), recommendW)
		return truncate(line, width)
	}
	lines := []string{row("", HelpSectionStyle.Render("ver"), HelpSectionStyle.Render("before"), HelpSectionStyle.Render("after"), HelpSectionStyle.Render("count"), HelpSectionStyle.Render("severity"), HelpSectionStyle.Render("recommendation"))}
	indexes := m.filteredGapIndexes()
	cursor := clamp(m.cursor, 0, maxInt(0, len(indexes)-1))
	rowsVisible := maxInt(1, height-1)
	start := 0
	if len(indexes) > rowsVisible {
		start = clamp(cursor-rowsVisible/2, 0, len(indexes)-rowsVisible)
	}
	end := minInt(len(indexes), start+rowsVisible)
	for pos := start; pos < end; pos++ {
		gap := m.gaps[indexes[pos]]
		before, after := gapBounds(gap)
		marker := ""
		if pos == cursor {
			marker = SelectedMarkerStyle.Render(">")
		}
		line := row(marker, VersionStyle.Render(gap.Version), before, after, "1", gapSeverityBadge(gap.Severity), gapRecommendation(gap))
		if pos == cursor && width >= 70 {
			line = SelectedStyle.Render(padRight(truncate(line, width), width))
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (m *Model) renderGapSummaryContent() string {
	total := len(m.gaps)
	errors := 0
	versions := make([]string, 0, len(m.gaps))
	for _, gap := range m.gaps {
		versions = append(versions, gap.Version)
		if strings.EqualFold(gap.Severity, "error") {
			errors++
		}
	}
	status := "warn"
	statusStyle := WarningMsgStyle
	if errors > 0 {
		status = "error"
		statusStyle = ErrorStyle
	}
	var b strings.Builder
	row := func(k, v string) {
		b.WriteString(DetailStyle.Render(k + ": "))
		b.WriteString(v)
		b.WriteString("\n")
	}
	row("total gaps", fmt.Sprintf("%d", total))
	row("missing versions", "["+strings.Join(versions, ", ")+"]")
	row("blocked", fmt.Sprintf("%d", errors))
	row("status", statusStyle.Render(status))
	return strings.TrimRight(b.String(), "\n")
}

func gapBounds(g queen.Gap) (before, after string) {
	if g.Type == queen.GapTypeNumbering {
		parts := strings.Fields(g.Description)
		for i, part := range parts {
			if part == "between" && i+3 < len(parts) && parts[i+2] == "and" {
				return strings.Trim(parts[i+1], ","), strings.Trim(parts[i+3], ",")
			}
		}
	}
	if len(g.BlockedBy) > 0 {
		return g.Version, strings.Join(g.BlockedBy, ",")
	}
	return "-", "-"
}

func gapSeverityBadge(severity string) string {
	switch strings.ToLower(severity) {
	case "error":
		return ErrorStyle.Render("error")
	default:
		return WarningMsgStyle.Render("warn")
	}
}

func gapRecommendation(g queen.Gap) string {
	switch g.Type {
	case queen.GapTypeNumbering:
		return fmt.Sprintf("add missing migration %s or mark as resolved", g.Version)
	case queen.GapTypeApplication:
		return fmt.Sprintf("apply migration %s or mark as resolved", g.Version)
	case queen.GapTypeUnregistered:
		return fmt.Sprintf("register migration %s or mark as resolved", g.Version)
	default:
		return "review gap and mark as resolved"
	}
}

func (m *Model) renderMigrationListContent(width, height int) string {
	if width < 42 {
		width = 42
	}
	if height < 1 {
		height = 1
	}
	indexes := m.filteredMigrationIndexes()
	layout := migrationListWidths(width)

	header := migrationListRow(
		"",
		HelpSectionStyle.Render("status"),
		HelpSectionStyle.Render("ver"),
		HelpSectionStyle.Render("name"),
		HelpSectionStyle.Render("dest"),
		HelpSectionStyle.Render("rollback"),
		HelpSectionStyle.Render("risk"),
		HelpSectionStyle.Render("est"),
		HelpSectionStyle.Render("impact"),
		layout,
		width,
	)
	if len(indexes) == 0 {
		return header + "\n" + DetailStyle.Render("No migrations found.")
	}

	rowsVisible := maxInt(1, height-1)
	cursor := clamp(m.cursor, 0, len(indexes)-1)
	start := 0
	if len(indexes) > rowsVisible {
		start = clamp(cursor-rowsVisible/2, 0, len(indexes)-rowsVisible)
	}
	end := minInt(len(indexes), start+rowsVisible)

	lines := []string{header}
	for pos := start; pos < end; pos++ {
		mig := m.migrations[indexes[pos]]
		plan := m.migrationPlan(mig.Version)
		marker := ""
		if pos == cursor {
			marker = SelectedMarkerStyle.Render(">")
		}
		line := migrationListRow(
			marker,
			migrationStatusLabel(mig.Status),
			VersionStyle.Render(mig.Version),
			NameStyle.Render(truncate(mig.Name, layout.nameW)),
			migrationDestructiveLabel(mig),
			migrationRollbackLabel(mig),
			migrationRiskLabel(mig, plan),
			migrationEstimateLabel(mig, plan),
			migrationImpactLabel(mig, plan),
			layout,
			width,
		)
		if pos == cursor && width >= 54 {
			line = SelectedStyle.Render(padRight(truncate(line, width), width))
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

type migrationListLayout struct {
	markerW      int
	statusW      int
	versionW     int
	nameW        int
	destructiveW int
	rollbackW    int
	riskW        int
	estimateW    int
	impactW      int
	showRisk     bool
	showEstimate bool
	showImpact   bool
}

func migrationListWidths(width int) migrationListLayout {
	l := migrationListLayout{
		markerW:      1,
		statusW:      8,
		versionW:     5,
		destructiveW: 4,
		rollbackW:    8,
	}
	if width >= 58 {
		l.showRisk = true
		l.riskW = 6
	}
	if width >= 72 {
		l.showEstimate = true
		l.estimateW = 7
	}
	if width >= 86 {
		l.showImpact = true
		l.impactW = 12
	}
	separatorW := 1 + 2*4
	used := l.markerW + l.statusW + l.versionW + l.destructiveW + l.rollbackW + separatorW
	if l.showRisk {
		used += l.riskW + 2
	}
	if l.showEstimate {
		used += l.estimateW + 2
	}
	if l.showImpact {
		used += l.impactW + 2
	}
	l.nameW = width - used
	if l.nameW < 8 {
		l.statusW = 4
		l.nameW = width - used + 4
	}
	if l.nameW < 6 {
		l.nameW = 6
	}
	return l
}

func migrationListRow(marker, status, version, name, destructive, rollback, risk, estimate, impact string, l migrationListLayout, width int) string {
	line := padRight(truncate(marker, l.markerW), l.markerW) +
		" " + padRight(truncate(status, l.statusW), l.statusW) +
		"  " + padRight(truncate(version, l.versionW), l.versionW) +
		"  " + padRight(truncate(name, l.nameW), l.nameW) +
		"  " + padRight(truncate(destructive, l.destructiveW), l.destructiveW) +
		"  " + padRight(truncate(rollback, l.rollbackW), l.rollbackW)
	if l.showRisk {
		line += "  " + padRight(truncate(risk, l.riskW), l.riskW)
	}
	if l.showEstimate {
		line += "  " + padRight(truncate(estimate, l.estimateW), l.estimateW)
	}
	if l.showImpact {
		line += "  " + padRight(truncate(impact, l.impactW), l.impactW)
	}
	return truncate(line, width)
}

func migrationDestructiveLabel(mig queen.MigrationStatus) string {
	if mig.Destructive {
		return ErrorStyle.Render("yes")
	}
	return DetailStyle.Render("-")
}

func migrationRiskLabel(mig queen.MigrationStatus, plan *queen.MigrationPlan) string {
	risk := migrationRisk(mig, plan)
	switch risk {
	case "danger":
		return ErrorStyle.Render(risk)
	case "warn", "slow":
		return WarningMsgStyle.Render(risk)
	default:
		return AppliedStyle.Render(risk)
	}
}

func migrationRisk(mig queen.MigrationStatus, plan *queen.MigrationPlan) string {
	sql := ""
	if plan != nil {
		sql = strings.ToLower(plan.SQL)
	}
	name := strings.ToLower(mig.Name)
	switch {
	case mig.Destructive || strings.Contains(sql, "drop table") || strings.Contains(sql, "truncate"):
		return "danger"
	case strings.Contains(name, "backfill") || strings.Contains(name, "slow") || strings.Contains(sql, "create index"):
		return "slow"
	case strings.Contains(sql, "alter table") || strings.Contains(sql, "delete from") || strings.Contains(sql, "update "):
		return "warn"
	default:
		return "safe"
	}
}

func migrationEstimateLabel(mig queen.MigrationStatus, plan *queen.MigrationPlan) string {
	risk := migrationRisk(mig, plan)
	name := strings.ToLower(mig.Name)
	switch {
	case risk == "danger":
		return ErrorStyle.Render("?")
	case strings.Contains(name, "backfill") || strings.Contains(name, "slow"):
		return WarningMsgStyle.Render("10-30s")
	case risk == "slow":
		return WarningMsgStyle.Render("3-15s")
	case risk == "warn":
		return WarningMsgStyle.Render("1-2s")
	default:
		return DetailStyle.Render("<1s")
	}
}

func migrationImpactLabel(mig queen.MigrationStatus, plan *queen.MigrationPlan) string {
	if plan != nil {
		if table := firstSQLObject(plan.SQL); table != "" {
			return NameStyle.Render(table)
		}
	}
	parts := strings.Split(mig.Name, "_")
	for i, part := range parts {
		switch part {
		case "user", "users", "post", "posts", "audit", "comment", "comments":
			if part == "audit" && i+1 < len(parts) && parts[i+1] == "events" {
				return NameStyle.Render("audit_events")
			}
			return NameStyle.Render(part)
		}
	}
	return DetailStyle.Render("-")
}

func migrationTypeLabel(t queen.MigrationType) string {
	switch t {
	case queen.MigrationTypeSQL:
		return HelpKeyStyle.Render("sql")
	case queen.MigrationTypeGoFunc:
		return HelpKeyStyle.Render("go-func")
	case queen.MigrationTypeMixed:
		return HelpKeyStyle.Render("mixed")
	default:
		return DetailStyle.Render(string(t))
	}
}

func firstSQLObject(sql string) string {
	fields := strings.Fields(strings.NewReplacer("(", " ", ")", " ", ";", " ", ",", " ").Replace(strings.ToLower(sql)))
	for i, field := range fields {
		switch field {
		case "table", "into", "update", "from":
			if i+1 < len(fields) {
				return cleanSQLIdent(fields[i+1])
			}
		case "on":
			if i+1 < len(fields) {
				return cleanSQLIdent(fields[i+1])
			}
		}
	}
	return ""
}

func cleanSQLIdent(s string) string {
	s = strings.Trim(s, "`\"[]")
	if i := strings.LastIndex(s, "."); i >= 0 && i+1 < len(s) {
		s = s[i+1:]
	}
	return s
}

func migrationRollbackLabel(mig queen.MigrationStatus) string {
	if mig.HasRollback {
		return RollbackBadgeStyle.Render("<-")
	}
	return DetailStyle.Render("-")
}

// refreshDetail recomputes detail-panel content based on view mode + selection.
func (m *Model) refreshDetail() {
	m.ensurePanels()
	switch m.viewMode {
	case ViewMigrations:
		m.detailVP.SetContent(m.renderMigrationDetailContent())
	case ViewGaps:
		m.detailVP.SetContent(m.renderGapDetailContent(m.detailVP.Width))
	case ViewHelp:
		m.detailVP.SetContent(m.renderHelpContent())
	}
}

// refreshTap rebuilds the tap viewport content from m.tapEvents / explain state.
func (m *Model) refreshTap() {
	m.ensurePanels()
	m.tapVP.SetContent(m.renderTapContent(m.tapVP.Width))
	if m.tapFollow {
		m.tapVP.GotoBottom()
	}
}

// --- detail content renderers ---------------------------------------------

func (m *Model) renderMigrationDetailContent() string {
	mig, _, ok := m.selectedMigration()
	if !ok {
		return DetailStyle.Render("No migration selected.")
	}
	var b strings.Builder
	row := func(k, v string) {
		b.WriteString(DetailStyle.Render(k + ": "))
		b.WriteString(v)
		b.WriteString("\n")
	}
	row("version", VersionStyle.Render(mig.Version))
	row("name", NameStyle.Render(mig.Name))
	row("status", migrationStatusLabel(mig.Status))
	if plan := m.migrationPlan(mig.Version); plan != nil {
		row("type", migrationTypeLabel(plan.Type))
	}
	row("rollback", boolBadge(mig.HasRollback))
	row("destructive", boolBadge(mig.Destructive))
	if c := mig.Checksum; c != "" {
		row("checksum", truncate(c, 14))
	}
	if mig.AppliedAt != nil {
		row("applied", mig.AppliedAt.Format("2006-01-02 15:04:05"))
	}
	action := "enter applies this migration"
	if mig.Status == queen.StatusApplied {
		if mig.HasRollback {
			action = "enter rolls back from here"
		} else {
			action = WarningMsgStyle.Render("no down migration available")
		}
	}
	b.WriteString("\n")
	b.WriteString(DetailStyle.Render(action))
	b.WriteString("\n")

	return b.String()
}

func (m *Model) sqlPreviewPanelTitle() string {
	mig, _, ok := m.selectedMigration()
	if !ok {
		return "sql preview"
	}
	if plan := m.migrationPlan(mig.Version); plan != nil && plan.Type == queen.MigrationTypeGoFunc {
		return "go function (" + strings.ToLower(plan.Direction) + ")"
	} else if plan != nil && plan.Direction != "" {
		return "sql preview (" + strings.ToLower(plan.Direction) + ")"
	}
	return "sql preview"
}

func (m *Model) renderMigrationSQLPreviewContent(width int) string {
	mig, _, ok := m.selectedMigration()
	if !ok {
		return DetailStyle.Render("No migration selected.")
	}
	plan := m.migrationPlan(mig.Version)
	if plan == nil {
		return wrappedPreviewMessage(width,
			"Preview unavailable.",
			"Queen could not load an execution plan for this migration.",
		)
	}
	if strings.TrimSpace(plan.SQL) == "" {
		switch plan.Type {
		case queen.MigrationTypeGoFunc:
			return wrappedPreviewMessage(width,
				"Go function migration.",
				"Static SQL is not available because this migration is implemented as Go code.",
				"Run the migration with tap enabled to see every SQL statement it executes.",
			)
		case queen.MigrationTypeMixed:
			return wrappedPreviewMessage(width,
				"Mixed migration.",
				"This migration has Go code but no static SQL block for the selected direction.",
				"Executed SQL will appear in tap stream while it runs.",
			)
		default:
			return wrappedPreviewMessage(width, "No SQL preview for this migration.")
		}
	}
	if width < 8 {
		width = 8
	}
	return renderSQLBlock(plan.SQL, width)
}

func wrappedPreviewMessage(width int, lines ...string) string {
	if width < 8 {
		width = 8
	}
	var b strings.Builder
	wrap := lipgloss.NewStyle().Width(width).Foreground(colorMuted)
	for i, line := range lines {
		if i == 0 {
			b.WriteString(HelpSectionStyle.Render(line))
		} else {
			b.WriteString(wrap.Render(line))
		}
		if i < len(lines)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (m *Model) renderGapDetailContent(width int) string {
	gap, ok := m.selectedGap()
	if !ok {
		return DetailStyle.Render("No gap selected.")
	}
	if width < 8 {
		width = 8
	}
	var b strings.Builder
	row := func(k, v string) {
		b.WriteString(DetailStyle.Render(k + ": "))
		b.WriteString(v)
		b.WriteString("\n")
	}
	before, after := gapBounds(gap)
	row("missing version", VersionStyle.Render(gap.Version))
	if before != "-" {
		row("before", VersionStyle.Render(before))
	}
	if after != "-" {
		row("after", VersionStyle.Render(after))
	}
	row("type", gapTypeLabel(gap.Type))
	sevStyle := WarningMsgStyle
	if gap.Severity == "error" {
		sevStyle = ErrorMsgStyle
	}
	row("severity", sevStyle.Render(gap.Severity))
	row("impact", gapImpactLabel(gap))
	if gap.Name != "" {
		row("name", NameStyle.Render(gap.Name))
	}
	if gap.AppliedAt != nil {
		row("applied", *gap.AppliedAt)
	}
	if len(gap.BlockedBy) > 0 {
		row("blocked by", strings.Join(gap.BlockedBy, ", "))
	}
	b.WriteString("\n")
	b.WriteString(HelpSectionStyle.Render("description"))
	b.WriteString("\n")
	wrapped := lipgloss.NewStyle().Width(width).Foreground(colorText).Render(gap.Description)
	b.WriteString(wrapped)
	b.WriteString("\n\n")
	b.WriteString(DetailStyle.Render("enter/f fills · i ignores"))
	return b.String()
}

func gapImpactLabel(g queen.Gap) string {
	switch g.Type {
	case queen.GapTypeUnregistered:
		return ErrorStyle.Render("high")
	case queen.GapTypeApplication:
		return WarningMsgStyle.Render("medium")
	default:
		return WarningMsgStyle.Render("medium")
	}
}

func (m *Model) renderHelpContent() string {
	var b strings.Builder
	section := func(title string, pairs [][2]string) {
		b.WriteString(HelpSectionStyle.Render(title))
		b.WriteString("\n")
		for _, p := range pairs {
			b.WriteString("  ")
			b.WriteString(HelpKeyStyle.Render(padRight(p[0], 12)))
			b.WriteString("  ")
			b.WriteString(HelpDescStyle.Render(p[1]))
			b.WriteString("\n")
		}
	}
	section("Navigation", [][2]string{
		{"j/k ↑/↓", "Move cursor"},
		{"g / G", "Jump to top / bottom"},
		{"tab", "Cycle focus"},
	})
	b.WriteString("\n")
	section("Actions", [][2]string{
		{"enter", "Apply / rollback / fill"},
		{"u / d", "Apply up to / rollback from cursor"},
		{"f / i", "Fill or ignore selected gap"},
		{"r", "Refresh"},
	})
	b.WriteString("\n")
	section("Views", [][2]string{
		{"1", "Migrations"},
		{"2", "Gap detection"},
		{"3 / ?", "Help"},
	})
	b.WriteString("\n")
	section("Filter", [][2]string{
		{"/", "Edit filter"},
		{"esc", "Cancel or clear"},
	})
	section("Tap (right-bottom panel)", [][2]string{
		{"t", "Toggle tap panel"},
		{"x / X", "EXPLAIN / EXPLAIN ANALYZE"},
		{"esc", "Clear pinned EXPLAIN"},
	})
	return b.String()
}

func (m *Model) renderHelpBody(width, height int) string {
	if width < 72 {
		return m.renderPanel("help", m.detailVP.View(), width, height, m.focus == FocusDetail)
	}

	topH := height * 66 / 100
	topH = clamp(topH, 8, maxInt(8, height-5))
	riskH := height - topH
	if riskH < 5 {
		riskH = 5
		topH = height - riskH
	}

	leftW := width / 3
	midW := width / 3
	rightW := width - leftW - midW
	navigation := m.renderPanel("navigation", helpRows(leftW-2, [][2]string{
		{"tab", "switch focus"},
		{"j / k", "navigate"},
		{"g / G", "top / bottom"},
		{"/", "search / filter"},
		{"q", "quit / back"},
	}), leftW, topH, false)
	actions := m.renderPanel("actions", helpRows(midW-2, [][2]string{
		{"enter", "apply / rollback / fill"},
		{"u", "apply up to cursor"},
		{"d", "rollback from cursor"},
		{"f / i", "fill / ignore gap"},
		{"x", "EXPLAIN"},
		{"X", "EXPLAIN ANALYZE"},
		{"esc", "Clear pinned EXPLAIN"},
	}), midW, topH, false)
	statuses := m.renderPanel("statuses", helpStatusRows(rightW-2), rightW, topH, false)
	top := lipgloss.JoinHorizontal(lipgloss.Top, navigation, actions, statuses)
	risk := m.renderPanel("risk levels", helpRiskRows(width-2), width, riskH, false)
	return lipgloss.JoinVertical(lipgloss.Left, top, risk)
}

func helpRows(width int, pairs [][2]string) string {
	var b strings.Builder
	keyW := 10
	if width < 28 {
		keyW = 7
	}
	descW := maxInt(1, width-keyW-2)
	for _, p := range pairs {
		b.WriteString(HelpKeyStyle.Render(padRight(truncate(p[0], keyW), keyW)))
		b.WriteString("  ")
		b.WriteString(HelpDescStyle.Render(truncate(p[1], descW)))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func helpStatusRows(width int) string {
	rows := []struct {
		label string
		style lipgloss.Style
		desc  string
	}{
		{"applied", AppliedStyle, "successfully applied"},
		{"pending", PendingStyle, "not yet applied"},
		{"modified", lipgloss.NewStyle().Foreground(lipgloss.Color("#F472B6")).Bold(true), "checksum drift"},
		{"danger", ErrorStyle, "destructive change"},
		{"failed", ErrorStyle, "failed to apply"},
	}
	var b strings.Builder
	labelW := 10
	if width < 28 {
		labelW = 8
	}
	descW := maxInt(1, width-labelW-2)
	for _, row := range rows {
		b.WriteString(row.style.Render(padRight(truncate(row.label, labelW), labelW)))
		b.WriteString("  ")
		b.WriteString(HelpDescStyle.Render(truncate(row.desc, descW)))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func helpRiskRows(width int) string {
	badge := func(label string, bg lipgloss.Color) string {
		return lipgloss.NewStyle().
			Background(bg).
			Foreground(colorBg).
			Bold(true).
			Padding(0, 1).
			Render(label)
	}
	left := lipgloss.JoinVertical(lipgloss.Left,
		badge("safe", colorGreen)+DetailStyle.Render("  no destructive changes, low lock risk"),
		badge("warn", colorAmber)+DetailStyle.Render("  may lock tables or be long-running"),
	)
	right := lipgloss.JoinVertical(lipgloss.Left,
		badge("slow", lipgloss.Color("#A16207"))+DetailStyle.Render("  expected to take a long time"),
		badge("danger", colorRed)+DetailStyle.Render("  destructive or irreversible changes"),
	)
	if width < 72 {
		return left + "\n" + right
	}
	leftW := width / 2
	return lipgloss.JoinHorizontal(lipgloss.Top,
		lipgloss.NewStyle().Width(leftW).Render(left),
		lipgloss.NewStyle().Width(width-leftW).Render(right),
	)
}

// --- tap renderer ---------------------------------------------------------

func (m *Model) renderTapContent(width int) string {
	if width < 8 {
		width = 8
	}
	if m.explainActive() {
		return m.renderExplainContent(width)
	}
	var b strings.Builder
	if m.operationLabel != "" {
		b.WriteString(DetailStyle.Render("operation "))
		b.WriteString(HelpKeyStyle.Render(m.operationLabel))
		b.WriteString("\n")
	}
	if len(m.tapEvents) == 0 {
		b.WriteString("\n")
		b.WriteString(DetailStyle.Render("Waiting for SQL and transaction events..."))
		return b.String()
	}
	follow := "paused"
	followStyle := WarningMsgStyle
	if m.tapFollow {
		follow = "live"
		followStyle = AppliedStyle
	}
	b.WriteString(DetailStyle.Render(fmt.Sprintf("events %d  ", len(m.tapEvents))))
	b.WriteString(followStyle.Render(follow))
	b.WriteString("\n")
	b.WriteString(DetailStyle.Render("x explain · X analyze · esc clear pin"))
	b.WriteString("\n\n")
	for i, ev := range m.tapEvents {
		line := formatTapEventLine(ev, width)
		if i == m.tapCursor {
			line = SelectedMarkerStyle.Render("▸ ") + line
		} else {
			line = "  " + line
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func (m *Model) renderExplainContent(width int) string {
	var b strings.Builder
	if m.operationLabel != "" {
		b.WriteString(DetailStyle.Render("operation "))
		b.WriteString(HelpKeyStyle.Render(m.operationLabel))
		b.WriteString("\n")
	}
	if m.explainPinned {
		b.WriteString(SelectedMarkerStyle.Render("pinned " + tapExplainEventLabel(m.explainEvent)))
		b.WriteString("\n")
	}
	b.WriteString(DetailStyle.Render("esc back to tap stream"))
	b.WriteString("\n\n")
	if m.explainQuery != "" {
		b.WriteString(HelpSectionStyle.Render(m.explainTitle() + " SQL"))
		b.WriteString("\n  ")
		wrap := lipgloss.NewStyle().Width(width - 2).Render(singleLine(m.explainQuery))
		b.WriteString(strings.ReplaceAll(wrap, "\n", "\n  "))
		b.WriteString("\n\n")
	}
	if m.explainError != "" {
		b.WriteString(ErrorMsgStyle.Render("Explain Error"))
		b.WriteString("\n  ")
		b.WriteString(m.explainError)
		return b.String()
	}
	if m.explainResult == nil {
		b.WriteString(DetailStyle.Render("status: running " + m.explainTitle()))
		return b.String()
	}
	b.WriteString(HelpSectionStyle.Render(string(m.explainResult.Mode) + " — " + formatTapDuration(m.explainResult.Duration)))
	b.WriteString("\n")
	for _, line := range strings.Split(m.explainResult.Plan, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func (m *Model) explainTitle() string {
	if m.explainMode != "" {
		return string(m.explainMode)
	}
	if m.explainResult != nil {
		return string(m.explainResult.Mode)
	}
	return string(tap.ExplainOnly)
}

func formatTapEventLine(ev tap.Event, width int) string {
	var op, body string
	switch ev.Kind {
	case tap.KindStart:
		op = "▶ start"
		body = migrationTapOp(ev) + " " + ev.Name
	case tap.KindEnd:
		if ev.Error != "" {
			op = "✗ end"
			body = ev.Error
		} else {
			op = "✓ end"
			body = "ok"
		}
	case tap.KindTxBegin:
		op = "↳ Begin"
		body = "begin"
	case tap.KindTxCommit:
		op = "✓ Commit"
		body = "commit"
	case tap.KindTxRollback:
		op = "✗ Rollback"
		body = ev.Error
	case tap.KindExec:
		op = "• Execute"
		body = singleLine(ev.BoundSQL)
		if body == "" {
			body = singleLine(ev.SQL)
		}
		flags := []string{}
		if ev.Slow {
			flags = append(flags, "SLOW")
		}
		if ev.NPlus1 {
			flags = append(flags, fmt.Sprintf("N+1/%d", ev.NPlus1Count))
		}
		if ev.Error != "" {
			flags = append(flags, "ERR")
		}
		if len(flags) > 0 {
			body += " [" + strings.Join(flags, " ") + "]"
		}
	default:
		op = string(ev.Kind)
		body = ""
	}
	dur := ""
	if ev.Duration > 0 {
		dur = " " + formatTapDuration(ev.Duration.Round(time.Microsecond))
	}
	style := tapOpStyleFor(ev)
	prefix := style.Render(op) + DetailStyle.Render(dur)
	remaining := width - lipgloss.Width(prefix) - 1
	if remaining < 6 {
		remaining = 6
	}
	return prefix + " " + NameStyle.Render(truncateMiddle(body, remaining))
}

func tapOpStyleFor(ev tap.Event) lipgloss.Style {
	switch ev.Kind {
	case tap.KindStart:
		return HelpKeyStyle
	case tap.KindEnd:
		if ev.Error != "" {
			return ErrorStyle
		}
		return AppliedStyle
	case tap.KindTxRollback:
		return ErrorStyle
	case tap.KindTxCommit:
		return AppliedStyle
	case tap.KindTxBegin:
		return VersionStyle
	case tap.KindExec:
		if ev.Error != "" {
			return ErrorStyle
		}
		return HelpSectionStyle
	default:
		return DetailStyle
	}
}

func migrationTapOp(ev tap.Event) string {
	if ev.Version == "" {
		return "Migration"
	}
	return "Mig " + ev.Version
}

func tapExplainEventLabel(ev tap.Event) string {
	if ev.Version == "" && ev.Name == "" {
		return "SQL event"
	}
	if ev.Name == "" {
		return ev.Version
	}
	if ev.Version == "" {
		return ev.Name
	}
	return ev.Version + " " + ev.Name
}

// --- small helpers --------------------------------------------------------

func (m *Model) explainActive() bool {
	return m.explainPinned || m.explainResult != nil || m.explainError != "" || m.explainQuery != ""
}

func migrationStatusBadge(s queen.Status) string {
	switch s {
	case queen.StatusApplied:
		return AppliedStyle.Render("applied")
	case queen.StatusModified:
		return WarningMsgStyle.Render("modified")
	default:
		return PendingStyle.Render("pending")
	}
}

func migrationStatusCell(s queen.Status) string {
	switch s {
	case queen.StatusApplied:
		return "applied"
	case queen.StatusModified:
		return "modified"
	default:
		return "pending"
	}
}

func migrationStatusLabel(s queen.Status) string {
	return migrationStatusBadge(s)
}

func gapTypeLabel(t queen.GapType) string {
	switch t {
	case queen.GapTypeNumbering:
		return "numbering"
	case queen.GapTypeApplication:
		return "application"
	case queen.GapTypeUnregistered:
		return "unregistered"
	default:
		return string(t)
	}
}

func boolBadge(b bool) string {
	if b {
		return AppliedStyle.Render("yes")
	}
	return DetailStyle.Render("no")
}

func (m *Model) migrationCounts() (applied, pending, modified, destructive int) {
	for _, mig := range m.migrations {
		switch mig.Status {
		case queen.StatusApplied:
			applied++
		case queen.StatusModified:
			modified++
		default:
			pending++
		}
		if mig.Destructive {
			destructive++
		}
	}
	return applied, pending, modified, destructive
}
