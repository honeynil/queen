package tui

import (
	"github.com/charmbracelet/bubbles/textinput"
)

type initStep int

const (
	stepDriver initStep = iota
	stepMigrationsDir
	stepConfig
	stepConfirm
)

type InitResult struct {
	Driver        string
	MigrationsDir string
	WithConfig    bool
	Confirmed     bool
}

type driverOption struct {
	name string
	desc string
}

type InitModel struct {
	step           initStep
	cursor         int
	drivers        []driverOption
	selectedDriver int
	dirInput       textinput.Model
	withConfig     bool
	width          int
	height         int
	quitting       bool
	result         *InitResult
}

func NewInitModel() *InitModel {
	ti := textinput.New()
	ti.Placeholder = "migrations"
	ti.SetValue("migrations")
	ti.CharLimit = 64
	ti.Width = 40

	return &InitModel{
		step:   stepDriver,
		cursor: 0,
		drivers: []driverOption{
			{"postgres", "PostgreSQL (recommended)"},
			{"mysql", "MySQL / MariaDB"},
			{"sqlite", "SQLite (file-based)"},
			{"clickhouse", "ClickHouse (analytics)"},
			{"cockroachdb", "CockroachDB (distributed SQL)"},
			{"mssql", "Microsoft SQL Server"},
		},
		selectedDriver: 0,
		dirInput:       ti,
		withConfig:     true,
		result:         nil,
	}
}

func (m *InitModel) Result() *InitResult {
	return m.result
}
