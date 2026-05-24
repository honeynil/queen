package tui

import "github.com/charmbracelet/bubbles/key"

type keyMap struct {
	Up        key.Binding
	Down      key.Binding
	Top       key.Binding
	Bottom    key.Binding
	Tab       key.Binding
	ShiftTab  key.Binding
	Migrate   key.Binding
	Gaps      key.Binding
	Help      key.Binding
	Enter     key.Binding
	UpTo      key.Binding
	DownFrom  key.Binding
	Fill      key.Binding
	Ignore    key.Binding
	Explain   key.Binding
	Analyze   key.Binding
	Filter    key.Binding
	Refresh   key.Binding
	ToggleTap key.Binding
	Esc       key.Binding
	Quit      key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		Up:        key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "up")),
		Down:      key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "down")),
		Top:       key.NewBinding(key.WithKeys("g"), key.WithHelp("g", "top")),
		Bottom:    key.NewBinding(key.WithKeys("G"), key.WithHelp("G", "bottom")),
		Tab:       key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "focus")),
		ShiftTab:  key.NewBinding(key.WithKeys("shift+tab"), key.WithHelp("⇧tab", "back focus")),
		Migrate:   key.NewBinding(key.WithKeys("1"), key.WithHelp("1", "migrations")),
		Gaps:      key.NewBinding(key.WithKeys("2"), key.WithHelp("2", "gaps")),
		Help:      key.NewBinding(key.WithKeys("3", "?"), key.WithHelp("3/?", "help")),
		Enter:     key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "apply/fill")),
		UpTo:      key.NewBinding(key.WithKeys("u"), key.WithHelp("u", "up to")),
		DownFrom:  key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "rollback")),
		Fill:      key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "fill")),
		Ignore:    key.NewBinding(key.WithKeys("i"), key.WithHelp("i", "ignore")),
		Explain:   key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "explain")),
		Analyze:   key.NewBinding(key.WithKeys("X"), key.WithHelp("X", "analyze")),
		Filter:    key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
		Refresh:   key.NewBinding(key.WithKeys("r", "R"), key.WithHelp("r", "refresh")),
		ToggleTap: key.NewBinding(key.WithKeys("t"), key.WithHelp("t", "tap")),
		Esc:       key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back")),
		Quit:      key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

// ShortHelp returns the compact footer help bindings.
func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Tab, k.ToggleTap, k.Explain, k.Analyze, k.Down, k.Enter, k.Filter, k.Refresh, k.Help, k.Quit}
}

// FullHelp returns grouped help bindings.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.Top, k.Bottom, k.Tab, k.ShiftTab},
		{k.Migrate, k.Gaps, k.Help, k.Refresh, k.Quit},
		{k.Enter, k.UpTo, k.DownFrom, k.Fill, k.Ignore},
		{k.Filter, k.Esc, k.Explain, k.Analyze, k.ToggleTap},
	}
}
