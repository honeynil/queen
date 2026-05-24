package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/yaop-labs/queen"
	"gopkg.in/yaml.v3"
)

// Config holds all configuration options for the CLI.
type Config struct {
	Driver      string        `yaml:"driver"`
	DSN         string        `yaml:"dsn"`
	Table       string        `yaml:"table"`
	LockTimeout time.Duration `yaml:"lock_timeout"`

	UseConfig        bool   `yaml:"-"`
	Env              string `yaml:"-"`
	UnlockProduction bool   `yaml:"-"`
	Yes              bool   `yaml:"-"`
	JSON             bool   `yaml:"-"`
	Verbose          bool   `yaml:"-"`

	configFile *ConfigFile
}

// ConfigFile represents the structure of .queen.yaml
type ConfigFile struct {
	ConfigLocked bool                    `yaml:"config_locked"`
	Naming       *NamingConfig           `yaml:"naming"`
	Environments map[string]*Environment `yaml:",inline"`
}

// NamingConfig represents naming pattern configuration in YAML.
type NamingConfig struct {
	Pattern string `yaml:"pattern"`
	Padding int    `yaml:"padding"`
	Enforce *bool  `yaml:"enforce"`
}

// Environment represents a single environment configuration.
type Environment struct {
	Driver                string        `yaml:"driver"`
	DSN                   string        `yaml:"dsn"`
	Table                 string        `yaml:"table"`
	LockTimeout           time.Duration `yaml:"lock_timeout"`
	RequireConfirmation   bool          `yaml:"require_confirmation"`
	RequireExplicitUnlock bool          `yaml:"require_explicit_unlock"`
}

func (app *App) loadConfigFile() error {
	configPath := ".queen.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("config file not found: .queen.yaml (use --use-config only when config file exists)")
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var cf ConfigFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	app.config.configFile = &cf

	if cf.ConfigLocked {
		return fmt.Errorf("config file is locked for safety. Remove 'config_locked: true' or use flags/ENV vars instead")
	}

	if app.config.Env != "" {
		env, ok := cf.Environments[app.config.Env]
		if !ok {
			return fmt.Errorf("environment '%s' not found in config file", app.config.Env)
		}

		if env.RequireExplicitUnlock && !app.config.UnlockProduction {
			return fmt.Errorf("environment '%s' requires --unlock-production flag", app.config.Env)
		}

		if app.config.Driver == "" {
			app.config.Driver = env.Driver
		}
		if app.config.DSN == "" {
			app.config.DSN = env.DSN
		}
		if app.config.Table == DefaultTableName && env.Table != "" {
			app.config.Table = env.Table
		}
		if app.config.LockTimeout == 0 && env.LockTimeout > 0 {
			app.config.LockTimeout = env.LockTimeout
		}

		app.config.configFile.Environments = map[string]*Environment{
			app.config.Env: env,
		}
	}

	return nil
}

func (app *App) requiresConfirmation() bool {
	if app.config.Yes {
		return false
	}

	if app.config.configFile == nil || app.config.Env == "" {
		return false
	}

	env, ok := app.config.configFile.Environments[app.config.Env]
	if !ok {
		return false
	}

	return env.RequireConfirmation
}

func (app *App) getEnvironmentName() string {
	if app.config.Env != "" {
		return app.config.Env
	}
	return "custom"
}

func (nc *NamingConfig) toQueenNamingConfig() *queen.NamingConfig {
	if nc == nil {
		return nil
	}

	config := &queen.NamingConfig{
		Pattern: queen.NamingPattern(nc.Pattern),
		Padding: nc.Padding,
		Enforce: true,
	}

	if nc.Enforce != nil {
		config.Enforce = *nc.Enforce
	}

	return config
}

func (app *App) getNamingConfig() *queen.NamingConfig {
	if app.config.configFile == nil || app.config.configFile.Naming == nil {
		return nil
	}

	return app.config.configFile.Naming.toQueenNamingConfig()
}
