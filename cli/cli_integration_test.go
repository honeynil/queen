package cli

import (
	"testing"
)

func TestLoadConfigWithEnv(t *testing.T) {
	t.Parallel()

	t.Run("loadConfig without use-config flag", func(t *testing.T) {
		t.Parallel()

		app := &App{
			config: &Config{
				UseConfig: false,
				Driver:    "postgres",
				DSN:       "test",
			},
		}

		err := app.loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if app.config.Driver != "postgres" {
			t.Errorf("driver should remain postgres, got %q", app.config.Driver)
		}
	})
}
