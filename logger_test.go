package queen

import (
	"context"
	"testing"
)

func TestNoopLogger(t *testing.T) {
	t.Parallel()

	logger := &noopLogger{}
	ctx := context.Background()

	t.Run("InfoContext doesn't panic", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("InfoContext panicked: %v", r)
			}
		}()
		logger.InfoContext(ctx, "test message", "key", "value")
	})

	t.Run("WarnContext doesn't panic", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WarnContext panicked: %v", r)
			}
		}()
		logger.WarnContext(ctx, "test warning", "key", "value")
	})

	t.Run("ErrorContext doesn't panic", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ErrorContext panicked: %v", r)
			}
		}()
		logger.ErrorContext(ctx, "test error", "key", "value")
	})
}

func TestDefaultLogger(t *testing.T) {
	t.Parallel()

	logger := defaultLogger()

	if logger == nil {
		t.Fatal("defaultLogger() returned nil")
	}

	if _, ok := logger.(*noopLogger); !ok {
		t.Errorf("defaultLogger() type = %T, want *noopLogger", logger)
	}
}

type testLogger struct {
	infoCalls  int
	warnCalls  int
	errorCalls int
}

func (l *testLogger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.infoCalls++
}

func (l *testLogger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.warnCalls++
}

func (l *testLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.errorCalls++
}

func TestLoggerInterface(t *testing.T) {
	t.Parallel()

	logger := &testLogger{}
	ctx := context.Background()

	var _ Logger = logger

	logger.InfoContext(ctx, "info")
	logger.WarnContext(ctx, "warn")
	logger.ErrorContext(ctx, "error")

	if logger.infoCalls != 1 {
		t.Errorf("infoCalls = %d, want 1", logger.infoCalls)
	}
	if logger.warnCalls != 1 {
		t.Errorf("warnCalls = %d, want 1", logger.warnCalls)
	}
	if logger.errorCalls != 1 {
		t.Errorf("errorCalls = %d, want 1", logger.errorCalls)
	}
}
