package cli

import (
	"io"
	"os"
	"testing"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}

	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()
	os.Stdout = oldStdout

	if err := w.Close(); err != nil {
		t.Fatalf("stdout pipe close error = %v", err)
	}

	out, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("stdout read error = %v", err)
	}

	return string(out)
}
