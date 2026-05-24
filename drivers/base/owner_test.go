package base

import (
	"encoding/hex"
	"testing"
)

func TestGenerateOwnerID(t *testing.T) {
	t.Run("generates non-empty ID", func(t *testing.T) {
		id, err := GenerateOwnerID()
		if err != nil {
			t.Fatalf("GenerateOwnerID() failed: %v", err)
		}
		if id == "" {
			t.Error("expected non-empty owner ID")
		}
	})

	t.Run("generates 32 character hex string", func(t *testing.T) {
		id, err := GenerateOwnerID()
		if err != nil {
			t.Fatalf("GenerateOwnerID() failed: %v", err)
		}
		if len(id) != 32 {
			t.Errorf("expected 32 characters, got %d", len(id))
		}
		// Verify it's valid hex
		_, err = hex.DecodeString(id)
		if err != nil {
			t.Errorf("owner ID should be valid hex: %v", err)
		}
	})

	t.Run("generates unique IDs", func(t *testing.T) {
		id1, err := GenerateOwnerID()
		if err != nil {
			t.Fatalf("GenerateOwnerID() failed: %v", err)
		}
		id2, err := GenerateOwnerID()
		if err != nil {
			t.Fatalf("GenerateOwnerID() failed: %v", err)
		}
		if id1 == id2 {
			t.Error("expected different IDs for consecutive calls")
		}
	})

	t.Run("generates many unique IDs", func(t *testing.T) {
		ids := make(map[string]bool)
		for range 100 {
			id, err := GenerateOwnerID()
			if err != nil {
				t.Fatalf("GenerateOwnerID() failed: %v", err)
			}
			if ids[id] {
				t.Errorf("duplicate ID generated: %s", id)
			}
			ids[id] = true
		}
		if len(ids) != 100 {
			t.Errorf("expected 100 unique IDs, got %d", len(ids))
		}
	})
}
