package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCursorFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor")
	if got, err := loadCursor(path); err != nil || got != "" {
		t.Fatalf("initial cursor = %q, err = %v", got, err)
	}
	if err := storeCursor(path, "s=step34;i=42"); err != nil {
		t.Fatal(err)
	}
	got, err := loadCursor(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "s=step34;i=42" {
		t.Fatalf("cursor = %q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("cursor file permissions = %o, want owner-only", info.Mode().Perm())
	}
}
