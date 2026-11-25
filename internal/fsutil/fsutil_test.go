package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRenameWithRetry(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "a"), filepath.Join(dir, "sub", "b")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameWithRetry(src, dst, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("the source must be gone")
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q, %v", got, err)
	}

	// Renaming over an existing file replaces it, on every platform.
	if err := os.WriteFile(src, []byte("newer"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameWithRetry(src, dst, 3); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "newer" {
		t.Fatalf("destination not replaced: %q", got)
	}

	// A failure is reported after the retries, not swallowed.
	if err := RenameWithRetry(filepath.Join(dir, "missing"), dst, 2); err == nil {
		t.Fatal("renaming a missing file must fail")
	}
	if err := RenameWithRetry(dst, filepath.Join(dir, "no-such-dir", "b"), 2); err == nil {
		t.Fatal("renaming into a missing directory must fail")
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatal("a failed rename must leave the source in place")
	}
}

func TestRemoveWithRetry(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveWithRetry(p, 3); err != nil {
		t.Fatal(err)
	}
	if err := RemoveWithRetry(p, 3); err != nil {
		t.Fatalf("a missing file counts as removed: %v", err)
	}
	// A directory that is not empty cannot be removed: the error must surface.
	if err := os.WriteFile(filepath.Join(dir, "g"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveWithRetry(dir, 2); err == nil {
		t.Fatal("removing a non-empty directory must fail")
	}
}

func TestSyncDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
