//go:build windows

package fsutil

// SyncDir is a no-op on Windows: directories cannot be opened for flushing
// and NTFS journals metadata updates itself.
func SyncDir(dir string) error { return nil }
