//go:build windows

package fsutil

// SyncDir is a no-op on Windows: a directory cannot be opened for flushing.
// The two things it protects on Unix are covered differently here: a new file
// becomes durable with its first fsync (FlushFileBuffers also flushes the
// file's metadata on NTFS), and a rename is written through by RenameWithRetry.
func SyncDir(dir string) error { return nil }
