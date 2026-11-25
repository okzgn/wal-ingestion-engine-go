//go:build !windows

package fsutil

import "os"

// rename moves oldPath to newPath. It is atomic; the callers make it durable
// with SyncDir on both directories.
func rename(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }
