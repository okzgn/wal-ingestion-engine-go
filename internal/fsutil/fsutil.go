// Package fsutil holds small filesystem helpers shared by the engine packages.
package fsutil

import (
	"os"
	"time"
)

// RemoveWithRetry removes path, retrying a few times with a short backoff.
// On Windows an antivirus or indexer may briefly hold a handle on a file that
// was just closed. A missing file counts as success.
func RemoveWithRetry(path string, attempts int) error {
	var err error
	for i := 1; i <= attempts; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		time.Sleep(time.Duration(i) * 20 * time.Millisecond)
	}
	return err
}

// RenameWithRetry renames oldPath to newPath with the same retry policy as
// RemoveWithRetry.
func RenameWithRetry(oldPath, newPath string, attempts int) error {
	var err error
	for i := 1; i <= attempts; i++ {
		err = os.Rename(oldPath, newPath)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i) * 20 * time.Millisecond)
	}
	return err
}
