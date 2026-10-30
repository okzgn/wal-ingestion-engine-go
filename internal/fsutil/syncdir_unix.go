//go:build !windows

package fsutil

import "os"

// SyncDir flushes the directory entry changes (create/rename/remove) of dir
// to stable storage. Without it a file whose data was fsynced can still
// vanish after a power loss on some filesystems.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
