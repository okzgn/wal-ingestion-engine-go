//go:build windows

package fsutil

import (
	"os"

	"golang.org/x/sys/windows"
)

// rename moves oldPath to newPath and does not return before the move is on
// disk. Windows has no way to fsync a directory, so the durability that
// SyncDir gives on Unix comes here from MOVEFILE_WRITE_THROUGH: without it a
// power loss right after the handoff could bring a rotated file back to where
// it was, or leave it in neither directory until the journal is replayed.
func rename(oldPath, newPath string) error {
	from, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: err}
	}
	to, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: err}
	}
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		// os.Rename knows about long paths and other corner cases; if it fails
		// too, its error is the one worth reporting.
		return os.Rename(oldPath, newPath)
	}
	return nil
}
