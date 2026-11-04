// Package spool manages the on-disk lifecycle of ingestion files:
//
//	ingestions/  active and just-rotated WAL files (written by internal/wal)
//	processing/  rotated files waiting for, or being processed by, a worker
//	failed/      dead letters: rejected lines (*.bad) and abandoned files
package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	// FileExt is the extension of WAL data files.
	FileExt = ".list"
	// CheckpointExt is the extension of the checkpoint stored next to a data file.
	CheckpointExt = ".ckpt"
	// BadExt is the extension of the per-file dead-letter of rejected lines.
	BadExt = ".bad"
)

// Dirs are the spool directories.
type Dirs struct {
	Root       string
	Ingestions string
	Processing string
	Failed     string
}

// NewDirs derives the spool layout from the data root.
func NewDirs(root string) Dirs {
	return Dirs{
		Root:       root,
		Ingestions: filepath.Join(root, "ingestions"),
		Processing: filepath.Join(root, "processing"),
		Failed:     filepath.Join(root, "failed"),
	}
}

// Create makes sure all directories exist. They are private to the service
// user: the files inside hold whatever the webhooks carried.
func (d Dirs) Create() error {
	for _, dir := range []string{d.Ingestions, d.Processing, d.Failed} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	return nil
}

// NewName builds a WAL file name. The zero-padded UTC timestamp comes first so
// that lexical order equals chronological order, also across restarts (the pid
// only disambiguates concurrent files).
func NewName(now time.Time, seq uint64, pid int) string {
	return fmt.Sprintf("ingest_%020d_%06d_%d%s", now.UTC().UnixNano(), seq, pid, FileExt)
}
