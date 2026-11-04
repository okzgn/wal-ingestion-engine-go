package spool

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gofrs/flock"
	"github.com/okzgn/wal-ingestion-engine-go/internal/fsutil"
)

// ErrLocked is returned when another instance already owns the data directory.
var ErrLocked = errors.New("data directory is locked by another instance")

// Spool ties the directories, the dispatcher and the instance lock together.
type Spool struct {
	Dirs Dirs
	Disp *Dispatcher

	log  *slog.Logger
	lock *flock.Flock
}

// Open creates the directories and takes the exclusive instance lock. A second
// instance on the same directory would otherwise move the first one's active
// file out from under it.
func Open(root string, log *slog.Logger) (*Spool, error) {
	if log == nil {
		log = slog.Default()
	}
	dirs := NewDirs(root)
	if err := dirs.Create(); err != nil {
		return nil, err
	}
	lock := flock.New(filepath.Join(root, "spool.lock"))
	ok, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock %s: %w", root, err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocked, root)
	}
	return &Spool{Dirs: dirs, Disp: NewDispatcher(), log: log, lock: lock}, nil
}

// Close releases the instance lock.
func (s *Spool) Close() error {
	s.Disp.Close()
	return s.lock.Unlock()
}

// Ready hands a closed WAL file over to the workers: it is moved atomically
// to processing/ and queued. A failed move leaves the file in ingestions/,
// where the next Recover picks it up.
func (s *Spool) Ready(path string) {
	name := filepath.Base(path)
	dst := filepath.Join(s.Dirs.Processing, name)
	info, err := os.Stat(path)
	if err != nil {
		s.log.Error("cannot stat rotated file", "file", name, "err", err)
		return
	}
	if err := fsutil.RenameWithRetry(path, dst, 5); err != nil {
		s.log.Error("cannot move file to processing; it will be retried on restart", "file", name, "err", err)
		return
	}
	s.syncDirs(s.Dirs.Ingestions, s.Dirs.Processing)
	if s.Disp.Add(dst, info.Size()) {
		s.log.Info("file ready to process", "file", name, "bytes", info.Size())
	}
}

// Quarantine moves a file (and its checkpoint) to failed/.
func (s *Spool) Quarantine(path string) error {
	name := filepath.Base(path)
	if err := fsutil.RenameWithRetry(path, filepath.Join(s.Dirs.Failed, name), 5); err != nil {
		return err
	}
	ckpt := path + CheckpointExt
	if _, err := os.Stat(ckpt); err == nil {
		_ = fsutil.RenameWithRetry(ckpt, filepath.Join(s.Dirs.Failed, name+CheckpointExt), 5)
	}
	s.syncDirs(s.Dirs.Processing, s.Dirs.Failed)
	s.Disp.Done(path)
	return nil
}

// syncDirs makes the entry changes of the given directories durable.
func (s *Spool) syncDirs(dirs ...string) {
	for _, dir := range dirs {
		if err := fsutil.SyncDir(dir); err != nil {
			s.log.Warn("directory fsync failed", "dir", dir, "err", err)
		}
	}
}

// Recover brings the spool to a consistent state after a start or a crash. It
// runs sequentially and before the WAL opens its active file, so no file can be
// queued twice and no live file can be taken by mistake:
//
//  1. sweep leftovers (orphan checkpoints, legacy markers);
//  2. move every non-empty ingestions/*.list to processing/ (empty ones are deleted);
//  3. queue processing/*.list in name (= chronological) order.
func (s *Spool) Recover() error {
	if err := s.sweepProcessing(); err != nil {
		return err
	}
	if err := s.adoptIngestions(); err != nil {
		return err
	}
	s.syncDirs(s.Dirs.Ingestions, s.Dirs.Processing, s.Dirs.Failed)
	return s.enqueueProcessing()
}

func (s *Spool) sweepProcessing() error {
	entries, err := os.ReadDir(s.Dirs.Processing)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(s.Dirs.Processing, name)
		switch {
		case strings.HasSuffix(name, ".delete"):
			// Marker of the previous engine version: its data file was fully processed.
			base := strings.TrimSuffix(strings.TrimSuffix(name, ".delete"), ".done")
			_ = os.Remove(filepath.Join(s.Dirs.Processing, base))
			_ = os.Remove(filepath.Join(s.Dirs.Processing, base+".done"))
			_ = os.Remove(path)
		case strings.HasSuffix(name, CheckpointExt), strings.HasSuffix(name, ".done"):
			data := strings.TrimSuffix(strings.TrimSuffix(path, CheckpointExt), ".done")
			if _, err := os.Stat(data); os.IsNotExist(err) {
				s.log.Info("removing orphan checkpoint", "file", name)
				_ = os.Remove(path)
			}
		}
	}
	return nil
}

func (s *Spool) adoptIngestions() error {
	entries, err := os.ReadDir(s.Dirs.Ingestions)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), FileExt) {
			continue
		}
		src := filepath.Join(s.Dirs.Ingestions, e.Name())
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Size() == 0 {
			_ = os.Remove(src)
			continue
		}
		if err := fsutil.RenameWithRetry(src, filepath.Join(s.Dirs.Processing, e.Name()), 5); err != nil {
			return fmt.Errorf("adopt %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (s *Spool) enqueueProcessing() error {
	entries, err := os.ReadDir(s.Dirs.Processing)
	if err != nil {
		return err
	}
	var names []string
	sizes := map[string]int64{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), FileExt) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Size() == 0 {
			_ = os.Remove(filepath.Join(s.Dirs.Processing, e.Name()))
			_ = os.Remove(filepath.Join(s.Dirs.Processing, e.Name()+CheckpointExt))
			continue
		}
		names = append(names, e.Name())
		sizes[e.Name()] = info.Size()
	}
	sort.Strings(names)
	for _, n := range names {
		path := filepath.Join(s.Dirs.Processing, n)
		if s.Disp.Add(path, sizes[n]) {
			s.log.Info("recovered file", "file", n, "bytes", sizes[n])
		}
	}
	return nil
}
