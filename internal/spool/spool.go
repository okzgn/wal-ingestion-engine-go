package spool

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"github.com/okzgn/wal-ingestion-engine-go/internal/fsutil"
	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
)

// ErrLocked is returned when another instance already owns the data directory.
var ErrLocked = errors.New("data directory is locked by another instance")

// errGone means a rotated file is no longer where the writer left it.
var errGone = errors.New("file is gone")

// Spool ties the directories, the dispatcher and the instance lock together.
type Spool struct {
	Dirs Dirs
	Disp *Dispatcher

	log  *slog.Logger
	lock *flock.Flock

	// rename moves a file between spool directories; tests inject faults here.
	rename             func(oldPath, newPath string) error
	retryMin, retryMax time.Duration

	// stranded holds, in rotation order, the files whose move to processing/
	// failed. A background goroutine keeps retrying them, so a transient
	// failure (a full or read-only directory, a handle held by a scanner) no
	// longer leaves acknowledged messages waiting for the next restart. Their
	// bytes count as backlog: the admission limit must see them.
	mu            sync.Mutex
	stranded      []strandedFile
	strandedBytes atomic.Int64
	wake          chan struct{}
	stop          chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
}

type strandedFile struct {
	path string
	size int64
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
	s := &Spool{
		Dirs: dirs, Disp: NewDispatcher(), log: log, lock: lock,
		rename:   func(oldPath, newPath string) error { return fsutil.RenameWithRetry(oldPath, newPath, 5) },
		retryMin: time.Second,
		retryMax: 30 * time.Second,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	metrics.SetBacklogOldest(s.oldestAge)
	go s.retryStranded()
	return s, nil
}

// oldestAge is how long the oldest undelivered file has been waiting.
func (s *Spool) oldestAge() time.Duration {
	oldest, found := s.Disp.Oldest()
	s.mu.Lock()
	for _, f := range s.stranded {
		if t, ok := TimeOf(filepath.Base(f.path)); ok && (!found || t.Before(oldest)) {
			oldest, found = t, true
		}
	}
	s.mu.Unlock()
	if !found {
		return 0
	}
	return max(time.Since(oldest), 0)
}

// Close stops the handoff retries and releases the instance lock. Files that
// are still stranded stay in ingestions/, where the next Recover adopts them.
func (s *Spool) Close() error {
	s.closeOnce.Do(func() { close(s.stop) })
	<-s.done
	s.Disp.Close()
	return s.lock.Unlock()
}

// BacklogBytes is everything that is on disk and not yet delivered: the files
// known to the dispatcher plus the ones still waiting to be moved to it.
func (s *Spool) BacklogBytes() int64 {
	return s.Disp.BacklogBytes() + s.strandedBytes.Load()
}

// BacklogFiles is the number of files not yet fully processed.
func (s *Spool) BacklogFiles() int {
	s.mu.Lock()
	n := len(s.stranded)
	s.mu.Unlock()
	return s.Disp.BacklogFiles() + n
}

// Ready hands a closed WAL file over to the workers: it is moved atomically
// to processing/ and queued. If the move fails the file stays in ingestions/
// and is retried in the background until it succeeds.
func (s *Spool) Ready(path string) {
	// Keep rotation order: while older files are stranded, a newer one waits
	// behind them instead of overtaking.
	s.mu.Lock()
	queued := len(s.stranded) > 0
	s.mu.Unlock()
	if !queued {
		err := s.handoff(path)
		if err == nil {
			return
		}
		if errors.Is(err, errGone) {
			s.log.Error("rotated file is gone", "file", filepath.Base(path), "err", err)
			return
		}
		s.log.Error("cannot move file to processing; retrying in the background", "file", filepath.Base(path), "err", err)
	}
	s.strand(path)
}

// handoff moves one file to processing/ and queues it.
func (s *Spool) handoff(path string) error {
	name := filepath.Base(path)
	dst := filepath.Join(s.Dirs.Processing, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %v", errGone, err)
		}
		return err
	}
	if err := s.rename(path, dst); err != nil {
		return err
	}
	s.syncDirs(s.Dirs.Ingestions, s.Dirs.Processing)
	if s.Disp.Add(dst, info.Size()) {
		s.log.Info("file ready to process", "file", name, "bytes", info.Size())
	}
	return nil
}

// strand records a file that could not be handed over and wakes the retries.
func (s *Spool) strand(path string) {
	var size int64
	if info, err := os.Stat(path); err == nil {
		size = info.Size()
	}
	s.mu.Lock()
	s.stranded = append(s.stranded, strandedFile{path: path, size: size})
	metrics.HandoffPending.Set(int64(len(s.stranded)))
	s.mu.Unlock()
	s.strandedBytes.Add(size)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// retryStranded hands the stranded files over, oldest first, with backoff.
func (s *Spool) retryStranded() {
	defer close(s.done)
	delay := s.retryMin
	for {
		s.mu.Lock()
		var head strandedFile
		pending := len(s.stranded) > 0
		if pending {
			head = s.stranded[0]
		}
		s.mu.Unlock()

		if !pending {
			select {
			case <-s.stop:
				return
			case <-s.wake:
			}
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.stop:
			timer.Stop()
			return
		case <-timer.C:
		}

		name := filepath.Base(head.path)
		err := s.handoff(head.path)
		switch {
		case err == nil:
			s.log.Info("stranded file moved to processing", "file", name)
		case errors.Is(err, errGone):
			s.log.Error("stranded file is gone", "file", name, "err", err)
		default:
			delay = min(delay*2, s.retryMax)
			s.log.Warn("still cannot move file to processing", "file", name, "err", err, "retry_in", delay)
			continue
		}
		delay = s.retryMin
		s.mu.Lock()
		s.stranded = s.stranded[1:]
		metrics.HandoffPending.Set(int64(len(s.stranded)))
		s.mu.Unlock()
		s.strandedBytes.Add(-head.size)
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
