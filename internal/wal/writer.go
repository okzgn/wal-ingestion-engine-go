// Package wal implements the write-ahead log: a single writer goroutine that
// groups incoming messages into batches, writes each batch with one write(2)
// and one fsync(2), and only then acknowledges every message of the batch.
//
// The guarantee is simple and honest: Append returns nil if, and only if, the
// message has been written and synced to disk. Any disk error is propagated to
// every message of the affected batch and puts the writer into a degraded state
// (fast 503s) until a fresh file can be created again.
package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/fsutil"
	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

var (
	// ErrUnavailable means the disk is failing; nothing was enqueued.
	ErrUnavailable = errors.New("wal: unavailable")
	// ErrOverloaded means the admission limits were hit; nothing was enqueued.
	ErrOverloaded = errors.New("wal: overloaded")
	// ErrClosed means the writer is shutting down.
	ErrClosed = errors.New("wal: closed")
	// ErrAckTimeout means the context ended while the message was in flight. The
	// message may still be committed, and the caller MUST NOT reuse the buffer.
	ErrAckTimeout = errors.New("wal: acknowledgement timed out")
	// ErrNewline is returned for payloads that would break the one-line framing.
	ErrNewline = errors.New("wal: payload contains a newline")
)

// File is the subset of *os.File the writer needs; tests inject faults here.
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// Opener creates a new, empty append-only file.
type Opener func(path string) (File, error)

// Handoff receives each closed file that holds data.
type Handoff interface {
	Ready(path string)
}

// Config tunes the writer. Zero values get sensible defaults in New.
type Config struct {
	Dir              string        // directory of the active file (ingestions/)
	MaxFileSize      int64         // rotate once the file reaches this size
	RotationInterval time.Duration // rotate a non-empty file after this long
	MaxBatchSize     int           // max messages per group commit
	MaxBatchBytes    int           // max bytes per group commit
	QueueSize        int           // capacity of the job queue
	MaxInflightBytes int64         // admission limit for queued + committing bytes
	Linger           time.Duration // optional wait to gather a larger batch (0 = commit asap)
	RetryMin         time.Duration // degraded-mode reopen backoff, first delay
	RetryMax         time.Duration // degraded-mode reopen backoff, cap

	Open    Opener
	Handoff Handoff
	Logger  *slog.Logger
	PID     int
	Now     func() time.Time
}

func (c *Config) defaults() {
	if c.MaxFileSize <= 0 {
		c.MaxFileSize = 1 << 20
	}
	if c.RotationInterval <= 0 {
		c.RotationInterval = 10 * time.Second
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 256
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 1 << 20
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 4096
	}
	if c.MaxInflightBytes <= 0 {
		c.MaxInflightBytes = 64 << 20
	}
	if c.RetryMin <= 0 {
		c.RetryMin = time.Second
	}
	if c.RetryMax <= 0 {
		c.RetryMax = 30 * time.Second
	}
	if c.Open == nil {
		c.Open = openOS
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.PID == 0 {
		c.PID = os.Getpid()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

func openOS(path string) (File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
}

type job struct {
	data []byte
	done chan error
}

// Writer is the group-commit write-ahead log.
type Writer struct {
	cfg Config
	log *slog.Logger

	jobs     chan job
	quit     chan struct{}
	stopped  chan struct{}
	closing  atomic.Bool
	healthy  atomic.Bool
	inflight atomic.Int64
	closeMu  sync.Once

	// Owned by the run goroutine.
	f         File
	path      string
	fileBytes int64 // bytes written to the current file (synced after every commit)
	seq       uint64
	rotTimer  *time.Timer
	batch     []byte
	acks      []job
}

// New opens the first file synchronously (failing fast on a bad directory) and
// starts the writer goroutine.
func New(cfg Config) (*Writer, error) {
	cfg.defaults()
	w := &Writer{
		cfg:     cfg,
		log:     cfg.Logger,
		jobs:    make(chan job, cfg.QueueSize),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
		batch:   make([]byte, 0, cfg.MaxBatchBytes+(64<<10)),
	}
	w.rotTimer = time.NewTimer(time.Hour)
	w.rotTimer.Stop()
	if err := w.openFile(); err != nil {
		return nil, err
	}
	w.healthy.Store(true)
	metrics.Degraded.Set(0)
	go w.run()
	return w, nil
}

// Healthy reports whether the writer can currently persist data.
func (w *Writer) Healthy() bool { return w.healthy.Load() && !w.closing.Load() }

// Append blocks until payload is durable and returns nil, or returns the reason
// it is not. payload must be a single line (no '\n'); the writer adds the
// terminator. The writer owns payload until Append returns, except after
// ErrAckTimeout, when it may still be read: the caller must then not reuse it.
func (w *Writer) Append(ctx context.Context, payload []byte) error {
	if bytes.IndexByte(payload, '\n') >= 0 {
		return ErrNewline
	}
	if w.closing.Load() {
		return ErrClosed
	}
	if !w.healthy.Load() {
		return ErrUnavailable
	}
	size := int64(len(payload)) + 1
	if w.inflight.Add(size) > w.cfg.MaxInflightBytes {
		w.inflight.Add(-size)
		return ErrOverloaded
	}
	j := job{data: payload, done: make(chan error, 1)}
	select {
	case w.jobs <- j:
	default:
		w.inflight.Add(-size)
		return ErrOverloaded
	}

	select {
	case err := <-j.done:
		return err
	case <-w.stopped:
		select {
		case err := <-j.done:
			return err
		default:
			return ErrClosed
		}
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrAckTimeout, ctx.Err())
	}
}

// Close stops accepting messages, commits everything already queued, closes the
// active file and hands it over. It waits for the writer goroutine or ctx.
func (w *Writer) Close(ctx context.Context) error {
	w.closeMu.Do(func() {
		w.closing.Store(true)
		close(w.quit)
	})
	select {
	case <-w.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) run() {
	defer close(w.stopped)
	retry := w.cfg.RetryMin
	retryTimer := time.NewTimer(time.Hour)
	retryTimer.Stop()
	defer retryTimer.Stop()
	armed := false

	for {
		if w.f == nil {
			// Degraded: refuse jobs quickly and keep trying to get a new file.
			if !armed {
				retryTimer.Reset(retry)
				armed = true
			}
			select {
			case j := <-w.jobs:
				w.resolve(j, ErrUnavailable)
			case <-retryTimer.C:
				armed = false
				if err := w.openFile(); err != nil {
					w.log.Warn("wal still degraded", "err", err, "retry_in", retry)
					retry = min(retry*2, w.cfg.RetryMax)
					continue
				}
				w.log.Info("wal recovered")
				retry = w.cfg.RetryMin
				w.healthy.Store(true)
				metrics.Degraded.Set(0)
			case <-w.quit:
				w.failQueued(ErrClosed)
				return
			}
			continue
		}

		select {
		case j := <-w.jobs:
			w.commitFrom(j)
		case <-w.rotTimer.C:
			if w.fileBytes > 0 {
				w.rotate()
			} else {
				w.rotTimer.Reset(w.cfg.RotationInterval)
			}
		case <-w.quit:
			w.drain()
			w.finalize()
			return
		}
	}
}

// commitFrom builds a batch starting with first, commits it and rotates if due.
func (w *Writer) commitFrom(first job) {
	w.batch = w.batch[:0]
	w.acks = w.acks[:0]
	w.add(first)

	if w.cfg.Linger > 0 {
		timer := time.NewTimer(w.cfg.Linger)
	gather:
		for w.batchRoom() {
			select {
			case j := <-w.jobs:
				w.add(j)
			case <-timer.C:
				break gather
			}
		}
		timer.Stop()
	}
	for w.batchRoom() {
		select {
		case j := <-w.jobs:
			w.add(j)
			continue
		default:
		}
		break
	}
	w.commit()
}

func (w *Writer) batchRoom() bool {
	return len(w.acks) < w.cfg.MaxBatchSize && len(w.batch) < w.cfg.MaxBatchBytes
}

func (w *Writer) add(j job) {
	w.batch = append(w.batch, j.data...)
	w.batch = append(w.batch, '\n')
	w.acks = append(w.acks, j)
}

// commit writes and syncs the pending batch, then acknowledges every job with
// the real outcome.
func (w *Writer) commit() {
	start := time.Now()
	n, err := w.f.Write(w.batch)
	if err == nil && n != len(w.batch) {
		err = fmt.Errorf("short write: %d of %d bytes", n, len(w.batch))
	}
	if err == nil {
		err = w.f.Sync()
	}
	metrics.FsyncMicros.Add(time.Since(start).Microseconds())
	metrics.FsyncCount.Add(1)

	if err != nil {
		w.log.Error("wal commit failed; entering degraded mode", "err", err, "messages", len(w.acks))
		w.resolveAll(fmt.Errorf("%w: %v", ErrUnavailable, err))
		w.degrade()
		return
	}

	w.fileBytes += int64(n)
	metrics.BatchMessages.Add(int64(len(w.acks)))
	metrics.BatchCount.Add(1)
	w.resolveAll(nil)

	if w.fileBytes >= w.cfg.MaxFileSize {
		w.rotate()
	}
}

func (w *Writer) resolveAll(err error) {
	for _, j := range w.acks {
		w.resolve(j, err)
	}
	w.acks = w.acks[:0]
}

func (w *Writer) resolve(j job, err error) {
	w.inflight.Add(-(int64(len(j.data)) + 1))
	if err == nil {
		metrics.Accepted.Add(1)
	}
	j.done <- err // buffered: never blocks
}

// degrade abandons the current file after a disk error. Whatever complete lines
// it holds are handed over; the (possibly torn) tail is dealt with by the
// processor. The writer then retries opening a new file with backoff.
func (w *Writer) degrade() {
	w.healthy.Store(false)
	metrics.Degraded.Set(1)
	w.rotTimer.Stop()
	if tr, ok := w.f.(interface{ Truncate(int64) error }); ok {
		_ = tr.Truncate(w.fileBytes) // drop the unsynced batch, best effort
	}
	w.closeCurrent()
}

// rotate closes the active file and starts a new one. Failing to open the new
// file degrades the writer instead of killing the process.
func (w *Writer) rotate() {
	w.closeCurrent()
	if err := w.openFile(); err != nil {
		w.log.Error("cannot open next wal file; entering degraded mode", "err", err)
		w.healthy.Store(false)
		metrics.Degraded.Set(1)
	}
}

// closeCurrent closes the active file and hands it to the workers (or removes it
// if it is empty).
func (w *Writer) closeCurrent() {
	if w.f == nil {
		return
	}
	if err := w.f.Close(); err != nil {
		w.log.Warn("closing wal file", "file", filepath.Base(w.path), "err", err)
	}
	path, size := w.path, w.fileBytes
	w.f, w.path, w.fileBytes = nil, "", 0
	if size == 0 {
		_ = os.Remove(path)
		return
	}
	metrics.FilesRotated.Add(1)
	if w.cfg.Handoff != nil {
		w.cfg.Handoff.Ready(path)
	}
}

func (w *Writer) openFile() error {
	w.seq++
	name := spool.NewName(w.cfg.Now(), w.seq, w.cfg.PID)
	path := filepath.Join(w.cfg.Dir, name)
	f, err := w.cfg.Open(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if err := fsutil.SyncDir(w.cfg.Dir); err != nil {
		w.log.Warn("directory fsync failed", "dir", w.cfg.Dir, "err", err)
	}
	w.f, w.path, w.fileBytes = f, path, 0
	w.rotTimer.Reset(w.cfg.RotationInterval)
	w.log.Info("wal file created", "file", name)
	return nil
}

// drain commits everything that is still queued (shutdown path).
func (w *Writer) drain() {
	for {
		select {
		case j := <-w.jobs:
			if w.f == nil {
				w.resolve(j, ErrClosed)
				continue
			}
			w.commitFrom(j)
		default:
			return
		}
	}
}

// finalize closes the active file for good.
func (w *Writer) finalize() {
	w.rotTimer.Stop()
	w.closeCurrent()
}

// failQueued answers every queued job with err (shutdown while degraded).
func (w *Writer) failQueued(err error) {
	for {
		select {
		case j := <-w.jobs:
			w.resolve(j, err)
		default:
			return
		}
	}
}
