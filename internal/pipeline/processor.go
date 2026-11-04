// Package pipeline turns rotated WAL files into deliveries: it reads each file
// line by line, hands every message to the Sink with retries, and keeps a
// checkpoint so a crash resumes where it left off. It never deletes a file that
// still holds undelivered messages or dead letters that are not yet on disk.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/fsutil"
	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
	"github.com/okzgn/wal-ingestion-engine-go/internal/sink"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

// ErrRetryLater is returned (wrapped) when a file could not be completed but is
// intact: the sink kept failing, the file could not be read, or its dead
// letters could not be made durable. The file and its checkpoint are kept and
// the runner schedules it again.
var ErrRetryLater = errors.New("file postponed, will be retried")

// errDeadLetters marks a failure to write or sync failed/<file>.bad. The
// checkpoint must then not advance and the data file must not be deleted,
// otherwise a rejected line would vanish without a trace.
var errDeadLetters = errors.New("dead letters are not durable")

// DefaultMaxLineSize bounds the memory one line may take while it is read.
const DefaultMaxLineSize = 16 << 20

// RetryPolicy controls per-message delivery retries.
type RetryPolicy struct {
	MaxAttempts int           // total attempts per message (>= 1)
	BaseDelay   time.Duration // first backoff
	MaxDelay    time.Duration // backoff cap
}

// Options configures a Processor.
type Options struct {
	Sink      sink.Sink
	FailedDir string

	// MaxLineSize is the longest line that is still delivered; a longer one is
	// dead-lettered with its first MaxLineSize bytes. It is deliberately not the
	// server's MAX_MESSAGE_SIZE: a file written under a larger limit must still
	// deliver its messages whole after the limit is lowered. Default 16 MiB.
	MaxLineSize int

	CheckpointInterval int
	Retry              RetryPolicy
	Logger             *slog.Logger
}

// Processor handles one file at a time. It is safe for concurrent use on
// different files.
type Processor struct {
	o        Options
	interval uint64
}

// NewProcessor applies defaults and returns a Processor.
func NewProcessor(o Options) *Processor {
	if o.Sink == nil {
		o.Sink = sink.Noop{}
	}
	if o.MaxLineSize <= 0 {
		o.MaxLineSize = DefaultMaxLineSize
	}
	if o.CheckpointInterval <= 0 {
		o.CheckpointInterval = 50
	}
	if o.Retry.MaxAttempts <= 0 {
		o.Retry.MaxAttempts = 5
	}
	if o.Retry.BaseDelay <= 0 {
		o.Retry.BaseDelay = 200 * time.Millisecond
	}
	if o.Retry.MaxDelay <= 0 {
		o.Retry.MaxDelay = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Processor{
		o:        o,
		interval: uint64(o.CheckpointInterval), //nolint:gosec // #nosec G115 -- forced to be >= 1 just above
	}
}

// ProcessFile delivers every message of the file at path. It returns:
//
//   - nil when the file was fully handled and has been deleted;
//   - ctx.Err() when interrupted (progress is checkpointed, the file is kept);
//   - an error wrapping ErrRetryLater when the file must be tried again later.
func (p *Processor) ProcessFile(ctx context.Context, path string) error {
	name := filepath.Base(path)
	log := p.o.Logger.With("file", name)

	ck, done, err := OpenCheckpoint(path + spool.CheckpointExt)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRetryLater, err)
	}
	f, err := os.Open(path)
	if err != nil {
		_ = ck.Close()
		return fmt.Errorf("%w: open file: %v", ErrRetryLater, err)
	}
	bad := &badWriter{path: filepath.Join(p.o.FailedDir, name+spool.BadExt), dir: p.o.FailedDir}
	release := func() {
		_ = f.Close()
		_ = ck.Close()
		_ = bad.close()
	}

	var handled, saved uint64 = 0, done
	// save makes the dead letters durable, then records the progress. A line is
	// never marked handled before its dead letter is on disk: a dead-letter
	// failure (errDeadLetters) therefore blocks the checkpoint, while a
	// checkpoint failure only means more redelivery after a crash.
	save := func() error {
		if handled <= saved {
			return nil
		}
		if err := bad.sync(); err != nil {
			return fmt.Errorf("%w: %v", errDeadLetters, err)
		}
		if err := ck.Save(handled); err != nil {
			return err
		}
		saved = handled
		return nil
	}
	checkpoint := func() {
		if err := save(); err != nil {
			log.Warn("progress not checkpointed; lines since the last checkpoint will be redelivered", "err", err)
		}
	}
	// postpone keeps the file for a later attempt.
	postpone := func(cause error) error {
		release()
		return fmt.Errorf("%w: %v", ErrRetryLater, cause)
	}
	deadLetter := func(lineNo uint64, reason string, payload []byte) error {
		if err := bad.write(lineNo, reason, payload); err != nil {
			return fmt.Errorf("%w: line %d: %v", errDeadLetters, lineNo, err)
		}
		metrics.DeadLettered.Add(1)
		return nil
	}

	if done > 0 {
		log.Info("resuming file", "from_line", done+1)
	}

	lr := newLineReader(f, p.o.MaxLineSize)
	var lineNo uint64
	for {
		line, tooLong, terminated, rerr := lr.next()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			checkpoint()
			return postpone(fmt.Errorf("read: %w", rerr))
		}
		lineNo++
		if lineNo <= done {
			handled = lineNo
			continue
		}
		if err := ctx.Err(); err != nil {
			checkpoint()
			release()
			return err
		}

		var derr error
		switch {
		case tooLong:
			derr = deadLetter(lineNo, fmt.Sprintf("line exceeds the maximum line size of %d bytes; its first %d bytes are kept", p.o.MaxLineSize, len(line)), line)
		case !terminated:
			derr = deadLetter(lineNo, "unterminated line (torn write)", line)
		case !json.Valid(line):
			derr = deadLetter(lineNo, "invalid JSON", line)
		default:
			key := fmt.Sprintf("%s:%d", name, lineNo)
			derr = p.deliver(ctx, key, line)
			switch {
			case derr == nil:
				metrics.Delivered.Add(1)
			case sink.IsPermanent(derr):
				log.Warn("message rejected for good by the sink", "line", lineNo, "err", derr)
				derr = deadLetter(lineNo, derr.Error(), line)
			case ctx.Err() != nil:
				checkpoint()
				release()
				return ctx.Err()
			default:
				// The failed line is NOT marked as handled: the checkpoint stays
				// right before it and nothing after it is skipped or deleted.
				log.Warn("sink keeps failing; postponing file", "line", lineNo, "err", derr)
				checkpoint()
				return postpone(fmt.Errorf("line %d: %w", lineNo, derr))
			}
		}
		if derr != nil {
			// The dead letter is not on disk, so the line is not handled: the
			// checkpoint stays before it and the file is tried again later.
			log.Error("cannot write dead letter; postponing file", "line", lineNo, "err", derr)
			return postpone(derr)
		}

		handled = lineNo
		if handled%p.interval == 0 {
			if err := save(); err != nil {
				if errors.Is(err, errDeadLetters) {
					log.Error("cannot make dead letters durable; postponing file", "err", err)
					return postpone(err)
				}
				log.Warn("cannot save checkpoint; more lines would be redelivered after a crash", "err", err)
			}
		}
	}

	// The file is deleted only once every dead letter is durable. The final
	// checkpoint matters only if the removal below fails.
	if err := save(); err != nil {
		if errors.Is(err, errDeadLetters) {
			log.Error("cannot make dead letters durable; keeping file", "err", err)
			return postpone(err)
		}
		log.Warn("cannot save checkpoint", "err", err)
	}
	release()
	log.Info("file processed", "lines", lineNo)

	if err := fsutil.RemoveWithRetry(path, 5); err != nil {
		// Keep the checkpoint: recovery will find a fully-checkpointed file,
		// redeliver nothing and try the removal again.
		log.Warn("cannot remove processed file; kept for the next recovery", "err", err)
		return nil
	}
	_ = fsutil.RemoveWithRetry(path+spool.CheckpointExt, 5)
	return nil
}

// deliver sends one message, retrying transient failures with exponential
// backoff and jitter. It returns nil, a permanent error, ctx.Err(), or the last
// transient error once the attempts are exhausted.
func (p *Processor) deliver(ctx context.Context, key string, line []byte) error {
	r := p.o.Retry
	var err error
	for attempt := 1; ; attempt++ {
		err = p.o.Sink.Deliver(ctx, key, line)
		if err == nil || sink.IsPermanent(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= r.MaxAttempts {
			return fmt.Errorf("giving up after %d attempts: %w", attempt, err)
		}
		metrics.Retried.Add(1)
		if serr := sleep(ctx, Backoff(attempt, r.BaseDelay, r.MaxDelay)); serr != nil {
			return serr
		}
	}
}

// Backoff returns the delay before retry number attempt (1-based): exponential
// from base, capped at max, with "equal jitter" (half fixed, half random).
func Backoff(attempt int, base, max time.Duration) time.Duration {
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + rand.N(half+1) //nolint:gosec // #nosec G404 -- jitter only spreads retries; it need not be unpredictable
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// badWriter appends rejected lines to failed/<file>.bad as
// "<line number>\t<reason>\t<payload>". The file is created on first use.
// After the first failure every call fails: the file is no longer trusted.
type badWriter struct {
	path  string
	dir   string
	f     *os.File
	dirty bool
	err   error
}

func (b *badWriter) write(lineNo uint64, reason string, payload []byte) error {
	if b.err != nil {
		return b.err
	}
	if b.f == nil {
		f, err := os.OpenFile(b.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			b.err = err
			return err
		}
		b.f = f
		if err := fsutil.SyncDir(b.dir); err != nil {
			b.err = err
			return err
		}
	}
	reason = strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(reason)
	rec := make([]byte, 0, len(reason)+len(payload)+32)
	rec = fmt.Appendf(rec, "%d\t%s\t", lineNo, reason)
	rec = append(rec, payload...)
	rec = append(rec, '\n')
	b.dirty = true
	if _, err := b.f.Write(rec); err != nil {
		b.err = err
		return err
	}
	return nil
}

// sync makes the dead letters durable; it must precede a checkpoint save so a
// line is never marked handled before its dead letter is on disk.
func (b *badWriter) sync() error {
	if b.err != nil {
		return b.err
	}
	if b.f == nil || !b.dirty {
		return nil
	}
	if err := b.f.Sync(); err != nil {
		b.err = err
		return err
	}
	b.dirty = false
	return nil
}

func (b *badWriter) close() error {
	if b.f == nil {
		return nil
	}
	return b.f.Close()
}
