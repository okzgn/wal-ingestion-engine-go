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
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// ErrCleanupPending is returned (wrapped) when every line of a file was handled
// but the file itself could not be removed. It is a kind of ErrRetryLater: the
// checkpoint covers the whole file, so the next attempt delivers nothing again
// and only retries the removal. It is not a delivery failure and must not
// count towards giving up on the file.
var ErrCleanupPending = fmt.Errorf("%w: processed file could not be removed", ErrRetryLater)

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
	// dead-lettered whole. It is deliberately not the server's MAX_MESSAGE_SIZE:
	// a file written under a larger limit must still deliver its messages after
	// the limit is lowered. MAX_MESSAGE_SIZE can never exceed it (the
	// configuration enforces that), or accepted messages could not be delivered.
	// Default 16 MiB.
	MaxLineSize int

	// Concurrency is how many messages of one file may be in delivery at the
	// same time (default 1). Deliveries then finish out of order, but progress
	// is still recorded as a contiguous prefix of the file, so nothing is ever
	// skipped. Only lines within Concurrency of that prefix are started, which
	// bounds what a crash can make redeliver to CheckpointInterval-1 plus
	// Concurrency lines per file. With 1 the file is delivered strictly in order.
	Concurrency int

	// MaxLineAttempts gives up on a message that keeps stalling its file: once
	// it has stalled the file this many times in a row, the next failure
	// dead-letters it, but only if the destination accepted another message
	// during that same attempt (so an outage never dead-letters anything).
	// 0 never gives up.
	MaxLineAttempts int

	CheckpointInterval int
	Retry              RetryPolicy
	Logger             *slog.Logger
}

// Processor handles one file at a time. It is safe for concurrent use on
// different files.
type Processor struct {
	o        Options
	interval uint64
	window   uint64    // Options.Concurrency
	bufs     sync.Pool // *[]byte: copies of the lines that are in delivery

	carryMu sync.Mutex
	carry   map[string]map[uint64]struct{} // see carryOver
	stalls  map[string]stall               // per file: the line it is stuck on

	// successes counts the deliveries that succeeded, on any file. A change
	// during an attempt is the evidence that the destination is up.
	successes atomic.Uint64

	// remove deletes a finished file or its checkpoint; tests inject faults here.
	remove func(path string) error
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
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	p := &Processor{
		o:        o,
		interval: uint64(o.CheckpointInterval), //nolint:gosec // #nosec G115 -- forced to be >= 1 just above
		window:   uint64(o.Concurrency),        //nolint:gosec // #nosec G115 -- forced to be >= 1 just above
		remove:   func(path string) error { return fsutil.RemoveWithRetry(path, 5) },
	}
	p.carry = map[string]map[uint64]struct{}{}
	p.stalls = map[string]stall{}
	p.bufs.New = func() any { return new([]byte) }
	return p
}

// maxPooledLine keeps rare large messages from bloating the buffer pool.
const maxPooledLine = 256 << 10

// copyLine returns a pooled copy of line: the reader reuses its buffer, and a
// line in delivery must outlive the next read.
func (p *Processor) copyLine(line []byte) *[]byte {
	bp := p.bufs.Get().(*[]byte)
	*bp = append((*bp)[:0], line...)
	return bp
}

func (p *Processor) freeLine(bp *[]byte) {
	if bp != nil && cap(*bp) <= maxPooledLine {
		p.bufs.Put(bp)
	}
}

// carryOver remembers the lines of a file that were delivered beyond its
// checkpoint when an attempt stops early (a line before them kept failing).
// The next attempt in this process skips them, so a transient failure in the
// middle of the delivery window does not deliver its neighbours twice. The
// memory is lost in a crash: that is the redelivery the window bounds.
func (p *Processor) carryOver(path string, settled map[uint64]bool) {
	var keep map[uint64]struct{}
	for n, delivered := range settled {
		if delivered {
			if keep == nil {
				keep = map[uint64]struct{}{}
			}
			keep[n] = struct{}{}
		}
	}
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	if keep == nil {
		delete(p.carry, path)
		return
	}
	p.carry[path] = keep
}

func (p *Processor) takeCarry(path string) map[uint64]struct{} {
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	keep := p.carry[path]
	delete(p.carry, path)
	return keep
}

// Forget drops what the processor remembers about a file that will not be
// processed again (it was moved away).
func (p *Processor) Forget(path string) {
	p.takeCarry(path)
	p.clearStall(path)
}

// StallError is returned when a file is postponed because the delivery of one
// of its lines kept failing. It is a kind of ErrRetryLater; Line lets the
// caller tell a file that is stuck on one message from one that advances
// between failures.
type StallError struct {
	Line uint64
	Err  error
}

func (e *StallError) Error() string {
	return fmt.Sprintf("%v: line %d: %v", ErrRetryLater, e.Line, e.Err)
}

// Unwrap makes errors.Is(err, ErrRetryLater) hold.
func (e *StallError) Unwrap() error { return ErrRetryLater }

// stall is how often in a row one line has stalled its file.
type stall struct {
	line   uint64
	rounds int
}

// noteStall records that line stalled the file once more.
func (p *Processor) noteStall(path string, line uint64) {
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	st := p.stalls[path]
	if st.line != line {
		st = stall{line: line}
	}
	st.rounds++
	p.stalls[path] = st
	metrics.StalledLines.Set(int64(len(p.stalls)))
}

func (p *Processor) clearStall(path string) {
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	delete(p.stalls, path)
	metrics.StalledLines.Set(int64(len(p.stalls)))
}

// suspectLine returns the line that has stalled the file MaxLineAttempts times
// (0 when there is none, or when giving up is disabled) and how many times.
func (p *Processor) suspectLine(path string, done uint64) (uint64, int) {
	if p.o.MaxLineAttempts <= 0 {
		return 0, 0
	}
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	st := p.stalls[path]
	if st.rounds < p.o.MaxLineAttempts || st.line <= done {
		return 0, 0
	}
	return st.line, st.rounds
}

// outcome is the result of delivering one line.
type outcome struct {
	lineNo uint64
	err    error
	line   *[]byte
}

// ProcessFile delivers every message of the file at path. It returns:
//
//   - nil when the file was fully handled and has been deleted;
//   - ctx.Err() when interrupted (progress is checkpointed, the file is kept);
//   - an error wrapping ErrRetryLater when the file must be tried again later
//     (ErrCleanupPending when only its removal is left).
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

	// handled is the contiguous prefix of the file that is fully dealt with:
	// every line up to it was delivered or dead-lettered. saved is the part of
	// it the checkpoint already records.
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

	// Deliveries run concurrently, up to Concurrency at a time, and report back
	// here: this goroutine alone owns the checkpoint and the dead-letter file.
	var (
		results  = make(chan outcome, p.o.Concurrency+1) // window + one probe: never blocks a delivery
		inflight int
		settled  = map[uint64]bool{} // lines beyond the prefix already dealt with; true = delivered
		nextSave = done + p.interval

		failed   = map[uint64]error{} // lines whose delivery kept failing: NOT settled
		dlErr    error                // a dead letter could not be written or synced
		canceled bool
		readErr  error

		before = p.successes.Load() // see giveUp
		window = p.window
		// suspect is the line that has already stalled this file MaxLineAttempts
		// times; if it fails once more it is kept (with its payload) in poison.
		suspect, rounds = p.suspectLine(path, done)
		poison          *outcome
		probed          bool // the one probe of this attempt has been sent
	)
	if suspect != 0 {
		// Leave room for one line beyond the usual window: it may have to go
		// out as a probe, to tell "the destination is down" from "it rejects
		// this message".
		window++
	}
	// Lines a previous attempt at this file delivered beyond its checkpoint.
	for n := range p.takeCarry(path) {
		if n > done {
			settled[n] = true
		}
	}
	// advance moves the prefix over the settled lines and checkpoints when due.
	advance := func() {
		for {
			if _, ok := settled[handled+1]; !ok {
				break
			}
			delete(settled, handled+1)
			handled++
		}
		if handled < nextSave || dlErr != nil {
			return
		}
		nextSave = handled + p.interval
		if err := save(); err != nil {
			if errors.Is(err, errDeadLetters) {
				dlErr = err
				return
			}
			log.Warn("cannot save checkpoint; more lines would be redelivered after a crash", "err", err)
		}
	}
	// settle takes in the outcome of one delivery.
	settle := func(o outcome) {
		inflight--
		switch {
		case o.err == nil:
			metrics.Delivered.Add(1)
			settled[o.lineNo] = true
		case sink.IsPermanent(o.err):
			log.Warn("message rejected for good by the sink", "line", o.lineNo, "err", o.err)
			if err := deadLetter(o.lineNo, o.err.Error(), *o.line); err != nil {
				dlErr = err
			} else {
				settled[o.lineNo] = false
			}
		case ctx.Err() != nil:
			canceled = true
		default:
			// The failed line is NOT settled: the prefix, and with it the
			// checkpoint, stops right before it and nothing after it is skipped
			// or deleted. Later lines that did get through are remembered (see
			// carryOver) so the next attempt does not deliver them twice.
			failed[o.lineNo] = o.err
			if o.lineNo == suspect {
				poison = &outcome{lineNo: o.lineNo, err: o.err, line: o.line}
				o.line = nil // kept: it may have to be dead-lettered
			}
		}
		if o.err == nil {
			p.successes.Add(1)
		}
		p.freeLine(o.line)
		advance()
	}
	stopping := func() bool { return len(failed) > 0 || dlErr != nil || canceled }
	// giveUp dead-letters the suspect line when it has failed again while the
	// destination accepted another message during this very attempt: fresh
	// evidence that the destination is up and it is this message it cannot
	// take. Without that evidence nothing is given up, however long it lasts.
	giveUp := func() {
		if poison == nil || dlErr != nil || canceled || p.successes.Load() == before {
			return
		}
		reason := fmt.Sprintf("gave up: the message stalled its file %d times while the destination accepted others; last error: %v", rounds+1, poison.err)
		if err := deadLetter(poison.lineNo, reason, *poison.line); err != nil {
			dlErr = err
			return
		}
		log.Error("message given up on and dead-lettered; the destination accepts other messages but not this one",
			"line", poison.lineNo, "rounds", rounds+1, "err", poison.err)
		metrics.GivenUp.Add(1)
		settled[poison.lineNo] = false
		delete(failed, poison.lineNo)
		p.freeLine(poison.line)
		poison = nil
		p.clearStall(path)
		advance()
	}

	lr := newLineReader(f, p.o.MaxLineSize)
	var lineNo uint64
	for {
		line, tooLong, terminated, rerr := lr.next()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			readErr = rerr
			break
		}
		lineNo++
		if lineNo <= done {
			handled = lineNo
			continue
		}
		if lineNo <= handled || settled[lineNo] {
			advance()
			continue // delivered by a previous attempt
		}

		// Take in whatever has finished, then hold this line until it is within
		// Concurrency of the prefix. That window is what bounds the redelivery
		// after a crash, and with Concurrency 1 it makes the file strictly
		// sequential: a line starts only once the previous one is dealt with.
		for more := true; more; {
			select {
			case o := <-results:
				settle(o)
			default:
				more = false
			}
		}
		for lineNo > handled+window && inflight > 0 {
			settle(<-results)
		}
		probing := false
		if stopping() {
			// Hear everything that is in flight before deciding: its successes
			// are the evidence giveUp needs.
			for inflight > 0 {
				settle(<-results)
			}
			giveUp()
			if stopping() {
				// The suspect failed again and nothing else went out in this
				// attempt, so nothing can be concluded yet: send this line as a
				// probe, once. If it gets through, the destination is up.
				if poison == nil || probed || len(failed) != 1 || dlErr != nil || canceled {
					break
				}
				probing = true
			}
		}
		if ctx.Err() != nil {
			canceled = true
			break
		}

		switch {
		case tooLong:
			// Too big to hold in memory, so it cannot be delivered; it is copied
			// whole from the data file instead of being cut to what was read.
			reason := fmt.Sprintf("line of %d bytes exceeds the maximum line size of %d bytes", lr.size, p.o.MaxLineSize)
			if !terminated {
				reason += "; unterminated (torn write)"
			}
			if err := bad.writeFrom(lineNo, reason, path, lr.start, lr.size); err != nil {
				dlErr = fmt.Errorf("%w: line %d: %v", errDeadLetters, lineNo, err)
			} else {
				metrics.DeadLettered.Add(1)
			}
		case !terminated:
			dlErr = deadLetter(lineNo, "unterminated line (torn write)", line)
		case !json.Valid(line):
			dlErr = deadLetter(lineNo, "invalid JSON", line)
		default:
			lp := p.copyLine(line)
			key := fmt.Sprintf("%s:%d", name, lineNo)
			probed = probed || probing
			inflight++
			go func(n uint64) {
				results <- outcome{lineNo: n, err: p.deliver(ctx, key, *lp), line: lp}
			}(lineNo)
			continue
		}
		if dlErr != nil {
			break
		}
		settled[lineNo] = false
		advance()
		if dlErr != nil {
			break
		}
	}
	for inflight > 0 {
		settle(<-results)
	}
	if stopping() {
		giveUp()
	}
	if poison != nil {
		p.freeLine(poison.line)
	}
	if stopping() || readErr != nil {
		p.carryOver(path, settled)
	}

	switch {
	case dlErr != nil:
		// The dead letter is not on disk, so its line is not handled: the
		// checkpoint stays before it and the file is tried again later.
		log.Error("cannot make dead letters durable; postponing file", "err", dlErr)
		return postpone(dlErr)
	case canceled:
		checkpoint()
		release()
		return ctx.Err()
	case len(failed) > 0:
		first := uint64(math.MaxUint64)
		for n := range failed {
			first = min(first, n)
		}
		p.noteStall(path, first)
		log.Warn("sink keeps failing; postponing file", "line", first, "err", failed[first])
		checkpoint()
		release()
		return &StallError{Line: first, Err: failed[first]}
	case readErr != nil:
		checkpoint()
		return postpone(fmt.Errorf("read: %w", readErr))
	case handled != lineNo:
		// Cannot happen: every line read is settled by now. If it ever did,
		// deleting the file would lose what lies beyond the prefix.
		checkpoint()
		return postpone(fmt.Errorf("internal error: %d of %d lines handled", handled, lineNo))
	}
	p.clearStall(path)

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

	if err := p.remove(path); err != nil {
		// Keep the checkpoint: the next attempt finds a fully-checkpointed
		// file, redelivers nothing and tries the removal again.
		log.Warn("cannot remove processed file; the removal will be retried", "err", err)
		return fmt.Errorf("%w: %v", ErrCleanupPending, err)
	}
	_ = p.remove(path + spool.CheckpointExt)
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
	if err := b.open(); err != nil {
		return err
	}
	rec := make([]byte, 0, len(reason)+len(payload)+32)
	rec = appendBadHeader(rec, lineNo, reason)
	rec = append(rec, payload...)
	rec = append(rec, '\n')
	b.dirty = true
	if _, err := b.f.Write(rec); err != nil {
		b.err = err
		return err
	}
	return nil
}

// writeFrom dead-letters the n bytes at offset off of the file src. The payload
// is streamed, so a record of any size is kept whole without being loaded.
func (b *badWriter) writeFrom(lineNo uint64, reason, src string, off, n int64) error {
	if err := b.open(); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err // nothing was written: the dead-letter file is still sound
	}
	defer func() { _ = in.Close() }()

	b.dirty = true
	if _, err = b.f.Write(appendBadHeader(nil, lineNo, reason)); err == nil {
		if _, err = io.Copy(b.f, io.NewSectionReader(in, off, n)); err == nil {
			_, err = b.f.Write([]byte{'\n'})
		}
	}
	if err != nil {
		b.err = err
	}
	return err
}

// open creates the dead-letter file on first use.
func (b *badWriter) open() error {
	if b.err != nil {
		return b.err
	}
	if b.f != nil {
		return nil
	}
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
	return nil
}

var badReasonReplacer = strings.NewReplacer("\n", " ", "\r", " ", "\t", " ")

func appendBadHeader(dst []byte, lineNo uint64, reason string) []byte {
	return fmt.Appendf(dst, "%d\t%s\t", lineNo, badReasonReplacer.Replace(reason))
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
