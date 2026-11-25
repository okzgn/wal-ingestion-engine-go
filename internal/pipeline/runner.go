package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

// Runner is the worker pool: it pulls files from the spool dispatcher and runs
// them through a Processor, rescheduling the ones that must be retried.
type Runner struct {
	Spool   *spool.Spool
	Proc    *Processor
	Workers int

	// MaxFileAttempts moves a file to failed/ after that many postponements.
	// 0 (default) never gives up: the backlog limit sheds load instead.
	MaxFileAttempts int
	RetryBase       time.Duration // first delay before a postponed file is retried
	RetryMax        time.Duration // cap of that delay
	Logger          *slog.Logger

	mu        sync.Mutex
	attempts  map[string]int
	cleanups  map[string]int    // rounds spent only retrying the removal of a file
	stalledAt map[string]uint64 // line each file was last stuck on
}

// Run starts the workers and blocks until ctx is cancelled and every worker has
// finished its current line and checkpointed.
func (r *Runner) Run(ctx context.Context) {
	if r.Workers <= 0 {
		r.Workers = 1
	}
	if r.RetryBase <= 0 {
		r.RetryBase = time.Second
	}
	if r.RetryMax <= 0 {
		r.RetryMax = time.Minute
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	r.attempts = map[string]int{}
	r.cleanups = map[string]int{}
	r.stalledAt = map[string]uint64{}

	var wg sync.WaitGroup
	for i := 0; i < r.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.worker(ctx)
		}()
	}
	wg.Wait()
}

func (r *Runner) worker(ctx context.Context) {
	for {
		path, ok := r.Spool.Disp.Next(ctx)
		if !ok {
			return
		}
		err := r.Proc.ProcessFile(ctx, path)
		switch {
		case err == nil:
			r.Spool.Disp.Done(path)
			r.forget(path)
		case ctx.Err() != nil || errors.Is(err, context.Canceled):
			// Shutting down: the file stays in processing/ with its checkpoint.
			return
		case errors.Is(err, ErrCleanupPending):
			// Everything was delivered; only the removal is left. It is retried
			// without counting as a failed attempt: a delivered file must never
			// end up quarantined in failed/.
			delay := Backoff(r.bumpCleanup(path), r.RetryBase, r.RetryMax)
			r.Logger.Warn("processed file not removed yet", "file", path, "retry_in", delay, "err", err)
			r.Spool.Disp.Requeue(path, delay)
		default:
			metrics.FilesPostponed.Add(1)
			n := r.bump(path, err)
			if r.MaxFileAttempts > 0 && n >= r.MaxFileAttempts {
				r.Logger.Error("giving up on file; moving it to failed/", "file", path, "attempts", n, "err", err)
				if qerr := r.Spool.Quarantine(path); qerr != nil {
					r.Logger.Error("cannot quarantine file", "file", path, "err", qerr)
					r.Spool.Disp.Requeue(path, r.RetryMax)
					continue
				}
				r.forget(path)
				r.Proc.Forget(path)
				continue
			}
			delay := Backoff(n, r.RetryBase, r.RetryMax)
			r.Logger.Warn("file postponed", "file", path, "attempt", n, "retry_in", delay, "err", err)
			r.Spool.Disp.Requeue(path, delay)
		}
	}
}

// bump counts one more failed attempt at a file. A file that is stuck on a
// different line than last time has made progress in between, so it starts
// over: MAX_FILE_ATTEMPTS is about a file that cannot advance, not about one
// that lives through a long, flapping outage.
func (r *Runner) bump(path string, err error) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var se *StallError
	if errors.As(err, &se) {
		if last, ok := r.stalledAt[path]; ok && last != se.Line {
			r.attempts[path] = 0
		}
		r.stalledAt[path] = se.Line
	}
	r.attempts[path]++
	return r.attempts[path]
}

func (r *Runner) bumpCleanup(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups[path]++
	return r.cleanups[path]
}

func (r *Runner) forget(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.attempts, path)
	delete(r.cleanups, path)
	delete(r.stalledAt, path)
}
