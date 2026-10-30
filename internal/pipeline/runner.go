package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

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

	mu       sync.Mutex
	attempts map[string]int
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
		default:
			n := r.bump(path)
			if r.MaxFileAttempts > 0 && n >= r.MaxFileAttempts {
				r.Logger.Error("giving up on file; moving it to failed/", "file", path, "attempts", n, "err", err)
				if qerr := r.Spool.Quarantine(path); qerr != nil {
					r.Logger.Error("cannot quarantine file", "file", path, "err", qerr)
					r.Spool.Disp.Requeue(path, r.RetryMax)
					continue
				}
				r.forget(path)
				continue
			}
			delay := Backoff(n, r.RetryBase, r.RetryMax)
			r.Logger.Warn("file postponed", "file", path, "attempt", n, "retry_in", delay, "err", err)
			r.Spool.Disp.Requeue(path, delay)
		}
	}
}

func (r *Runner) bump(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts[path]++
	return r.attempts[path]
}

func (r *Runner) forget(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.attempts, path)
}
