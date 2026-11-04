package spool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
)

// Dispatcher is a FIFO of files waiting for a worker. It never blocks
// producers, rejects duplicates (a file that is queued, delayed or in flight
// cannot be added again) and tracks the on-disk backlog.
type Dispatcher struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []string
	known  map[string]int64 // path -> size, for queued, delayed and in-flight files
	bytes  atomic.Int64     // read lock-free by every webhook request
	timers map[*time.Timer]struct{}
	closed bool
}

// NewDispatcher creates an empty dispatcher.
func NewDispatcher() *Dispatcher {
	d := &Dispatcher{known: map[string]int64{}, timers: map[*time.Timer]struct{}{}}
	d.cond = sync.NewCond(&d.mu)
	return d
}

// Add enqueues path. It reports false when the file is already known.
func (d *Dispatcher) Add(path string, size int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	if _, dup := d.known[path]; dup {
		return false
	}
	d.known[path] = size
	d.bytes.Add(size)
	d.queue = append(d.queue, path)
	d.publish()
	d.cond.Signal()
	return true
}

// Next blocks until a file is available or ctx is cancelled / the dispatcher closed.
func (d *Dispatcher) Next(ctx context.Context) (string, bool) {
	stop := context.AfterFunc(ctx, func() {
		d.mu.Lock()
		d.cond.Broadcast()
		d.mu.Unlock()
	})
	defer stop()

	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.queue) == 0 {
		if d.closed || ctx.Err() != nil {
			return "", false
		}
		d.cond.Wait()
	}
	if ctx.Err() != nil {
		return "", false
	}
	path := d.queue[0]
	d.queue = d.queue[1:]
	return path, true
}

// Done marks a file as finished (deleted or moved away): it leaves the backlog.
func (d *Dispatcher) Done(path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if size, ok := d.known[path]; ok {
		d.bytes.Add(-size)
		delete(d.known, path)
	}
	d.publish()
}

// Requeue puts an in-flight file back after delay. The file stays in the
// backlog meanwhile.
func (d *Dispatcher) Requeue(path string, delay time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		delete(d.timers, t)
		if d.closed {
			return
		}
		d.queue = append(d.queue, path)
		d.cond.Signal()
	})
	d.timers[t] = struct{}{}
}

// BacklogBytes is the total size of files not yet fully processed. It is
// called on every request, so it does not take the lock.
func (d *Dispatcher) BacklogBytes() int64 { return d.bytes.Load() }

// BacklogFiles is the number of files not yet fully processed.
func (d *Dispatcher) BacklogFiles() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.known)
}

// Close wakes all waiters and drops pending retries. Files stay on disk in
// processing/ and are picked up again by the next Recover.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for t := range d.timers {
		t.Stop()
	}
	d.timers = map[*time.Timer]struct{}{}
	d.cond.Broadcast()
}

// publish mirrors the backlog into the metrics. Callers hold d.mu.
func (d *Dispatcher) publish() {
	metrics.BacklogFiles.Set(int64(len(d.known)))
	metrics.BacklogBytes.Set(d.bytes.Load())
}
