// Package logutil holds small logging helpers shared by the engine packages.
package logutil

import (
	"sync"
	"time"
)

// Throttle rate-limits a log line that can repeat thousands of times per
// second, such as "message not persisted" while the disk is failing: it lets
// one through per interval and reports how many were suppressed meanwhile, so
// the log keeps the information without the flood.
type Throttle struct {
	interval time.Duration
	now      func() time.Time

	mu         sync.Mutex
	next       time.Time
	suppressed int64
}

// NewThrottle allows one call per interval.
func NewThrottle(interval time.Duration) *Throttle {
	return &Throttle{interval: interval, now: time.Now}
}

// Allow reports whether the caller may log now and, if so, how many calls were
// suppressed since the previous allowed one.
func (t *Throttle) Allow() (ok bool, suppressed int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if now.Before(t.next) {
		t.suppressed++
		return false, 0
	}
	t.next = now.Add(t.interval)
	suppressed, t.suppressed = t.suppressed, 0
	return true, suppressed
}
