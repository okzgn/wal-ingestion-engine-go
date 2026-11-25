// Package metrics exposes the engine counters through the standard expvar
// registry (served by the admin endpoint at /debug/vars).
package metrics

import (
	"expvar"
	"math"
	"sync/atomic"
	"time"
)

var (
	// Accepted counts webhooks acknowledged with 200 (durably on disk).
	Accepted = expvar.NewInt("accepted")
	// Rejected counts rejected requests by reason (invalid_json, too_large, ...).
	Rejected = expvar.NewMap("rejected")

	// AckLatencyMicros / AckCount allow deriving the mean ack latency.
	AckLatencyMicros = expvar.NewInt("ack_latency_us_sum")
	AckCount         = expvar.NewInt("ack_count")

	// FsyncMicros / FsyncCount measure the cost of each group commit.
	FsyncMicros = expvar.NewInt("fsync_us_sum")
	FsyncCount  = expvar.NewInt("fsync_count")

	// BatchMessages / BatchCount give the mean group-commit batch size.
	BatchMessages = expvar.NewInt("batch_messages_sum")
	BatchCount    = expvar.NewInt("batch_count")

	FilesRotated = expvar.NewInt("files_rotated")
	// Degraded is 1 while the WAL cannot persist data.
	Degraded = expvar.NewInt("degraded")

	BacklogFiles = expvar.NewInt("backlog_files")
	BacklogBytes = expvar.NewInt("backlog_bytes")
	// HandoffPending counts rotated files that could not be moved to
	// processing/ yet and are being retried. Anything above 0 for long means
	// the data directory needs attention.
	HandoffPending = expvar.NewInt("handoff_pending")

	Delivered    = expvar.NewInt("delivered")
	Retried      = expvar.NewInt("retried")
	DeadLettered = expvar.NewInt("dead_lettered")

	// FilesPostponed counts the times a file had to be put back for later
	// because the destination kept failing. A steady climb means an outage.
	FilesPostponed = expvar.NewInt("files_postponed")
	// StalledLines is the number of files currently stuck on one message.
	StalledLines = expvar.NewInt("stalled_lines")
	// GivenUp counts messages dead-lettered by MAX_LINE_ATTEMPTS: the
	// destination accepted other messages but kept failing on these.
	GivenUp = expvar.NewInt("given_up")
)

// backlogOldest returns the age of the oldest file still waiting.
var backlogOldest atomic.Pointer[func() time.Duration]

func init() {
	// backlog_oldest_seconds is the one number to alert on: it grows while
	// messages wait, whatever the reason (slow or failing destination, a file
	// stuck on one message, a handoff that cannot complete).
	expvar.Publish("backlog_oldest_seconds", expvar.Func(func() any {
		if f := backlogOldest.Load(); f != nil {
			return math.Round((*f)().Seconds()*10) / 10
		}
		return 0.0
	}))
}

// failedBytes returns the size of the failed/ directory.
var failedBytes atomic.Pointer[func() int64]

func init() {
	// failed_bytes: dead letters and quarantined files. Only the operator
	// removes them, so a value that keeps growing needs a look.
	expvar.Publish("failed_bytes", expvar.Func(func() any {
		if f := failedBytes.Load(); f != nil {
			return (*f)()
		}
		return int64(0)
	}))
}

// SetFailedBytes installs the source of failed_bytes.
func SetFailedBytes(f func() int64) { failedBytes.Store(&f) }

// SetBacklogOldest installs the source of backlog_oldest_seconds.
func SetBacklogOldest(f func() time.Duration) { backlogOldest.Store(&f) }

// Reject records a rejected request.
func Reject(reason string) { Rejected.Add(reason, 1) }
