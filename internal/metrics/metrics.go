// Package metrics exposes the engine counters through the standard expvar
// registry (served by the admin endpoint at /debug/vars).
package metrics

import "expvar"

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

	Delivered    = expvar.NewInt("delivered")
	Retried      = expvar.NewInt("retried")
	DeadLettered = expvar.NewInt("dead_lettered")
)

// Reject records a rejected request.
func Reject(reason string) { Rejected.Add(reason, 1) }
