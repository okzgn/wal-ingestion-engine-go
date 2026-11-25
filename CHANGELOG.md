# Changelog

## v2.2.0

A second review, this time with measurements against the real binary: one way left to lose an
acknowledged message, a delivery path far slower than ingestion, and several places where the
engine waited for a restart or for an operator instead of recovering by itself.

**Two defaults change behaviour** (see "Changed"): messages of a file are delivered concurrently,
and a message the backend keeps failing on is eventually dead-lettered.

### Fixed: data loss

* `MAX_MESSAGE_SIZE` above 16 MiB acknowledged messages that the delivery side then cut to their
  first 16 MiB in `failed/`. The setting is now capped at 16 MiB at startup, and an oversized line is
  never truncated again: it is streamed whole to `failed/<file>.bad`, whatever its size.

### Fixed: reliability

* A rotated file that could not be moved to `processing/` stayed in `ingestions/` until the next
  restart, undelivered and invisible to the backlog limit. The move is now retried in the background
  with backoff, in rotation order, and the file counts as backlog meanwhile (`handoff_pending`).
* A processed file that could not be removed left the backlog while staying on disk. Its removal is
  now retried, without redelivering anything and without counting as a failed attempt.
* `MAX_FILE_ATTEMPTS` counted every postponement, so a file that advanced between failures during a
  long, flapping outage ended up quarantined. It now counts consecutive failures on the same message.
* A body larger than the limit was refused by closing the connection while the client was still
  sending, which the client often saw as a reset instead of the `413`. The refused body is now read
  and discarded (bounded) before the connection is closed.
* A file is never deleted unless every line read from it is accounted for (a defensive check).

### Changed

* **`SINK_CONCURRENCY` (default 8): the messages of a file are delivered concurrently.** A file used
  to go out one message at a time, so the drain rate was `WORKERS ÷ backend latency` whatever the
  load: 160 msg/s against a 20 ms backend while ingesting 25,000 req/s. It is now ~1,100 msg/s with
  the defaults. The checkpoint is still a contiguous prefix of the file, and a transient failure in
  the middle of the window repeats nothing while the process lives. After a crash up to
  `CHECKPOINT_INTERVAL - 1 + SINK_CONCURRENCY` lines per in-flight file can be delivered again (it was
  `CHECKPOINT_INTERVAL - 1`). **In-order delivery now needs `WORKERS=1` and `SINK_CONCURRENCY=1`.**
  A custom `Sink` must be safe for concurrent use.
* **`MAX_LINE_ATTEMPTS` (default 10): a message that keeps stalling its file is given up on.** A
  message the backend answered `500` to every time stalled its file for ever. After it has stalled the
  file that many times in a row, the next failure dead-letters it (reason `gave up: ...`), but only if
  another message got through in that same attempt; one line is sent as a probe when needed. A backend
  that is down never dead-letters anything. `MAX_LINE_ATTEMPTS=0` restores the old behaviour.
* Request bodies are read by the handler, after being admitted against `MAX_BODY_MEMORY` (default
  256 MiB), instead of being buffered by the server before any check: up to `MAX_CONNECTIONS ×
  MAX_MESSAGE_SIZE` (10 GB with the defaults) could be held before a single signature was verified.
  A chunked body is cut off one byte past the limit. Ingestion throughput is unchanged.
* `WEBHOOK_SIGNATURE_HEADER` defaults to the header of the chosen scheme (still `X-Signature-256`
  for `generic`).
* Boolean flags work without a value (`-admin-pprof`). A size that overflows is reported instead of
  wrapping around.

### Added

* `WEBHOOK_SIGNATURE_SCHEME`: `github`, `stripe` and `slack` besides `generic`, each with its own
  headers, signed payload and (stripe, slack) timestamp tolerance. Checked against the documented
  examples of GitHub and Slack.
* `SINK_KEY_JSON_POINTER`: the sender's own event id becomes the `Idempotency-Key`, so the backend
  can also discard an event the sender delivered twice. `X-Delivery-Id` always carries the engine key.
* `MAX_FAILED_BYTES`: shed load while `failed/` is over a limit (off by default).
* Metrics to alert on: `backlog_oldest_seconds`, `files_postponed`, `stalled_lines`, `given_up`,
  `failed_bytes`, `handoff_pending`.
* Windows: renames between spool directories are written through to disk.
* Tests: a power-loss simulation that rebuilds the disk from what had been fsynced; the
  graceful-shutdown end-to-end test now also runs on Windows; `fsutil` is covered.
  `loadgen -sink-addr` runs a backend with a fixed latency and reports the drain rate.
* `SECURITY.md`; Dependabot for Go modules, CI actions and Docker images.

### Security

* Go 1.26.11 is the minimum toolchain: 1.26.10 carries GO-2026-6859 (`net/http`), reachable from the
  sink client and the admin listener. Dependencies upgraded (Fiber v3.7.0, fasthttp v1.80.0).
* CI actions are pinned to commits and `govulncheck` to a version.

## v2.1.0

Production-readiness review: two bugs that broke the documented guarantees, a vulnerable
toolchain, the CI that v2.0.0 announced but did not ship, and a round of operational hardening.

### Fixed: data loss

* The HTTP sink no longer follows redirects. A `301`/`302`/`303` turned the `POST` into a body-less
  `GET`, and a `200` on that `GET` counted as a delivery the destination never received. A `3xx` is now
  a transient error until `SINK_URL` is fixed.
* A dead letter that cannot be written or fsynced no longer vanishes. The data file is kept, the
  checkpoint stays before the rejected line and the file is retried, instead of being deleted after a
  warning.
* A line longer than the current `MAX_MESSAGE_SIZE` is still delivered whole: the reader has its own
  16 MiB limit. Lowering the limit with a backlog on disk used to truncate those messages to a 4 KiB
  preview in `failed/`. A line above the reader's limit keeps its first 16 MiB in the dead letter.

### Changed

* `401`, `403`, `404`, `405` and every other non-payload failure from the destination are transient:
  a credential rotation or a deploy no longer dead-letters a whole backlog. Only `400`, `413`, `415`
  and `422` are permanent by default (`SINK_PERMANENT_STATUSES`).
* An environment variable set to the empty string counts as set: `ADMIN_ADDR=` disables the admin
  listener, as documented.
* Listeners are bound before the data directory is locked, so a busy port fails fast and cleanly.
* The shutdown sequence fits within `SHUTDOWN_TIMEOUT`; it could overrun it by up to 3 s.
* Data directories are created `0700` and WAL, checkpoint and dead-letter files `0600`.
* `/debug/vars` no longer publishes `cmdline`. The admin server has read and idle timeouts.
* The rename and directory fsyncs of a rotation run on their own goroutine, in order, so they never
  delay the next group commit; only the directories that changed are fsynced.
* Repeated `message not persisted` / `request failed` log lines are throttled to one per second, with
  a count of the suppressed ones.
* The sink keeps as many idle connections as there are workers and honours `HTTP_PROXY`; the backlog
  byte counter is read without a lock; invalid JSON is refused with `400` even while the backlog is
  full; JSON is parsed once per request instead of twice.

### Added

* `SINK_AUTHORIZATION` and `SINK_HEADERS` (environment only) for destinations that need credentials;
  credentials inside `SINK_URL` are rejected because they would end up in logs.
* `TRUSTED_PROXIES` / `PROXY_HEADER`: behind a reverse proxy the per-client rate limit keyed on the
  proxy's IP, which made it a global limit. `X-Forwarded-For` is only believed from the listed proxies.
* GitHub Actions CI: gofmt, vet, race tests on Linux and Windows, golangci-lint, govulncheck and the
  Docker build. `make check` runs the same locally.
* A regression test for every item above (`docs/regressions.md`).

### Security

* Go 1.26.10 is the minimum toolchain: 1.26.0 had 28 known standard-library vulnerabilities reachable
  from this code. Dependencies upgraded (Fiber v3.6.1).
* The systemd unit gains the usual sandboxing directives, a `0700` state directory and `UMask=0077`.

## v2.0.0

A hardening release. It fixes every way v1 could acknowledge or delete data it had not delivered.
**Breaking changes:** see "Migrating from v1" in the README (entry point, default port, data
directory layout, checkpoint format, Go module path, Go 1.26).

### Fixed: data loss

* A downstream error no longer deletes the rest of the file. The checkpoint stays *before* the failed
  line, the file is kept and retried with backoff.
* Pretty-printed (multi-line) JSON is compacted to one line instead of being acknowledged and dropped.
  Empty and invalid bodies are `400`, oversized bodies `413`; none is acknowledged with `200` any more.
* A line larger than the reader's limit no longer aborts its whole file, and no code path truncates a
  data file (`os.Create` on the failure path is gone).
* Disk errors (`write`/`fsync`) are propagated: every message of the batch gets `503` instead of `200`.
* Invalid, oversized or torn lines go to `failed/<file>.bad` instead of the log only.

### Fixed: reliability

* Failing to create the next WAL file degrades the writer (`503`, `/readyz` fails) and recovers by
  itself, instead of `log.Fatalf`.
* Startup recovery is sequential and queue entries are deduplicated: no file can be processed twice
  concurrently. An instance lock prevents two processes sharing a data directory.
* Checkpoints are two checksummed slots; a torn write can't lose progress or skip lines.
* Shutdown is bounded (`SHUTDOWN_TIMEOUT`), workers checkpoint on cancel, HTTP idle connections time out,
  and the final log line no longer claims "without data loss" regardless of the situation.
* Directory entries are fsynced after creating or renaming WAL files (Unix).
* File names sort chronologically across restarts; leaked handles and orphan checkpoints are gone.

### Added

* HMAC-SHA256 signature verification with optional anti-replay timestamp; per-IP rate limit; refusal of
  compressed bodies (decompression bombs).
* Real backpressure: in-flight byte budget, backlog byte limit, `Retry-After`.
* `Sink` interface with a `Noop` and an `HTTP` forwarder (`Idempotency-Key`, transient/permanent
  classification), per-message retries with exponential backoff and jitter.
* Configuration through validated environment variables and flags.
* Admin listener: `/healthz`, `/readyz`, `/debug/vars` (expvar metrics), opt-in pprof. Structured logging
  with `log/slog`.
* Dockerfile (distroless, non-root, `healthcheck` subcommand), hardened systemd unit, Makefile, CI
  (gofmt, vet, race tests on Linux and Windows, golangci-lint, govulncheck).
* Unit tests per package, a fuzz test, and end-to-end tests that crash the real binary with `kill -9`.

### Performance

* Adaptive group commit (no more 100 ms flush ticker, one `write` + one `fsync` per batch): single
  connection 10 → ~1,750 req/s (p50 100 ms → 0.5 ms), 64 connections 632 → ~37,700 req/s.
  See `docs/benchmarks.md`.

### Security

* Dependencies upgraded (`golang.org/x/net`, `x/text`, `x/crypto`, `x/sys`, `fasthttp`): no known
  module vulnerabilities per `govulncheck`.

## v1.1

Initial public version (single `ingestor.go`).
