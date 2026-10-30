# Changelog

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
