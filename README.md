# WAL Ingestion Engine

A webhook ingestion engine in Go built around a write-ahead log (WAL). It answers
`200 OK` **only after the message has been fsynced to disk**, then hands it to your
backend at the backend's own pace, with retries, checkpoints and crash recovery.

---

## The problem

When you receive large volumes of third-party webhooks:

1. **Your backend can't keep up.** Proxying requests straight into an API or a database exhausts pools and takes you down.
2. **In-memory queues lose data.** A restart, crash or OOM kill drops every webhook you already acknowledged.
3. **Slow acknowledgements cause retry storms.** Providers time out after 2-5 seconds and send everything again.

## The solution

- **Acknowledge fast, durably.** `200` means the JSON is valid *and* on disk (`fsync`). Anything else is an explicit `4xx` or `503`.
- **Decouple ingestion from delivery.** A fast sequential disk write on one side; a worker pool with retries on the other.
- **Survive crashes.** Interrupted files resume from a checkpoint. `kill -9` in the middle of a load loses nothing that was acknowledged.
- **Shed load honestly.** When the disk fails or the backlog is full it answers `503` + `Retry-After` instead of accepting what it cannot keep.

---

## Guarantees (and what is *not* guaranteed)

| | |
|---|---|
| **Durability** | A `200` is sent only after `write` + `fsync` of the batch succeeded. A disk error is reported to *every* message of the batch as `503`; it is never acknowledged. |
| **Delivery** | **At-least-once.** After a crash up to `CHECKPOINT_INTERVAL - 1` lines per in-flight file can be delivered again, and retries can repeat a message. |
| **Deduplication** | Every delivery carries a deterministic `Idempotency-Key: <file>:<line>`, identical on every attempt. Your backend should deduplicate on it. |
| **Ordering** | **None across files** with `WORKERS > 1`. With `WORKERS=1` files are delivered in arrival order while the backend is healthy; a postponed file is retried later, behind newer ones. |
| **No silent loss** | Invalid, oversized or torn lines are never dropped silently: they go to `failed/<file>.bad`. A file that still holds undelivered messages is never deleted. |
| **Not provided** | Replication (the disk is the single point of failure), TLS (put a reverse proxy in front), exactly-once delivery. Durability is as good as your disk's `fsync`; check that the write cache is not lying. |

---

## Architecture

```text
              HTTP webhook requests
                       │
                       ▼
      ┌────────────────────────────────────┐
      │  Fiber server                      │   400 empty/invalid JSON   401 bad signature
      │  validate · HMAC · compact to      │   413 too large            415 Content-Encoding
      │  one line · admission control      │   429 rate limit           503 not durable / full
      └────────────────┬───────────────────┘
                       │ Append(): blocks until durable
                       ▼
      ┌────────────────────────────────────┐
      │  WAL writer (1 goroutine)          │   group commit: gather whatever is queued,
      │  one write + one fsync per batch   │   one write(2), one fsync(2), ack everyone
      │  degraded mode on disk errors      │   rotate at MAX_FILE_SIZE / ROTATION_INTERVAL
      └────────────────┬───────────────────┘
                       │ rotated file: rename + dir fsync
                       ▼
       ingestions/ ──────────► processing/ ──► dispatcher (FIFO, no duplicates, backlog)
                                                   │
                                                   ▼
      ┌────────────────────────────────────┐
      │  Worker pool (WORKERS)             │   per line: validate → Sink.Deliver with
      │  line reader · retries · checkpoint│   retries; checkpoint every N lines
      └───────┬───────────────┬────────────┘
              │ ok            │ rejected line (invalid / oversize / permanent 4xx)
              ▼               ▼
        file + ckpt       failed/<file>.bad
        deleted           sink down → file kept, retried later with backoff
```

### Directory lifecycle

| Path (under `DATA_DIR`) | Contents |
| :--- | :--- |
| `ingestions/` | The active WAL file being written and synced. |
| `processing/*.list` | Rotated files waiting for or being processed by a worker. |
| `processing/*.list.ckpt` | Checkpoint: how many lines of that file are fully handled. Two checksummed slots, overwritten in place; a torn write can never lose the previous value. |
| `failed/<file>.bad` | Dead letters, one per line: `<line number>⇥<reason>⇥<payload>`. |
| `failed/<file>.list` | Files given up on after `MAX_FILE_ATTEMPTS` postponements (off by default). |
| `spool.lock` | Instance lock: a second process on the same `DATA_DIR` refuses to start. |

On start the engine recovers sequentially, before accepting traffic: sweeps orphan checkpoints, moves any
leftover `ingestions/*.list` into `processing/`, and queues everything in chronological order.

---

## HTTP contract

`POST /webhook` (path configurable) with a JSON body.

| Status | Meaning | Sender should |
| :---: | :--- | :--- |
| `200` | Valid JSON, **durably stored**. | Consider it delivered. |
| `400` | Empty body or not valid JSON. | Fix the request; do not retry as is. |
| `401` | Missing/invalid HMAC signature or stale timestamp. | Check the secret. |
| `405` / `404` | Wrong method / path. | Fix the request. |
| `413` | Body larger than `MAX_MESSAGE_SIZE`. | Send less. |
| `415` | `Content-Encoding` present (compression is refused: it would bypass the size limit and break signatures). | Send it uncompressed. |
| `429` | Per-IP rate limit exceeded. | Back off. |
| `503` + `Retry-After` | **Not stored**: disk failing, queue/in-flight/backlog limit hit, ack timeout, or shutting down. | Retry later. |

Pretty-printed JSON is accepted: each message is compacted to a single line before it is stored.

---

## Getting started

Requires **Go 1.26+**.

```bash
git clone https://github.com/okzgn/wal-ingestion-engine-go.git
cd wal-ingestion-engine-go
go run ./cmd/ingestor            # listens on :8080, data in ./data
```

```bash
curl -i -X POST http://localhost:8080/webhook \
     -H "Content-Type: application/json" \
     -d '{"event": "payment_received", "amount": 100, "customer_id": "cust_12345"}'
```

Forward messages to your backend and require signatures:

```bash
SINK_URL=http://localhost:9000/events WEBHOOK_SECRET=change-me go run ./cmd/ingestor
```

```bash
BODY='{"event":"ping"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac change-me | sed 's/^.* //')
curl -i -X POST http://localhost:8080/webhook -H "X-Signature-256: sha256=$SIG" -d "$BODY"
```

Health and metrics (admin listener, localhost only by default):

```bash
curl localhost:9090/healthz      # process is alive
curl localhost:9090/readyz       # 503 if the WAL is degraded or the backlog is full
curl localhost:9090/debug/vars   # accepted, rejected{reason}, fsync_*, batch_*, backlog_*, delivered, retried, dead_lettered, degraded
```

---

## Configuration

Every setting is an environment variable and, except the secret, a flag (`-workers 2`).
**Precedence: flag > environment > default.** `ingestor -h` lists them all. Invalid combinations
are rejected at startup with a message naming the setting.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LISTEN_ADDR` | `:8080` | Webhook listener. |
| `DATA_DIR` | `./data` | Root of `ingestions/`, `processing/`, `failed/`. |
| `ENDPOINT` | `/webhook` | Webhook path. |
| `MAX_MESSAGE_SIZE` | `1MiB` | Largest body; also the HTTP body limit (`413`). |
| `MAX_FILE_SIZE` / `ROTATION_INTERVAL` | `1MiB` / `10s` | Rotate the WAL file at this size, or after this long if non-empty. |
| `MAX_BATCH_SIZE` / `MAX_BATCH_BYTES` | `256` / `1MiB` | Upper bounds of one group commit. |
| `COMMIT_LINGER` | `0s` | Optional extra wait to gather a bigger batch (`0` = commit immediately). |
| `QUEUE_SIZE` | `4096` | Capacity of the writer queue. |
| `MAX_INFLIGHT_BYTES` | `64MiB` | `503` above this much data waiting for the disk. |
| `MAX_BACKLOG_BYTES` | `1GiB` | `503` above this much unprocessed data on disk (`0` = no limit). |
| `WORKERS` | `4` | Concurrent file workers (`1` = in-order delivery). |
| `CHECKPOINT_INTERVAL` | `50` | Lines between checkpoint saves. |
| `SINK_URL` | *(empty)* | Forward each message here with `POST`. Empty = store and discard. |
| `SINK_TIMEOUT` | `10s` | Timeout of one delivery request. |
| `SINK_MAX_ATTEMPTS` | `5` | Attempts per message before the file is postponed. |
| `SINK_BACKOFF_BASE` / `SINK_BACKOFF_MAX` | `200ms` / `30s` | Per-message retry backoff (exponential, jittered). |
| `FILE_RETRY_BASE` / `FILE_RETRY_MAX` | `1s` / `1m` | Backoff before a postponed file is retried. |
| `MAX_FILE_ATTEMPTS` | `0` | Move a file to `failed/` after this many postponements (`0` = never; the backlog limit sheds load instead). |
| `WEBHOOK_SECRET` | *(empty)* | Enables HMAC-SHA256 verification. Environment only, so it never shows up in a process list. |
| `WEBHOOK_SIGNATURE_HEADER` | `X-Signature-256` | Header carrying `sha256=<hex>`. |
| `WEBHOOK_TIMESTAMP_HEADER` | *(empty)* | If set, the MAC covers `<ts>.<body>` and the timestamp must be fresh (anti-replay). |
| `WEBHOOK_TOLERANCE` | `5m` | Accepted clock skew for the timestamp. |
| `RATE_LIMIT_PER_MINUTE` | `0` | Per-IP limit (`0` = off). |
| `READ_TIMEOUT` / `WRITE_TIMEOUT` / `IDLE_TIMEOUT` | `10s` / `15s` / `60s` | HTTP timeouts. Idle keep-alive connections are closed, so they cannot hold slots or block shutdown. |
| `ACK_TIMEOUT` | `5s` | Max wait for the disk before answering `503`. Must be below `WRITE_TIMEOUT`. |
| `MAX_CONNECTIONS` | `10000` | Max concurrent HTTP connections. |
| `SHUTDOWN_TIMEOUT` | `15s` | Grace period at shutdown. |
| `ADMIN_ADDR` | `127.0.0.1:9090` | Health/metrics listener (empty = disabled). |
| `ADMIN_PPROF` | `false` | Expose `/debug/pprof` on the admin listener. |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | `debug`/`info`/`warn`/`error`; `text` or `json` (structured, via `log/slog`). |

---

## Integrating your backend

Messages are delivered through the `Sink` interface (`internal/sink`):

```go
type Sink interface {
    Deliver(ctx context.Context, key string, line []byte) error
}
```

* Return `nil` on success.
* Return an error wrapped in `*sink.PermanentError` when retrying cannot help: the line goes to `failed/<file>.bad` and processing continues.
* Return any other error for a transient failure: it is retried with backoff; if it persists the file is kept and retried later. **Nothing after the failed message is skipped or deleted.**
* `key` is `<file>:<line>`, deterministic: use it to deduplicate.

The built-in `HTTPSink` (`SINK_URL`) does a `POST` with `Content-Type: application/json` and `Idempotency-Key`.
`2xx` is success; `408`, `425`, `429`, `5xx` and network errors are transient; every other status is permanent.

### Replaying dead letters

```bash
# payload = third tab-separated field
cut -f3- data/failed/ingest_....list.bad | while IFS= read -r line; do
  curl -s -X POST localhost:8080/webhook -d "$line"
done
```

---

## Deployment

**systemd** (`deploy/wal-ingestor.service`): hardened unit with `LimitNOFILE=65535`, `KillSignal=SIGTERM`
and `TimeoutStopSec=30` (longer than `SHUTDOWN_TIMEOUT`). Keep the secret in `/etc/wal-ingestor/wal-ingestor.env` (`chmod 600`).

```bash
go build -o /usr/local/bin/wal-ingestor ./cmd/ingestor
sudo cp deploy/wal-ingestor.service /etc/systemd/system/
sudo systemctl enable --now wal-ingestor
```

**Docker** (distroless, non-root, built-in `healthcheck` subcommand). Mount a volume on `/data`:

```bash
docker build -t wal-ingestion-engine .
docker run -p 8080:8080 -v wal-data:/data -e SINK_URL=http://host.docker.internal:9000/events wal-ingestion-engine
```

### Graceful shutdown

On `SIGINT`/`SIGTERM` the engine, in this order: stops accepting connections and lets in-flight requests finish
and get their ACK → commits and closes the WAL → stops the workers, which save their checkpoint. Files not
finished stay in `processing/` and resume on the next start; the final log line reports how many.
A `SIGKILL` is also safe, only less tidy.

---

## Tuning

* **Throughput is bounded by `fsync` latency, not by the HTTP stack.** The group commit amortises one `fsync` across every request that arrived meanwhile, so more concurrency means bigger batches, not slower ones.
* **Slow backend?** Raise `WORKERS` first. If it is down for long, `MAX_BACKLOG_BYTES` turns the growing backlog into `503`s instead of filling the disk; the sender retries.
* **Many connections:** raise the file descriptor limit (`ulimit -n 65535`, or `LimitNOFILE` in systemd).
* **Benchmarks** (see [`docs/benchmarks.md`](docs/benchmarks.md)): on the development machine, 1 connection went from 10 to ~1,750 req/s and 64 connections from 632 to ~37,700 req/s versus v1.

---

## Migrating from v1

| v1 | v2 |
| :--- | :--- |
| `go run .` (root `ingestor.go`) | `go run ./cmd/ingestor` |
| Port `80`, constants in the source | `LISTEN_ADDR=:8080` by default; everything is configurable. Use `LISTEN_ADDR=:80` for the old port. |
| `./ingestions`, `./processing`, `./failed` in the working directory | Under `DATA_DIR` (`./data`). Use `DATA_DIR=.` to keep the old layout. |
| `*.list.done` text checkpoints | `*.list.ckpt` binary checkpoints. Leftover v1 files are processed anyway (the line format is unchanged); an old checkpoint is ignored, so such a file is reprocessed from its start, which is safe under at-least-once. |
| Backend error → rest of the file deleted | Backend error → file kept, retried with backoff. |
| Any body accepted with `200` | Invalid JSON `400`, oversized `413`. |

---

## Development

```bash
make test          # unit + end-to-end tests
make test-short    # skip the end-to-end tests
make race          # with the race detector (needs a C toolchain)
make bench         # WAL micro-benchmarks
make lint          # golangci-lint
```

The end-to-end tests (`internal/e2e`) build the real binary, point it at a fake backend and `kill -9` it
mid-load, then check that every acknowledged message reached the backend. `docs/regressions.md` lists the
bugs of v1 and the tests that now cover them. `tools/loadgen` is the load generator used for the benchmarks.

---

## License

MIT License.

Copyright © 2026 [OKZGN](https://okzgn.com)
