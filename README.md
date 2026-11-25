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
| **Delivery** | **At-least-once.** After a crash up to `CHECKPOINT_INTERVAL - 1 + SINK_CONCURRENCY` lines per in-flight file can be delivered again. While the process lives, a failed delivery repeats nothing but itself. |
| **Deduplication** | Every delivery carries `X-Delivery-Id: <file>:<line>`, which names one stored message and is identical on every attempt; by default it is also the `Idempotency-Key`. It covers the engine's own redeliveries, **not** a sender that delivers the same event twice (after a timeout or a `503` it retries, and the retry is a new message). For that, point `SINK_KEY_JSON_POINTER` at the sender's event id: see [Deduplicating](#deduplicating). |
| **Ordering** | **None by default.** Files are processed in parallel (`WORKERS`) and so are the messages of a file (`SINK_CONCURRENCY`). With `WORKERS=1` and `SINK_CONCURRENCY=1` messages are delivered in arrival order while the backend is healthy; a postponed file is retried later, behind newer ones. |
| **No silent loss** | Invalid, oversized or torn lines are never dropped silently: they go **whole** to `failed/<file>.bad`, and a file is deleted only once its dead letters are fsynced and every line it holds is accounted for; if `failed/` cannot be written the file is kept and retried. After a crash or a postponed file, dead letters since the last checkpoint can appear twice: deduplicate on the line number. |
| **Giving up** | Only on evidence. A message the backend rejects with a permanent status is dead-lettered at once. One it keeps failing on with a transient error is dead-lettered after it has stalled its file `MAX_LINE_ATTEMPTS` times **and** the backend accepted another message in the same attempt. A backend that is down never dead-letters anything, however long it lasts. |
| **Not provided** | Replication (the disk is the single point of failure), TLS (put a reverse proxy in front), exactly-once delivery. Durability is as good as your disk's `fsync`; check that the write cache is not lying. On Windows there is no directory fsync: renames are written through instead (`MOVEFILE_WRITE_THROUGH`). |

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
      │  Worker pool (WORKERS files)       │   per line: validate → Sink.Deliver with
      │  SINK_CONCURRENCY lines at a time  │   retries; the checkpoint is the contiguous
      │  line reader · retries · checkpoint│   prefix of the file that is dealt with
      └───────┬───────────────┬────────────┘
              │ ok            │ rejected line (invalid / oversize / permanent 4xx / given up on)
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

Directories are created `0700` and files `0600`: they hold whatever the webhooks carried.

On start the engine recovers sequentially, before accepting traffic: sweeps orphan checkpoints, moves any
leftover `ingestions/*.list` into `processing/`, and queues everything in chronological order.

---

## HTTP contract

`POST /webhook` (path configurable) with a JSON body.

| Status | Meaning | Sender should |
| :---: | :--- | :--- |
| `200` | Valid JSON, **durably stored**. | Consider it delivered. |
| `400` | Empty body or not valid JSON. | Fix the request; do not retry as is. |
| `401` | Missing/invalid signature or stale timestamp. | Check the secret and the scheme. |
| `405` / `404` | Wrong method / path. | Fix the request. |
| `413` | Body larger than `MAX_MESSAGE_SIZE`, declared or chunked. | Send less. |
| `415` | `Content-Encoding` present (compression is refused: it would bypass the size limit and break signatures). | Send it uncompressed. |
| `429` | Per-client rate limit exceeded (behind a reverse proxy set `TRUSTED_PROXIES`, or the client is the proxy). | Back off. |
| `503` + `Retry-After` | **Not stored**: disk failing, a limit hit (queue, in-flight, body memory, backlog, `failed/`), ack timeout, or shutting down. The `reason` field of the answer says which. | Retry later. |

Pretty-printed JSON is accepted: each message is compacted to a single line before it is stored.
After a `413` or a `503` that refuses the body, the connection is closed.

### Verifying signatures

`WEBHOOK_SECRET` turns verification on; `WEBHOOK_SIGNATURE_SCHEME` says how the sender signs. Every
scheme is HMAC-SHA256 over the **raw** body.

| Scheme | Header (default) | Signed payload | Replay protection |
| :--- | :--- | :--- | :--- |
| `generic` (default) | `X-Signature-256: sha256=<hex>` | body, or `<ts>.<body>` with `WEBHOOK_TIMESTAMP_HEADER` | only with the timestamp header |
| `github` | `X-Hub-Signature-256: sha256=<hex>` | body | none (GitHub sends no timestamp) |
| `stripe` | `Stripe-Signature: t=<ts>,v1=<hex>` | `<t>.<body>`; any of several `v1` | `WEBHOOK_TOLERANCE` |
| `slack` | `X-Slack-Signature: v0=<hex>` + `X-Slack-Request-Timestamp` | `v0:<ts>:<body>` | `WEBHOOK_TOLERANCE` |

`WEBHOOK_SIGNATURE_HEADER` overrides the header name. The body must still be JSON: this is an
ingestion engine for JSON events, not a general proxy.

---

## Getting started

Requires **Go 1.26.11+** (`go.mod` pins the minimum: earlier 1.26 patch releases carry known
standard-library vulnerabilities; `GOTOOLCHAIN=auto` fetches it).

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
curl localhost:9090/debug/vars   # accepted, rejected{reason}, fsync_*, batch_*, backlog_*, delivered, retried, dead_lettered, degraded, ...
```

What to alert on:

| Metric | Meaning |
| :--- | :--- |
| `backlog_oldest_seconds` | Age of the oldest message still waiting. **The one number to watch**: it grows whatever the reason (slow or failing backend, a file stuck on one message, a handoff that cannot complete). |
| `degraded` | `1` while the WAL cannot write: every request is getting `503`. |
| `files_postponed` | Climbs while the backend keeps failing. |
| `stalled_lines` | Files currently stuck on one message. |
| `given_up` | Messages dead-lettered by `MAX_LINE_ATTEMPTS`: look at `failed/`. |
| `failed_bytes` | Size of `failed/`. Nothing empties it but you. |
| `handoff_pending` | Rotated files that cannot be moved to `processing/` yet: check the data directory. |

---

## Configuration

Every setting is an environment variable and, except the secrets, a flag (`-workers 2`).
**Precedence: flag > environment > default.** An environment variable set to the empty string counts
as set (`ADMIN_ADDR=` disables the admin listener). `ingestor -h` lists them all. Invalid combinations
are rejected at startup with a message naming the setting.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LISTEN_ADDR` | `:8080` | Webhook listener. |
| `DATA_DIR` | `./data` | Root of `ingestions/`, `processing/`, `failed/`. |
| `ENDPOINT` | `/webhook` | Webhook path. |
| `MAX_MESSAGE_SIZE` | `1MiB` | Largest body; also the HTTP body limit (`413`). At most `16MiB`: the largest line the delivery side reads. |
| `MAX_FILE_SIZE` / `ROTATION_INTERVAL` | `1MiB` / `10s` | Rotate the WAL file at this size, or after this long if non-empty. |
| `MAX_BATCH_SIZE` / `MAX_BATCH_BYTES` | `256` / `1MiB` | Upper bounds of one group commit. |
| `COMMIT_LINGER` | `0s` | Optional extra wait to gather a bigger batch (`0` = commit immediately). |
| `QUEUE_SIZE` | `4096` | Capacity of the writer queue. |
| `MAX_INFLIGHT_BYTES` | `64MiB` | `503` above this much data waiting for the disk. |
| `MAX_BODY_MEMORY` | `256MiB` | `503` above this many bytes of request bodies in memory at once (`0` = no limit). A body is admitted against it before it is read. |
| `MAX_BACKLOG_BYTES` | `1GiB` | `503` above this much unprocessed data on disk (`0` = no limit). |
| `MAX_FAILED_BYTES` | `0` | `503` while `failed/` holds more than this (`0` = no limit). |
| `WORKERS` | `4` | Files processed at the same time. |
| `SINK_CONCURRENCY` | `8` | Messages of one file in delivery at the same time (1-64). `WORKERS × SINK_CONCURRENCY` requests can hit the backend at once. `WORKERS=1` with `SINK_CONCURRENCY=1` = in-order delivery. |
| `CHECKPOINT_INTERVAL` | `50` | Lines between checkpoint saves. |
| `SINK_URL` | *(empty)* | Forward each message here with `POST`. Empty = store and discard. |
| `SINK_TIMEOUT` | `10s` | Timeout of one delivery request. |
| `SINK_MAX_ATTEMPTS` | `5` | Attempts per message before the file is postponed. |
| `SINK_BACKOFF_BASE` / `SINK_BACKOFF_MAX` | `200ms` / `30s` | Per-message retry backoff (exponential, jittered). |
| `SINK_AUTHORIZATION` | *(empty)* | `Authorization` header sent with every delivery, e.g. `Bearer <token>`. Environment only. |
| `SINK_HEADERS` | *(empty)* | Extra delivery headers, `Name: value, Name2: value2`. Environment only. |
| `SINK_PERMANENT_STATUSES` | `400,413,415,422` | Destination statuses that dead-letter a message; every other failure is retried. |
| `SINK_KEY_JSON_POINTER` | *(empty)* | JSON Pointer (e.g. `/id`) to the sender's own message id; its value becomes the `Idempotency-Key`. Empty = `<file>:<line>`. |
| `FILE_RETRY_BASE` / `FILE_RETRY_MAX` | `1s` / `1m` | Backoff before a postponed file is retried. |
| `MAX_LINE_ATTEMPTS` | `10` | Dead-letter a message after it has stalled its file this many times in a row while the backend accepts others (`0` = never give up). |
| `MAX_FILE_ATTEMPTS` | `0` | Move a whole file to `failed/` after it is postponed this many times on the same message (`0` = never). A blunt tool: prefer `MAX_LINE_ATTEMPTS`. |
| `WEBHOOK_SECRET` | *(empty)* | Enables signature verification. Environment only, so it never shows up in a process list. |
| `WEBHOOK_SIGNATURE_SCHEME` | `generic` | `generic`, `github`, `stripe` or `slack`: see [Verifying signatures](#verifying-signatures). |
| `WEBHOOK_SIGNATURE_HEADER` | *(the scheme's)* | Overrides the signature header (`X-Signature-256` for `generic`). |
| `WEBHOOK_TIMESTAMP_HEADER` | *(empty)* | `generic`: if set, the MAC covers `<ts>.<body>` and the timestamp must be fresh (anti-replay). `slack`: overrides `X-Slack-Request-Timestamp`. |
| `WEBHOOK_TOLERANCE` | `5m` | Accepted clock skew for the timestamp. |
| `RATE_LIMIT_PER_MINUTE` | `0` | Per-client limit (`0` = off). |
| `TRUSTED_PROXIES` | *(empty)* | IPs or CIDRs of reverse proxies whose `PROXY_HEADER` carries the client IP. Without it the limit keys on the TCP peer, which behind a proxy is the proxy itself. |
| `PROXY_HEADER` | `X-Forwarded-For` | Header believed from `TRUSTED_PROXIES`; a chain is walked from the right past the trusted hops. |
| `READ_TIMEOUT` / `WRITE_TIMEOUT` / `IDLE_TIMEOUT` | `10s` / `15s` / `60s` | HTTP timeouts. Idle keep-alive connections are closed, so they cannot hold slots or block shutdown. |
| `ACK_TIMEOUT` | `5s` | Max wait for the disk before answering `503`. Must be below `WRITE_TIMEOUT`. |
| `MAX_CONNECTIONS` | `10000` | Max concurrent HTTP connections. Each costs a few KiB until its body is admitted against `MAX_BODY_MEMORY`. |
| `SHUTDOWN_TIMEOUT` | `15s` | Grace period at shutdown; the whole sequence fits in it. Give the supervisor a longer stop timeout. |
| `ADMIN_ADDR` | `127.0.0.1:9090` | Health/metrics listener (`ADMIN_ADDR=` disables it). It has no authentication: keep it on localhost or a trusted network. |
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
* `Deliver` is called from several goroutines at once (`WORKERS × SINK_CONCURRENCY`): it must be safe for concurrent use.

The built-in HTTP sink (`SINK_URL`) does a `POST` with `Content-Type: application/json`, `Idempotency-Key`,
`X-Delivery-Id`, `SINK_AUTHORIZATION` and `SINK_HEADERS`. `2xx` is success; `400`, `413`, `415` and `422` are permanent
(`SINK_PERMANENT_STATUSES`); everything else is transient and retried, on purpose: a `401`/`403` after a
credential rotation or a `404` during a deploy is a misconfiguration to fix, not a reason to dead-letter a
backlog. Redirects are never followed (a `301`/`302` would turn the `POST` into a body-less `GET`), so a `3xx`
is a transient error until `SINK_URL` is fixed. Credentials inside `SINK_URL` are rejected: use `SINK_AUTHORIZATION`.

### Deduplicating

Two different things can make your backend see a message twice, and one header cannot cover both:

| Cause | What repeats | Header that stays the same |
| :--- | :--- | :--- |
| The engine redelivers (after a crash, or a retry whose first answer was lost) | The **same stored message** | `X-Delivery-Id: <file>:<line>` |
| The sender delivers the same event twice (it timed out or got a `503`, and retried) | **Two stored messages** with the same content | Only the sender's own id, inside the body |

By default `Idempotency-Key` equals `X-Delivery-Id`, which covers the first case only. If your senders
put an id in their events, set `SINK_KEY_JSON_POINTER` to it (`/id` for Stripe and most others,
`/event_id` for Slack): its value becomes the `Idempotency-Key` and both cases collapse into one key.
A message where the pointer finds no string or number keeps the engine key.

### A message the backend cannot take

A permanent status (`SINK_PERMANENT_STATUSES`) dead-letters the message at once. A message that makes
the backend answer `500` or time out every time looks transient, so it is retried, and it used to
stall its file for ever. Now, after it has stalled the file `MAX_LINE_ATTEMPTS` times in a row (10 by
default), the next failure dead-letters it with the reason `gave up: ...`, **provided another message
got through in that same attempt**; if nothing else did, one further line is sent as a probe. So a
backend that is down, or that rejects everything, never loses a message to `failed/`: there is no
evidence against any single one. `MAX_LINE_ATTEMPTS=0` restores "never give up".

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
sudo useradd --system --home-dir /var/lib/wal-ingestor --shell /usr/sbin/nologin wal-ingestor
go build -o /usr/local/bin/wal-ingestor ./cmd/ingestor
sudo cp deploy/wal-ingestor.service /etc/systemd/system/
sudo systemctl enable --now wal-ingestor
```

**Docker** (distroless, non-root, built-in `healthcheck` subcommand). Mount a volume on `/data`:

```bash
docker build -t wal-ingestion-engine .
docker run -p 8080:8080 -v wal-data:/data -e SINK_URL=http://host.docker.internal:9000/events wal-ingestion-engine
```

Stop it with `docker stop -t 30` (`stop_grace_period: 30s` in Compose, `terminationGracePeriodSeconds: 30` in
Kubernetes): the default 10 s grace period is shorter than `SHUTDOWN_TIMEOUT`. The image binds the admin listener
on `0.0.0.0:9090` so orchestrator probes can reach it; it has no authentication, so publish that port only on a
trusted network.

### Graceful shutdown

On `SIGINT`/`SIGTERM` the engine, in this order: stops accepting connections and lets in-flight requests finish
and get their ACK → commits and closes the WAL → stops the workers, which save their checkpoint. Files not
finished stay in `processing/` and resume on the next start; the final log line reports how many.
The whole sequence completes within `SHUTDOWN_TIMEOUT`. A `SIGKILL` is also safe, only less tidy.

---

## Tuning

* **Throughput is bounded by `fsync` latency, not by the HTTP stack.** The group commit amortises one `fsync` across every request that arrived meanwhile, so more concurrency means bigger batches, not slower ones. Rotations hand the file over on their own goroutine, so a rename never delays a commit.
* **Delivery is bounded by your backend, not by the engine.** The ceiling is `WORKERS × SINK_CONCURRENCY ÷ backend latency`: 4 × 8 ÷ 20 ms = 1,600 msg/s with the defaults. Ingestion is far faster than that (tens of thousands per second), so under sustained load above the ceiling the backlog grows until `MAX_BACKLOG_BYTES` turns it into `503`s. Size it: raise `SINK_CONCURRENCY` first (it also helps when there is a single file), then `WORKERS`, and make sure the backend can take that many requests at once.
* **Delivery latency has a floor.** A message is delivered after its file is rotated: at most `ROTATION_INTERVAL` (10 s) later under light load, sooner once `MAX_FILE_SIZE` fills. Lower `ROTATION_INTERVAL` if seconds matter; the price is more, smaller files.
* **Backend down for long?** `MAX_BACKLOG_BYTES` turns the growing backlog into `503`s instead of filling the disk; the sender retries.
* **Memory:** request bodies are bounded by `MAX_BODY_MEMORY`, data waiting for the disk by `MAX_INFLIGHT_BYTES`, and each file in delivery holds up to `SINK_CONCURRENCY` messages.
* **Many connections:** raise the file descriptor limit (`ulimit -n 65535`, or `LimitNOFILE` in systemd).
* **Benchmarks** (see [`docs/benchmarks.md`](docs/benchmarks.md)): on the development machine, ingestion went from 10 to 1,600-2,000 req/s with 1 connection and from 632 to ~35,000 req/s with 64 versus v1; delivery to a 20 ms backend went from 160 to ~1,100 msg/s with `SINK_CONCURRENCY`.

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
make vuln          # govulncheck
make check         # everything CI runs: fmt, vet, lint, vuln, test
```

CI (`.github/workflows/ci.yml`) runs gofmt, vet and the race tests on Linux and Windows, golangci-lint,
govulncheck and the Docker build on every push and pull request.

The end-to-end tests (`internal/e2e`) build the real binary, point it at a fake backend and `kill -9` it
mid-load, then check that every acknowledged message reached the backend. A killed process keeps its page
cache, so a second test (`TestPowerLoss_AcknowledgedMessagesSurvive`) rebuilds the disk from what had
actually been fsynced at a chosen instant and checks the same thing. `docs/regressions.md` lists the bugs
found so far and the tests that now cover them. `tools/loadgen` is the load generator used for the
benchmarks; with `-sink-addr` it also runs a backend with a fixed latency and reports the drain rate.

Security reports: see [`SECURITY.md`](SECURITY.md).

---

## License

MIT License.

Copyright © 2026 [OKZGN](https://okzgn.com)
