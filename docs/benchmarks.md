# Benchmarks

Measured with `tools/loadgen` against the binary, on the same machine (Windows 11, local disk, real `fsync`).

```bash
# ingestion only
loadgen -url http://127.0.0.1:<port>/webhook -n <N> -c <C> -size <B>
# ingestion and delivery: loadgen also runs a backend with a fixed latency (point SINK_URL at it)
loadgen -url http://127.0.0.1:<port>/webhook -n <N> -c <C> -size <B> -sink-addr 127.0.0.1:<port+1> -sink-delay 20ms
```

Two different rates matter, and they are bounded by different things:

* **Ingestion** (requests acknowledged per second) is bounded by the `fsync` latency of the disk.
* **Delivery** (messages reaching the backend per second) is bounded by the backend:
  `WORKERS × SINK_CONCURRENCY ÷ backend latency`.

Under sustained load, the lower of the two is the throughput of the system; the difference
accumulates as backlog until `MAX_BACKLOG_BYTES` turns it into `503`s.

## Ingestion

Empty `SINK_URL` (the sink does not influence the ingestion path).

| Concurrency | Size | v1 (`71af138`) | v2.0.0 | v2.2.0 |
|---:|---:|---:|---:|---:|
| 1 | 200 B | 10 req/s (p50 100 ms) | 1,758 req/s (p50 0.52 ms) | **1,600-2,000 req/s** (p50 0.52 ms, p99 1.1 ms) |
| 64 | 200 B | 632 req/s (p50 99.9 ms) | 37,705 req/s (p50 1.6 ms) | **~35,000 req/s** (p50 1.6 ms, p99 3-6 ms) |
| 64 | 4 KB | 645 req/s (p50 99.4 ms) | 17,897 req/s (p50 3.1 ms) | **~17,000 req/s** (p50 3.0 ms, p99 7-25 ms) |
| 256 | 200 B | 41,522 req/s (p50 4.3 ms) | 48,621 req/s (p50 3.8 ms) | not repeated (see below) |

The v2.2.0 column summarises the runs made while developing it, right after the change in how the
request body is read (by the handler, against `MAX_BODY_MEMORY`): five runs at 1 connection (1,602,
1,610, 1,611, 1,947 and 1,988 req/s) and three at 64 (37,818, 39,915 and an outlier of 18,675 req/s
at 200 B; 15,772, 16,563 and 17,026 at 4 KB). The build just before that change, measured in the same
session, gave 1,772-1,975, 32,532-36,804 and 16,636-18,324: the same, within the noise of this machine.

Why v2 is so much faster than v1: v1 only flushed once 100 messages were queued or a 100 ms ticker
fired, so with little concurrency **every request waited ~100 ms**. v2 does an *adaptive group commit*:
the writer blocks until the first message, gathers without waiting whatever is already queued, writes
the batch with a single `write` and does a single `fsync`. While the `fsync` runs the next messages
pile up, so the batch size adapts to the load by itself. With 256 connections v1 already filled its
batch at once, hence the small gain there.

Writer micro-benchmark (`go test -bench Append -benchmem ./internal/wal`, 12 threads, 68 B message,
real files, measured on v2.0.0): ~3.8 µs/op, 128 B/op, 2 allocs/op.

## Delivery

20,000 messages of 400 B sent over 32 connections, backend answering in 20 ms, `WORKERS=4`. "Time"
runs from the first request until the backend has received the last message, so it includes the
ingestion (under a second) and the wait for the last file to be rotated (`ROTATION_INTERVAL`, 10 s).

| `SINK_CONCURRENCY` | Ingestion | Time until everything is delivered | Drain rate | Duplicates |
|---:|---:|---:|---:|---:|
| 1 (the behaviour up to v2.1) | 25,621 req/s | 2 min 4.9 s | **160 msg/s** | 0 |
| 8 (default) | 26,733 req/s | 17.8 s | **1,123 msg/s** | 0 |
| 32 | 26,990 req/s | 12.5 s | **1,606 msg/s** | 0 |

Reading:

* Up to v2.1 a file went out one message at a time: `WORKERS ÷ latency` = 4 ÷ 0.02 s = 200 msg/s at
  best, 160 measured. Ingestion was 160 times faster than delivery.
* With the default of 8 the ceiling is 4 × 8 ÷ 0.02 s = 1,600 msg/s. The measured 1,123 is lower
  because the run is short: the fixed wait for the last rotation weighs on it.
* At 32 the same fixed wait dominates (12.5 s for a 10 s rotation): the backend is no longer the limit
  for a batch of this size. A longer run would get closer to the ceiling.

A first measurement during the review, with other parameters (7,264 req/s ingested, drain measured
over a 10 s window in the middle of the backlog), gave 191 msg/s for the sequential delivery.

## Caveats

* This disk's `fsync` is fast; on a disk with a slow `fsync` the absolute ingestion numbers drop, but
  the group commit amortises that cost across every concurrent request.
* The numbers vary ±25 % from one run to the next on this machine. The order of magnitude and the
  comparisons hold.
* When comparing two builds, alternate them and repeat: running B always right after A showed an 18 %
  difference at 1 connection that disappeared (and reversed) when the order changed.
* Concurrency 256 was not repeated for v2.2.0: on this Windows machine the `loadgen` *client* exhausts
  the runtime's thread limit there (not a server fault), and after tens of thousands of connections
  thousands of sockets stay in `TIME_WAIT` and the ephemeral ports run out, which causes ~10 s stalls
  and network errors in the client that have nothing to do with the server.
