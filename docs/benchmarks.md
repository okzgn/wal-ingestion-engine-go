# Benchmarks

Measured with `tools/loadgen` against the binary, on the same machine (Windows 11, local disk, real `fsync`).
Command: `loadgen -url http://127.0.0.1:<port>/webhook -n <N> -c <C> -size <B>`

> **Version measured: v2.0.0.** The figures have not been repeated after v2.1.0, which moves the rename and
> the directory `fsync`s of each rotation off the writer goroutine. That change only affects the p99 under
> heavy load (a rotation no longer delays the next batch); throughput should not change. Re-measuring with
> `make loadgen` is pending.

## Baseline: original code (commit `71af138`)

| Concurrency | Size | Requests | Throughput | p50 | p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 200 B | 100 | **10 req/s** | 100 ms | 100.6 ms |
| 64 | 200 B | 20,000 | **632 req/s** | 99.9 ms | 186 ms |
| 64 | 4 KB | 10,000 | **645 req/s** | 99.4 ms | 147 ms |
| 256 | 200 B | 10,000 | **41,522 req/s** | 4.3 ms | 28 ms |

Reading: the original engine only flushes once 100 messages are queued (`MaxBatchSize`) or when the
100 ms ticker fires. With little concurrency **every request waits ~100 ms** and the ceiling is
`concurrency / 0.1 s`. With 256 connections the batch fills at once and the ticker stops mattering.
This is finding **M2** and the main goal of Phase 5 (adaptive group commit).

Note: at concurrency 512 the `loadgen` *client* exhausted the runtime's thread limit on this Windows
machine (not a server fault); the high values are measured up to 256.

## v2.0.0 (branch `fix/hardening`)

Same loads, same machine, empty `SINK_URL` (the sink does not influence the ingestion path).

| Concurrency | Size | Requests | Throughput | p50 | p99 | Before (req/s) | Gain |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 200 B | 100 | **1,758 req/s** | 0.52 ms | 1.0 ms | 10 | **x176** |
| 64 | 200 B | 20,000 | **37,705 req/s** | 1.6 ms | 3.3 ms | 632 | **x60** |
| 64 | 4 KB | 10,000 | **17,897 req/s** | 3.1 ms | 6.9 ms | 645 | **x28** |
| 256 | 200 B | 10,000 | **48,621 req/s** | 3.8 ms | 29.5 ms | 41,522 | x1.2 |

What changed (finding **M2**): there is no 100 ms ticker any more. The writer does an *adaptive group
commit*: it blocks until the first message, gathers without waiting whatever is already queued, writes the
batch with a single `write` and does a single `fsync`. While the `fsync` runs the next messages pile up, so
the batch size adapts to the load by itself and the latency under light load drops from 100 ms to ~0.5 ms.
Under heavy load (256 connections) the old design already filled its batch of 100, hence the small gain
there.

Writer micro-benchmark (`go test -bench Append -benchmem ./internal/wal`, 12 threads, 68 B message,
real files): **~3.8 µs/op, 128 B/op, 2 allocs/op** (**M8**: message buffers from the `sync.Pool`, one
reused batch buffer).

Caveat: this disk's `fsync` is fast; on a disk with a slow `fsync` the absolute numbers will drop, but the
*group commit* amortises that cost across every concurrent request.

## Run-to-run variation

The numbers vary ±25 % from one run to the next on this machine (for example, with 1 connection between
~1,000 and ~1,750 req/s were measured, and with 64 connections between ~30,000 and ~38,000 req/s after
upgrading the dependencies). The order of magnitude and the comparison with the baseline hold.

Beware when repeating load tests on Windows: after tens of thousands of connections, thousands of sockets
stay in `TIME_WAIT` and the ephemeral ports run out. That causes ~10 s stalls and network errors in the
*client* at 256 connections that have nothing to do with the server (verified: with clean sockets, the same
binary does ~45,000 req/s without errors, like the one before the upgrade).
