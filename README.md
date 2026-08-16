# WAL Ingestion Engine

A high-throughput, crash-resilient webhook ingestion engine in Go, powered by Write-Ahead Logging (WAL), in-situ checkpointing, and controlled load shedding.

---

## 🎯 The Problem

When receiving massive volumes of third-party webhooks:
1. **Downstream APIs can't keep up:** Directly proxying requests or writing to a relational database causes database pool exhaustion, high latencies, and service crashes.
2. **Standard In-Memory Queues risk data loss:** Buffering purely in RAM means that a server restart, crash, or OOM kill drops unacknowledged webhooks.
3. **Slow Acknowledgments cause retries:** Third-party webhook providers timeout if not acknowledged within 2-5 seconds, creating devastating retry storms.

## 💡 The Solution

**WAL Ingestion Engine** acts as an unbreakable shock absorber:
- It responds with **`200 OK` only after payloads are physically committed to disk** via `fsync` (WAL durability).
- It decouples fast sequential disk writes from heavy downstream processing via a dedicated **Worker Pool**.
- It provides **automatic crash recovery** using lightweight in-place checkpointing.
- It applies **Load Shedding (`503 Service Unavailable`)** when saturation thresholds are reached, protecting host memory.

---

## 🏗️ Architecture & Data Flow

```text
               HTTP Webhook Requests
                         │
                         ▼
        ┌───────────────────────────────────┐
        │       Fiber HTTP Ingestion        │
        │  (Backpressure / Non-blocking)    │
        └─────────────────┬─────────────────┘
                          │ (jobQueue channel)
                          ▼
        ┌───────────────────────────────────┐
        │     Disk Writer Engine (WAL)      │
        │  • RAM buffer (128 KB)            │
        │  • Batch flush + fsync            │
        │  • Unblocks HTTP 200 OK ACK       │
        └─────────────────┬─────────────────┘
                          │
            File Rotation (1 MB / 10s)
            [./ingestions] ──> [./processing]
                          │
                          ▼
        ┌───────────────────────────────────┐
        │    Fixed Worker Pool (4 Workers)  │
        │  • Sequential JSON line scanning  │
        │  • In-situ Checkpoints (.done)    │
        │  • Forward to downstream API      │
        └─────────────────┬─────────────────┘
                          │
               ┌──────────┴──────────┐
               ▼                     ▼
          [Success]              [Failure]
       Delete file & chk     Move to [./failed]
```

---

## 🚀 Key Features

* **🛡️ Write-Ahead Log (WAL) Durability:** Payloads are grouped in micro-batches and synced to disk (`fsync`) before confirming receipt (`200 OK`).
* **🚦 Controlled Load Shedding:** Automatically rejects requests with `503 Service Unavailable` if the internal buffer is full, preventing Out-Of-Memory (OOM) panics.
* **🔄 Zero-Copy In-Place Checkpointing:** Resumes interrupted file processing line-by-line after unexpected crashes using `O_RDWR` + `Seek(0, 0)` padding, eliminating disk rewrite overhead.
* **⚡ Decoupled Worker Pool:** Background workers process completed logs without blocking the ingestion path.
* **🛑 Graceful Shutdown:** Captures `SIGINT`/`SIGTERM` to safely drain buffered jobs, perform a final `fsync`, and shut down cleanly without dropping a single event.

---

## 📁 Directory Lifecycle State Machine

| Directory | Description |
| :--- | :--- |
| `./ingestions` | Active log files being written and synced by the WAL engine. |
| `./processing` | Completed rotated files currently being processed by background workers. |
| `./processing/*.done` | Checkpoint tracking the exact last successfully processed line number. |
| `./failed` | Files that encountered unrecoverable JSON scanner errors for manual inspection. |

---

## 🛠️ Getting Started

### Prerequisites
* **Go 1.22+** installed.

### Installation & Run

```bash
# 1. Clone the repository
git clone https://github.com/okzgn/wal-ingestion-engine-go.git
cd wal-ingestion-engine-go

# 2. Download dependencies
go mod tidy

# 3. Run the engine
go run .
```

### Test Ingestion via cURL

```bash
curl -X POST http://localhost/webhook \
     -H "Content-Type: application/json" \
     -d '{"event": "payment_received", "amount": 100, "customer_id": "cust_12345"}'
```

And send massive brute-force requests to `WAL Ingestion Engine` endpoint.

---

## ⚙️ Configuration & Tuning

All engine parameters can be configured via constants in `ingestor.go`:

| Parameter | Default | Description |
| :--- | :---: | :--- |
| `maxConnections` | `1024` | Maximum concurrent HTTP connections allowed. |
| `NumProcessingWorkers` | `4` | Number of concurrent background worker goroutines. |
| `MaxFileSize` | `1 MB` | Maximum file size before rotating to `./processing`. |
| `RotationTimeout` | `10s` | Maximum time an active file stays open before rotating. |
| `FlushTimeout` | `100ms` | Maximum delay before flushing pending jobs to disk (`fsync`). |
| `MaxBatchSize` | `100` | Number of messages that triggers an immediate disk sync. |
| `CheckpointInterval` | `50` | Slices of lines between checkpoint disk synchronizations. |
| `MaxFileLineBufferSize` | `2 MB` | Maximum supported single JSON payload size. |

---

## 🔌 Integrating Your Downstream Logic

To forward validated webhooks to your internal microservices or database, customize the `processJSONLine` function in `ingestor.go`:

```go
func processJSONLine(jsonPayload []byte) error {
    // Example: Forward payload to internal service or broker
    // resp, err := http.Post("http://internal-api/events", "application/json", bytes.NewReader(jsonPayload))
    // return err
    return nil
}
```

---

## 🐧 Production Deployment (Linux)

For high-throughput production environments, ensure your OS open-file limits are increased:

```bash
# Set file descriptor limit
ulimit -n 65535
```

Or add the limit to your **Systemd Service Unit** (`/etc/systemd/system/wal-ingestor.service`):

```ini
[Unit]
Description=WAL Ingestion Engine
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/wal-ingestor
Restart=always
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

---

## License

MIT License.

Copyright © 2026 [OKZGN](https://okzgn.com)
