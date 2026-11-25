# Regressions reproduced in the original code

Every scenario was reproduced against commit `71af138` and became an automated test in the redesign.
Until all of them pass, the finding stays open.

| ID | Scenario | Original behaviour | Expected behaviour | Test |
|---|---|---|---|---|
| C2 | `POST` with multi-line JSON (`{\n "id": 2\n}`) | `200`, the message is dropped at processing time | `200` and **one** valid JSON line is delivered | `TestHandler_MultilineStoredAsOneLine` |
| C2 | `POST` with `not json` or an empty body | `200`, silently dropped | `400` | `TestHandler_InvalidBodyIs400` |
| C1 | Sink fails on line 3 of 6 | File deleted; lines 4–6 lost; checkpoint = 3 | File and checkpoint kept; all 6 delivered once the sink recovers | `TestWorker_SinkFailureKeepsFileAndCheckpoint` |
| C3 | 3 MB `POST` | `200`; `token too long`; `os.Create` truncates the file to 0 B (Windows) | `413` | `TestHandler_Oversize413` |
| C3 | Huge line inside a file | Aborts the whole file | Only that line goes to `failed/*.bad` | `TestLineReader_OversizeLineQuarantined` |
| C4 | `Sync`/`Flush` return an error | ACK `200` regardless | Every pending message gets `503` | `TestWriter_SyncErrorNacksAll` |
| A2 | The next file cannot be created | `log.Fatalf` (the process dies) | Degraded state, `503`, automatic recovery | `TestWriter_OpenFailureDegradesAndRecovers` |
| A1 | Start with files in both `ingestions/` and `processing/` | Race: a file can be queued twice | Each file is processed exactly once | `TestSpool_RecoverNoDuplicates` |

## Production review (v2.1)

Findings of the v2.0 review, reproduced before being fixed.

| ID | Scenario | v2.0 behaviour | Expected behaviour | Test |
|---|---|---|---|---|
| R1 | `SINK_URL` answers `301`/`302` | The `POST` becomes a body-less `GET`; a `200` counts as delivered | The redirect is never followed; a `3xx` is a transient error | `TestHTTP_RedirectIsNotFollowed` |
| R2 | `failed/` cannot be written | A warning is logged, the file is deleted and the rejected line vanishes | File kept, checkpoint before the line, retried later | `TestProcessor_UnwritableDeadLetterKeepsFile` |
| R3 | `MAX_MESSAGE_SIZE` lowered with a backlog on disk | Messages accepted earlier are truncated to 4 KiB in `.bad` | Delivered whole; the reader has its own 16 MiB limit | `TestProcessor_LinesAboveMessageSizeAreStillDelivered` |
| R4 | Backend answers `401`/`404` during a deploy or a token rotation | All traffic goes to `failed/*.bad` without retry | Transient: retried; only `400`/`413`/`415`/`422` are permanent | `TestHTTP_StatusClassification` |
| R5 | `RATE_LIMIT_PER_MINUTE` behind a reverse proxy | Every request shares the proxy's IP: a global limit | Per client with `TRUSTED_PROXIES`; `X-Forwarded-For` ignored without them | `TestHandler_RateLimitKeysOnClientBehindTrustedProxy` |
| R6 | `ADMIN_ADDR=` through the environment | The default value is applied | The admin listener is disabled | `TestLoad_EmptyEnvCountsAsSet` |
| R7 | Busy port at start | The lock is taken and `DATA_DIR` touched before failing | Fails before touching the data directory | `TestServe_BusyPortFailsBeforeTouchingData` |
| R8 | Invalid body while the backlog is full | `503` (looks like the engine's fault) | `400` (the sender's fault) | `TestHandler_InvalidJSONIsRefusedEvenWithFullBacklog` |

## Second review (v2.2)

Findings of the v2.1 review. R9, R10 and R14 were reproduced against the real binary before being fixed.

| ID | Scenario | v2.1 behaviour | Expected behaviour | Test |
|---|---|---|---|---|
| R9 | `MAX_MESSAGE_SIZE=32MiB`, 20 MiB `POST` | `200`, never delivered, cut to its first 16 MiB in `.bad` | The setting is refused at startup; an oversized line is dead-lettered whole | `TestLoad_Errors`, `TestMessageLimitNeverExceedsTheReaders`, `TestProcessor_OversizeLineIsDeadLetteredWhole` |
| R10 | 20,000 messages, backend answering in 20 ms | Delivered at 160 msg/s: one message of a file at a time | Concurrent within the file; checkpoint stays a contiguous prefix | `TestProcessor_ConcurrentDeliveryIsFaster`, `TestProcessor_CheckpointIsContiguousPrefix`, `TestProcessor_TransientFailureMidWindowLosesNothing`, `TestProcessor_RedeliveryAfterCrashIsBounded` |
| R11 | The move of a rotated file to `processing/` fails | File left in `ingestions/` until a restart, outside the backlog | Retried in the background, in order, counted as backlog | `TestSpool_ReadyRetriesFailedHandoff` |
| R12 | A processed file cannot be removed | Leaves the backlog, stays on disk until a restart | Removal retried, nothing redelivered, never quarantined | `TestRunner_RetriesRemovalWithoutRedelivery` |
| R13 | `MAX_FILE_ATTEMPTS` during a flapping outage | A file that advances between failures is quarantined | Only consecutive failures on the same message count | `TestRunner_FileAttemptsResetOnProgress` |
| R14 | Backend answers `500` to one message, every time | Its file is stalled for ever | Dead-lettered after `MAX_LINE_ATTEMPTS`, on evidence that the backend is up; never during an outage | `TestProcessor_PoisonLineIsDeadLetteredWhenSinkIsAlive`, `TestProcessor_NeverGivesUpWhileSinkIsDown` |
| R15 | Many connections sending bodies at the limit | Up to `MAX_CONNECTIONS × MAX_MESSAGE_SIZE` buffered before any check | Bodies admitted against `MAX_BODY_MEMORY`; chunked bodies cut at the limit | `TestBody_MemoryBudgetSheds503`, `TestBody_ChunkedOversizeIs413`, `TestBody_StalledBodyReleasesItsReservation` |
| R16 | `-admin-pprof` | `flag needs an argument` | Enables it | `TestLoad_BooleanFlagNeedsNoValue` |
| R17 | `MAX_BACKLOG_BYTES=99999999999GiB` | Wraps around; reported as "must not be negative", or accepted | "size too large" | `TestParseSize` |
| R18 | A crash that loses the page cache (power loss) | Not covered: `kill -9` keeps the page cache | Everything acknowledged survives; the torn tail is dead-lettered | `TestPowerLoss_AcknowledgedMessagesSurvive` |

`TestRunner_RetriesPostponedFilesUntilTheSinkRecovers` failed about once in 40 runs before this
review: it checked the backlog the instant the file disappeared. It now waits for it.
