# Online Judge at Scale — Code Companion

Maps every blog part to the source files that implement it.

## Sync Rule

> **When the blog claims a mechanism exists, this companion must point to the file or test that implements it.** If no file exists, the entry must say "Not implemented" and the implementation plan must list it as a gap.

---

## Blog Part → Code Map

| Blog Part | Code Location | Status |
|---|---|---|
| **Part 1: The Problem and the Numbers** | N/A — pure architecture, no code | Architecture only |
| **Part 2: System Overview** | All modules | Architecture only |
| **Part 3: API Gateway** | `api-gateway/` — `SubmissionController`, `RateLimitService` | Implemented (rate limit via Redis sliding window; no JWT auth or contest window enforcement) |
| **Part 4: Contest Service** | Not implemented | Gap — Contest Service is documented in implementation plan as not yet built |
| **Part 5: Submission Service** | `api-gateway/` — `SubmissionService`, `OutboxEvent`, `OutboxPublisherJob` | Implemented (poll-based outbox, not CDC) |
| **Part 6: Execution Service** | `execution-worker/` — `SubmissionConsumer`, `DockerExecutionService`, `IdempotencyService` | Implemented (Docker, not Firecracker) |
| **Part 7: Scoring Pipeline** | `scoring-pipeline/` — `ScoringFunction`, `ScoreEncoder`, `RedisLeaderboardSink`, `ScoringState`, `ScoreUpdate` | Implemented (real Flink 1.18) |
| **Part 8: Leaderboard + Push** | `leaderboard-service/` — `LeaderboardService`, `ScoreUpdateSubscriber`, `WebSocketConfig`, `RedisConfig`, `LeaderboardController` | Implemented (single Redis node, STOMP WebSocket, Redis Pub/Sub) |
| **Part 9: Analytics** | `analytics-pipeline/` — `AnalyticsConsumer`, `ClickHouseWriter` | Implemented (HTTP batch insert) |

---

## Detailed File Map

### `api-gateway/` — Submission Ingest + Transactional Outbox

| File | Blog Reference | What It Does |
|---|---|---|
| `controller/SubmissionController.java` | Part 3 (Gateway pipeline) | POST `/api/v1/submissions` — rate limit check, delegates to `SubmissionService`, returns 202 Accepted |
| `service/SubmissionService.java` | Part 5 (Transactional Outbox) | Single `@Transactional` method: persists `Submission` + `OutboxEvent` in one ACID transaction. T0 stamp via `System.currentTimeMillis()`. |
| `service/OutboxPublisherJob.java` | Part 5 (CDC substitute) | `@Scheduled` poll-based publisher: reads unpublished `OutboxEvent` rows, publishes to Kafka keyed by `userId`, marks published. Production alternative: CockroachDB changefeed. |
| `service/RateLimitService.java` | Part 3 (Rate limiting) | Redis sliding window: `INCR` on `rate_limit:submit:{userId}:{minuteBucket}`, 60s TTL, 10 submissions/min limit. |
| `model/Submission.java` | Part 5 (Schema) | JPA entity: `id`, `userId`, `problemId`, `contestId`, `language`, `s3CodeUrl`, `status`, `gatewayTsMs`, `createdAt`. |
| `model/OutboxEvent.java` | Part 5 (Outbox table) | JPA entity: `id`, `submissionId`, `eventType`, `payload` (JSON), `published`, `createdAt`. |
| `dto/SubmissionRequest.java` | Part 5 (HTTP contract) | Request DTO with validation: `@NotBlank userId`, `problemId`, `language`, `code` (`@Size(max=65536)`). |
| `dto/SubmissionResponse.java` | Part 5 (202 contract) | Response DTO: `submissionId`, `status`, `gatewayTsMs`, `message`. |
| `repository/OutboxEventRepository.java` | Part 5 (Outbox reads) | JPA repository with `findUnpublished(limit)` query for poll-based publisher. |
| `repository/SubmissionRepository.java` | Part 5 (Submission persistence) | JPA repository for `Submission` entity. |
| `config/KafkaConfig.java` | Part 5 (Kafka topics) | Creates `submissions.pretest` and `submissions.system` topics with 12 partitions. |
| `resources/db/migration/V1__init.sql` | Part 5 (Schema) | Flyway migration: `submissions`, `outbox_events`, `idempotency_keys` tables with indexes. |

### `execution-worker/` — Kafka Consumer + Code Execution

| File | Blog Reference | What It Does |
|---|---|---|
| `consumer/SubmissionConsumer.java` | Part 6 (6-step flow) | Kafka consumer: polls `submissions.pretest`, idempotency check, Docker execution, verdict publish to `evaluated_results` + `analytics_events`. Manual ack after verdict publish. |
| `service/IdempotencyService.java` | Part 6 (Consumer-side dedup) | `INSERT ... ON CONFLICT DO NOTHING` against `idempotency_keys` table. Returns `false` on duplicate. `markCompleted()` updates status. |
| `service/DockerExecutionService.java` | Part 6 (Sandbox execution) | Runs code in Docker container: `--rm --network none --memory --cpus --pids-limit --read-only`. Returns `ExecutionResult(status, output, timeMs, memoryMb)`. Gap: production uses Firecracker. |
| `model/IdempotencyKey.java` | Part 6 (Dedup table) | JPA entity: `key` (PK), `submissionId`, `status`, `createdAt`. |

### `scoring-pipeline/` — Flink Stateful Scoring

| File | Blog Reference | What It Does |
|---|---|---|
| `ScoringJobApplication.java` | Part 7 (Pipeline setup) | Flink main class: Kafka source → `keyBy(userId)` → `ScoringFunction` → `RedisLeaderboardSink`. BoundedOutOfOrderness(5 min) watermark using `gatewayTsMs`. ABS checkpointing every 30s. |
| `function/ScoringFunction.java` | Part 7 (KeyedProcessFunction) | ICPC scoring: `ValueState<ScoringState>` per user. ACCEPTED → add points + 20min penalty per prior wrong attempt. WRONG_ANSWER → increment wrong attempts. Emits `ScoreUpdate` on state change. |
| `util/ScoreEncoder.java` | Part 7 (Score encoding) | `encode(totalPoints, penaltyMinutes)` → `(points * 10_000_000) - penalty`. Packs two dimensions into one ZSET float. |
| `sink/RedisLeaderboardSink.java` | Part 7 (Atomic Lua ZADD) | Flink `RichSinkFunction`: atomic Lua script does `ZADD` + `PUBLISH` in one Redis operation. Jedis connection pool. |
| `model/ScoreUpdate.java` | Part 7 (Sink input) | Record: `userId`, `contestId`, `totalScore`, `penaltyMinutes`, `zsetScore`, `eventTsMs`. |
| `model/ScoringState.java` | Part 7 (Managed state) | Serializable state: `totalScore`, `totalPenaltyMinutes`, `wrongAttemptsPerProblem`, `acceptedProblems`. |

### `leaderboard-service/` — Redis ZSET Reads + WebSocket Push

| File | Blog Reference | What It Does |
|---|---|---|
| `service/LeaderboardService.java` | Part 8 (Leaderboard reads) | `ZREVRANGE` for pages, `ZREVRANK` for user rank, `ZCARD` for count. Decodes composite ZSET score back to points + penalty. |
| `service/ScoreUpdateSubscriber.java` | Part 8 (Differential push) | Redis Pub/Sub `MessageListener`: receives `score_updates:{contestId}` messages, pushes to WebSocket clients at `/topic/leaderboard/{contestId}`. |
| `controller/LeaderboardController.java` | Part 8 (HTTP endpoints) | `GET /api/v1/leaderboard/{contestId}` (paginated), `GET /api/v1/leaderboard/{contestId}/rank/{userId}`. |
| `config/WebSocketConfig.java` | Part 8 (WebSocket setup) | STOMP broker at `/ws` endpoint, `/topic` prefix for broadcasts. |
| `config/RedisConfig.java` | Part 8 (Pub/Sub subscription) | `RedisMessageListenerContainer` subscribing to `score_updates:*` pattern. |

### `analytics-pipeline/` — ClickHouse Batch Writer

| File | Blog Reference | What It Does |
|---|---|---|
| `consumer/AnalyticsConsumer.java` | Part 9 (Analytics) | Kafka consumer on `analytics_events` topic. Parses verdict events, buffers to `ClickHouseWriter`. Non-critical: acks even on error. |
| `service/ClickHouseWriter.java` | Part 9 (ClickHouse ingest) | Batch HTTP insert to ClickHouse. `CopyOnWriteArrayList` buffer, flushes at batch-size or 5s interval. TabSeparated format. Re-buffers on failure. |

### `common/` — Shared Protobuf + DTOs

| File | Blog Reference | What It Does |
|---|---|---|
| `Events.java` (generated) | Part 2 (Protobuf events) | Generated from `events.proto`: `SubmissionEvent`, `VerdictEvent`, `AnalyticsEvent`. Currently unused — modules use JSON. |

### Infrastructure

| File | Blog Reference | What It Does |
|---|---|---|
| `docker-compose.yml` | All parts | CockroachDB, Kafka (Confluent CP 7.5), Redis 7, ClickHouse 23.8, Flink jobmanager + taskmanager. |
| `database/init.sql` | Part 5 (Schema) | CockroachDB initialization: creates database and tables. |
| `database/clickhouse-init.sql` | Part 9 (Analytics) | ClickHouse `submission_analytics` MergeTree table. |

---

## Gaps (Blog Claims Not Yet Implemented)

| Blog Mechanism | Status | Notes |
|---|---|---|
| JWT authentication (Part 3) | Not implemented | Gateway accepts all requests; no auth layer. |
| Contest window enforcement (Part 3) | Not implemented | No Contest Service exists. |
| Contest Service (Part 4) | Not implemented | Lifecycle, encrypted pre-load, zero-second gate — all Part 4 mechanisms. |
| Problem Service (Part 2) | Not implemented | Problem CRUD, test case storage, pre-signed URLs. |
| Sandbox Manager (Part 6) | Not implemented | VM pool lifecycle, warm pools, host-side watchdog. Execution uses Docker directly. |
| CockroachDB changefeed CDC (Part 5) | Substituted | Poll-based `OutboxPublisherJob` used instead. Documented gap. |
| Firecracker MicroVMs (Part 6) | Substituted | Docker containers used. Documented gap. |
| Score-range sharding (Part 7) | Not implemented | Single Redis node, single ZSET per contest. |
| Confluent Cluster Linking (Part 7) | Not implemented | Single Kafka cluster locally. |
| Regional read replicas (Part 8) | Not implemented | Single Redis node. |
| Push Service materiality filter (Part 8) | Not implemented | All Pub/Sub messages pushed to all WebSocket clients. |
| Client-side idempotency key (Part 5) | Not implemented | No `Idempotency-Key` header handling. Server generates submission IDs. |
| Protobuf serialization (Part 2) | Not implemented | JSON used throughout. Proto definitions exist but are unused. |
