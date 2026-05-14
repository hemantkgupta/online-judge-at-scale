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
| **Part 3: API Gateway** | `api-gateway/` — `SubmissionController`, `RateLimitService`, `ContestWindowFilter`, `IdempotencyFilter`, `JwtTokenProvider`, `JwtAuthenticationFilter`, `SecurityConfig`, `AuthController` | Implemented (JWT auth via Spring Security + JJWT, rate limit, contest window cache, client idempotency) |
| **Part 4: Contest Service** | `contest-service/` — `ContestStateMachine`, `ContestService`, `EncryptionService`, `LifecycleWorker` | Implemented (5-state FSM, AES-256-GCM encryption, automated T0/T1 transitions, Kafka state fanout) |
| **Part 4.5: Problem Service** | `problem-service/` — `ProblemService`, `Problem`, `TestCase`, `ProblemController` | Implemented (CRUD, pre-signed URL generation, pretest/system test ordinal split) |
| **Part 5: Submission Service** | `api-gateway/` — `SubmissionService`, `OutboxEvent`, `OutboxPublisherJob` (default); `database/changefeed-setup.sql` (opt-in CDC) | Both paths supported: polling publisher by default, or CockroachDB native changefeed by config flag |
| **Part 6: Execution Service** | `execution-worker/` — `SubmissionConsumer` (Phase 1 + Phase 2 consumers), `ExecutionBackend` interface with `DockerExecutionService` (default) and `FirecrackerExecutionService` (Linux-only), `IdempotencyService`, `SandboxManager`, `SandboxPool`, `TestCaseValidator` | Implemented. Backend chosen by `app.sandbox.backend` (`docker` default, `firecracker` on Linux with `/dev/kvm`). Docker backend optionally applies Linux-only Seccomp-BPF + capability drop + private cgroup namespace when `app.sandbox.linux-hardening.enabled=true`. |
| **Part 7: Scoring Pipeline** | `scoring-pipeline/` — `ScoringFunction` (thin Flink wrapper), `Scorer` (pure-function rules), `ProblemScoreState`, `ScoringState`, `ScoreEncoder`, `RedisLeaderboardSink`, `ScoreUpdate` | Implemented (real Flink 1.18; sink writes to score-range-sharded ZSET keys; event-time-aware correction handles late WAs and earlier-arriving ACs) |
| **Part 8: Leaderboard + Push** | `leaderboard-service/` — `LeaderboardService`, `ScoreUpdateSubscriber`, `VerdictPushConsumer`, `WebSocketConfig`, `RedisConfig`, `LeaderboardController` (router lives in `common/`) | Implemented (sharded reads end-to-end on a single Redis node; STOMP WebSocket for both leaderboard deltas and per-user verdict push) |
| **Part 9: Analytics** | `analytics-pipeline/` — `AnalyticsConsumer`, `ClickHouseWriter` | Implemented (HTTP batch insert) |
| **Observability** | `infra/signoz/` — `otel-collector-config.yaml`, SigNoz stack in `docker-compose.yml` | Implemented (OTLP → ClickHouse, zero-code Java agent instrumentation) |

---

## Detailed File Map

### `api-gateway/` — Submission Ingest + Transactional Outbox

| File | Blog Reference | What It Does |
|---|---|---|
| `controller/SubmissionController.java` | Part 3 (Gateway pipeline) | POST `/api/v1/submissions` — rate limit check, resolves the originating region via `RegionResolver`, delegates to `SubmissionService`, returns 202 Accepted. Also exposes `GET /{id}/verdict` HTTP fallback. |
| `service/SubmissionService.java` | Part 5 (Transactional Outbox) + Part 6 (Regional locality) | Single `@Transactional` method: persists `Submission` + `OutboxEvent` in one ACID transaction with `region` stamped on both rows and propagated into the outbox payload. T0 stamp via `System.currentTimeMillis()`. |
| `service/RegionResolver.java` | Part 6 (Regional locality) | Resolves the region for an incoming write. `X-Region` request header wins, otherwise `app.region` (default `us-east-1`). The value lands on `submissions.region` and `outbox_events.region` — the REGIONAL BY ROW key on a multi-region CRDB cluster (see `database/multi-region-setup.sql`). |
| `service/OutboxPublisherJob.java` | Part 5 (Polling CDC substitute) | `@Scheduled` poll-based publisher gated by `@ConditionalOnProperty(app.outbox.publisher.enabled=true)`. Reads unpublished `OutboxEvent` rows, publishes to Kafka keyed by `userId`, marks published. The real CockroachDB native changefeed lives in `database/changefeed-setup.sql` — set the flag to `false` to disable this job and rely on CDC. |
| `database/changefeed-setup.sql` | Part 5 (Production CDC) | `CREATE CHANGEFEED FOR TABLE outbox_events INTO 'kafka://kafka:29092?topic_name=submissions.pretest'` — CockroachDB tails its own Raft log and emits row events directly to Kafka. Free-tier sink on v24.1+. Runnable via `docker exec cockroachdb cockroach sql --insecure < database/changefeed-setup.sql`. The execution-worker's `SubmissionConsumer.unwrapEnvelope()` auto-detects the `{"after":...}` envelope format so the rest of the system doesn't change. |
| `service/RateLimitService.java` | Part 3 (Rate limiting) | Atomic Lua script over two buckets — `rate_limit:user:{userId}:{minuteBucket}` and `rate_limit:ip:{sourceIp}:{minuteBucket}`. `INCR` + conditional `EXPIRE` in one round-trip; returns `OK` / `USER_LIMIT` / `IP_LIMIT`. Defaults: 10/min/user, 60/min/IP, 70s TTL. |
| `model/Submission.java` | Part 5 (Schema) | JPA entity: `id`, `userId`, `problemId`, `contestId`, `language`, `s3CodeUrl`, `status`, `gatewayTsMs`, `createdAt`. |
| `model/OutboxEvent.java` | Part 5 (Outbox table) | JPA entity: `id`, `submissionId`, `eventType`, `payload` (JSON), `published`, `createdAt`. |
| `dto/SubmissionRequest.java` | Part 5 (HTTP contract) | Request DTO with validation: `@NotBlank userId`, `problemId`, `language`, `code` (`@Size(max=65536)`). |
| `dto/SubmissionResponse.java` | Part 5 (202 contract) | Response DTO: `submissionId`, `status`, `gatewayTsMs`, `message`. |
| `repository/OutboxEventRepository.java` | Part 5 (Outbox reads) | JPA repository with `findUnpublished(limit)` query for poll-based publisher. |
| `repository/SubmissionRepository.java` | Part 5 (Submission persistence) | JPA repository for `Submission` entity. |
| `config/KafkaConfig.java` | Part 5 (Kafka topics) | Creates `submissions.pretest` and `submissions.system` topics with 12 partitions. |
| `resources/db/migration/V1__init.sql` | Part 5 (Schema) | Flyway migration: `submissions`, `outbox_events`, `idempotency_keys` tables with indexes. |
| `resources/db/migration/V2__add_region.sql` | Part 6 (Regional locality) | Adds `region VARCHAR(32)` to `submissions` and `outbox_events` plus `idx_outbox_region_unpublished` so a regional changefeed reads its own region's events. The schema shape is what `database/multi-region-setup.sql` upgrades to REGIONAL BY ROW. |

### `execution-worker/` — Kafka Consumer + Code Execution

| File | Blog Reference | What It Does |
|---|---|---|
| `consumer/SubmissionConsumer.java` | Part 6 / Part 7 (6-step flow + Phase 1/2 split) | Two `@KafkaListener` methods sharing a single `processSubmission` helper: `consumePretest` on `submissions.pretest` (concurrency 4) and `consumeSystem` on `submissions.system` (concurrency 2). Idempotency is scoped per phase. On ACCEPTED Phase 1, the original event is forwarded to `submissions.system` to trigger Phase 2. Verdicts are tagged with a `phase` field. Manual ack after verdict publish. |
| `service/IdempotencyService.java` | Part 6 / Part 7 (Consumer-side dedup) | `INSERT ... ON CONFLICT DO NOTHING` against `idempotency_keys` with a composite key (`submissionId:phase`) so a single submission can legitimately run twice — once for pretests, once for system tests. Returns `false` on duplicate; `markCompleted(submissionId, phase)` updates status. |
| `service/ExecutionBackend.java` | Part 6 (Sandbox abstraction) | Interface implemented by both sandbox backends. `SubmissionConsumer` depends on this — backend is chosen at startup by `@ConditionalOnProperty(app.sandbox.backend)`. |
| `service/DockerExecutionService.java` | Part 6 (Sandbox execution) | Default backend. Runs code in Docker container: `--rm --network none --memory --cpus --pids-limit --read-only`. When `app.sandbox.linux-hardening.enabled=true` AND the host kernel is Linux, also adds `--security-opt seccomp=…` (profile in `infra/seccomp/sandbox-seccomp.json`), `--security-opt no-new-privileges`, `--cap-drop=ALL`, `--cgroupns=private` — same Linux kernel facilities Firecracker uses internally, applied to a container. |
| `service/FirecrackerExecutionService.java` | Part 7 (Firecracker MicroVM sandbox) | Selected when `app.sandbox.backend=firecracker`. Linux-only — constructor refuses to start without `/dev/kvm`. Spawns one Firecracker process per submission, drives its REST API over a per-submission Unix socket (`curl --unix-socket`), boots a microVM from `app.sandbox.firecracker.{kernel-image,rootfs-image}`, runs the submission inside the guest, kills the VM on completion. Operator setup in [`infra/firecracker/README.md`](../infra/firecracker/README.md). |
| `model/IdempotencyKey.java` | Part 6 (Dedup table) | JPA entity: `key` (PK), `submissionId`, `status`, `createdAt`. |

### `scoring-pipeline/` — Flink Stateful Scoring

| File | Blog Reference | What It Does |
|---|---|---|
| `ScoringJobApplication.java` | Part 7 (Pipeline setup) | Flink main class: Kafka source → `keyBy(userId)` → `ScoringFunction` → `RedisLeaderboardSink`. BoundedOutOfOrderness(5 min) watermark using `gatewayTsMs`. ABS checkpointing every 30s. |
| `function/ScoringFunction.java` | Part 7 (KeyedProcessFunction) | Thin Flink wrapper: loads `ValueState<ScoringState>`, delegates to `Scorer.apply()`, persists the mutated state, emits any `ScoreUpdate`. The Flink runtime contract (state descriptor, deserialization, emit) lives here; the scoring rules live in `Scorer`. |
| `function/Scorer.java` | Part 7 (Scoring rules) | Pure function (`apply(state, ..., gatewayTsMs)`) implementing event-time-aware ICPC scoring. Late WAs with event-time before the current accepted time recompute penalty; an earlier-arriving AC replaces `acceptedAtMs` and re-derives penalty. Returns an `Optional<ScoreUpdate>` so the wrapper emits only on user-visible changes. Unit-testable without a Flink runtime. |
| `model/ProblemScoreState.java` | Part 7 (Per-problem state) | `acceptedAtMs` (earliest seen AC event-time, or `Long.MAX_VALUE` if not yet accepted), `points`, and a sorted `TreeSet<Long>` of wrong-attempt event-times. `currentPenaltyMinutes()` counts only WAs strictly before `acceptedAtMs`. |
| `util/ScoreEncoder.java` | Part 7 (Score encoding) | `encode(totalPoints, penaltyMinutes)` → `(points * 10_000_000) - penalty`. Packs two dimensions into one ZSET float. |
| `sink/RedisLeaderboardSink.java` | Part 7 (Atomic Lua ZADD) | Flink `RichSinkFunction`: takes a `ScoreRangeShardRouter`, computes the target shard for each update's composite score, and runs an atomic Lua script that first `ZREM`s the user from every non-target shard (handles boundary crossings) then `ZADD`s to the target shard and `PUBLISH`s the change. Jedis connection pool. |
| `model/ScoreUpdate.java` | Part 7 (Sink input) | Record: `userId`, `contestId`, `totalScore`, `penaltyMinutes`, `zsetScore`, `eventTsMs`. |
| `model/ScoringState.java` | Part 7 (Managed state) | Serializable state: `totalScore`, `totalPenaltyMinutes`, `wrongAttemptsPerProblem`, `acceptedProblems`. |

### `leaderboard-service/` — Redis ZSET Reads + WebSocket Push (verdict + leaderboard delta)

| File | Blog Reference | What It Does |
|---|---|---|
| `service/LeaderboardService.java` | Part 8 (Leaderboard reads) | Multi-shard reads through the shared `ScoreRangeShardRouter`: pagination walks shards highest-score first, accumulating until the page is full; user rank locates the user's shard via `ZSCORE` then sums `ZCARD` of higher shards + local `ZREVRANK`; participant count sums `ZCARD` across all shards. Decodes the composite ZSET score back to points + penalty. |
| `service/ScoreUpdateSubscriber.java` | Part 8 (Differential push) | Redis Pub/Sub `MessageListener`: receives `score_updates:{contestId}` messages from the Flink sink's Lua script, pushes to WebSocket clients at `/topic/leaderboard/{contestId}`. |
| `service/VerdictPushConsumer.java` | Part 9 (Verdict push + co-partitioning) | Kafka consumer on `evaluated_results` (group `leaderboard-verdict-push`). For each verdict: (1) caches the payload in Redis at `verdict:{submissionId}` with 24h TTL — the HTTP fallback's source of truth; (2) consults `VerdictConnectionRegistry.isConnectedLocally(userId)` and pushes to `/topic/verdicts/{userId}` only if the user's STOMP session is held by this instance. Co-partitioning (`PartitionAssigner` in `common/`) guarantees the verdict and the WebSocket land on the same instance, so a "not connected here" miss means the user is genuinely offline and the HTTP fallback will deliver on reconnect. |
| `service/VerdictConnectionRegistry.java` | Part 9 (Connection registry) | Per-instance `ConcurrentHashMap<userId, sessionId>` tracking which contestants hold a live STOMP session here. Driven by the STOMP CONNECT/DISCONNECT channel interceptor in `WebSocketConfig`. Read by `VerdictPushConsumer` before broadcasting — turns the gateway into a real "fast path + reliable fallback" router. |
| `controller/LeaderboardController.java` | Part 8 (HTTP endpoints) | `GET /api/v1/leaderboard/{contestId}` (paginated), `GET /api/v1/leaderboard/{contestId}/rank/{userId}`. |
| `config/WebSocketConfig.java` | Part 8 / Part 9 (WebSocket setup) | STOMP broker at `/ws` endpoint, `/topic` prefix for broadcasts (both leaderboard deltas and per-user verdict topics). A `ChannelInterceptor` on the inbound channel watches for STOMP CONNECT (reads the `userId` native header and calls `registry.register`) and DISCONNECT (calls `registry.unregisterBySession`) so the per-instance connection map stays consistent. |
| `config/RedisConfig.java` | Part 8 / Part 9 (Pub/Sub + shard router + read replica) | `RedisMessageListenerContainer` subscribing to `score_updates:*`, a `ScoreRangeShardRouter` `@Bean`, and the primary/read template split: `readRedisTemplate` targets `app.redis.replica.host/port` when set, falls back to the primary otherwise. `LeaderboardService` uses `@Qualifier("readRedisTemplate")` so leaderboard ZSET reads route through the replica; verdict-cache writes and the Pub/Sub listener stay on the primary. |

### `analytics-pipeline/` — ClickHouse Batch Writer

| File | Blog Reference | What It Does |
|---|---|---|
| `consumer/AnalyticsConsumer.java` | Part 9 (Analytics) | Kafka consumer on `analytics_events` topic. Parses verdict events, buffers to `ClickHouseWriter`. Non-critical: acks even on error. |
| `service/ClickHouseWriter.java` | Part 9 (ClickHouse ingest) | Batch HTTP insert to ClickHouse. `CopyOnWriteArrayList` buffer, flushes at batch-size or 5s interval. TabSeparated format. Re-buffers on failure. |

### `contest-service/` — Contest Lifecycle + Encryption

| File | Blog Reference | What It Does |
|---|---|---|
| `model/Contest.java` | Part 4 (Lifecycle) | JPA entity: id, title, state (ENUM), startTime, endTime, encryptionKey, encryptedBundleUrl, optimistic lock version. |
| `model/ContestState.java` | Part 4 (State machine) | Enum: CREATED→REGISTRATION→ACTIVE→CLOSED→RESULTS. Forward-only transitions with valid-next-states map. |
| `service/ContestStateMachine.java` | Part 4 (Transitions) | Validates and executes transitions. Precondition checks per state (e.g., ACTIVE requires encryption key). Idempotent. |
| `service/ContestService.java` | Part 4 (Core logic) | CRUD + lifecycle operations. Kafka state-change fanout. Decryption key delivery endpoint. |
| `service/EncryptionService.java` | Part 4 (Encryption) | AES-256-GCM: key generation, encrypt, decrypt. 12-byte IV, 128-bit auth tag. Tamper detection via GCM. |
| `service/LifecycleWorker.java` | Part 4 (Automation) | `@Scheduled(1s)`: REGISTRATION→ACTIVE at T0, ACTIVE→CLOSED at T1. Concurrent-safe via optimistic locking. |
| `controller/ContestController.java` | Part 4 (API) | REST endpoints: create, register, activate, close, results, key delivery, window check. |
| `repository/ContestRepository.java` | Part 4 (Queries) | findByStateAndStartTimeBefore, findByStateAndEndTimeBefore for lifecycle worker. |

### `problem-service/` — Problem CRUD + Pre-signed URLs

| File | Blog Reference | What It Does |
|---|---|---|
| `model/Problem.java` | Part 4.5 (Data model) | JPA entity: id, title, timeLimitMs, memoryLimitMb, points, statementR2Key, testCaseCount. CockroachDB GLOBAL locality. |
| `model/TestCase.java` | Part 4.5 (Test cases) | JPA entity: id, problemId, ordinal, inputR2Key, expectedOutputR2Key, maxScore. `isPretest()` = ordinal ≤ 10. |
| `service/ProblemService.java` | Part 4.5 (Core logic) | CRUD, pre-signed URL generation (5-min TTL), pretest vs system test filtering by ordinal. |
| `controller/ProblemController.java` | Part 4.5 (API) | REST: `GET /problems/{id}/test-cases?pretestOnly=true`. Called by Execution Service. |
| `repository/ProblemRepository.java` | Part 4.5 (Queries) | Standard JPA repository. |
| `repository/TestCaseRepository.java` | Part 4.5 (Queries) | findByProblemIdOrderByOrdinal, findByProblemIdAndOrdinalLessThanEqual for pretest filtering. |

### `common/` — Shared Protobuf + Cross-module Types

| File | Blog Reference | What It Does |
|---|---|---|
| `Events.java` (generated) | Part 2 (Protobuf events) | Generated from `events.proto`: `SubmissionEvent`, `VerdictEvent`, `AnalyticsEvent` — each carries `region` and `phase`. The Kafka wire format on `submissions.pretest`, `submissions.system`, `evaluated_results`, and `analytics_events` is the proto bytes; the WebSocket leg from `VerdictPushConsumer` to the browser transcodes back to JSON (browser clients stay JSON-only per Part 9). |
| `sharding/ScoreRangeShardRouter.java` | Part 8 (Score-range sharding) | Shared shard topology used on both write (Flink sink) and read (Leaderboard Service) sides so they agree on which key holds which user. Default 3 shards at 0 / 30M / 60M score boundaries. Exposes `shardForScore`, `shardKey`, `higherShardIndices`, and `computeGlobalRank`. |
| `sharding/PartitionAssigner.java` | Part 9 (Push Service co-partitioning) | Pure stateless utility — given a `userId` and an `instanceCount`, returns the gateway instance index that owns the user (FNV-1a hash, `floorMod`). Used both by the LB-config generator (WebSocket routing) and by the gateway's "is this user mine?" check; both sides agree because they share this function. |

### Infrastructure + Observability

| File | Blog Reference | What It Does |
|---|---|---|
| `docker-compose.yml` | All parts | CockroachDB, Kafka (regional `kafka` + global `kafka-global`, both Confluent CP 7.5), MirrorMaker 2 (`mirrormaker2` service replicating regional → global), Redis 7 + replica, ClickHouse 23.8, Flink jobmanager + taskmanager, SigNoz stack. |
| `infra/mm2/mm2.properties` | Part 8 (Regional → global replication) | MirrorMaker 2 config: `regional` → `global` cluster aliases, replicates `evaluated_results` and `submissions.(pretest|system)`; mirrored topics land as `regional.<topic>` on the global cluster. |
| `database/init.sql` | Part 5 (Schema) | CockroachDB initialization: creates database and tables. `submissions` and `outbox_events` carry the `region` column. |
| `database/multi-region-setup.sql` | Part 6 (Regional locality) | Production reference, not run locally. Declares regions on the database, re-types the `region` column to `crdb_internal_region`, and applies `LOCALITY REGIONAL BY ROW AS region` to the write-path tables. With this in place, CRDB pins each row's replicas to the region named in its column. |
| `database/clickhouse-init.sql` | Part 9 (Analytics) | ClickHouse `submission_analytics` MergeTree table. |
| `infra/signoz/otel-collector-config.yaml` | Observability | OTLP gRPC/HTTP receivers → batch processor → ClickHouse exporters for traces, metrics, logs. |

---

## Gaps (Blog Claims Not Yet Implemented)

| Blog Mechanism | Status | Notes |
|---|---|---|
| Push Service materiality filter (Part 9) | Not implemented | Requires a frontend that publishes viewport messages. |

## Previously Documented Gaps — Now Resolved

| Mechanism | Resolution |
|---|---|
| Contest Service (Part 4) | Implemented: `contest-service/` with 5-state FSM, AES-256-GCM, lifecycle automation |
| Problem Service (Part 5) | Implemented: `problem-service/` with CRUD, pre-signed URLs, pretest/system split |
| Contest window enforcement (Part 3) | Implemented: `api-gateway/security/ContestWindowFilter.java` with push+pull cache |
| Client-side idempotency (Part 6) | Implemented: `api-gateway/security/IdempotencyFilter.java` with Redis SETNX |
| Observability | Implemented: SigNoz stack in docker-compose + OTel Collector config |
| **Sandbox Manager (5-state FSM + host-side watchdog)** (Part 7) | Resolved: `SandboxManager` enforces `PROVISIONING → READY → LEASED → DIRTY → TERMINATED` via atomic CAS on `Sandbox.compareAndSetState`. Per-language pools (python/java/cpp) with configurable targets. `@Scheduled` replenishment (500ms) and `@Scheduled` host-side `watchdogScan` (1s) — the watchdog kills any LEASED sandbox past `executionTimeoutSeconds + watchdogGraceSeconds` and is exercised by `SandboxManagerWatchdogTest`. |
| **Event-time correction for late verdicts** (Part 8) | Resolved: `ScoringFunction` delegates to a pure-function `Scorer` over per-problem `ProblemScoreState` (`acceptedAtMs` + sorted `TreeSet<Long>` of wrong-attempt event-times). Late WAs whose event-time precedes the accepted time trigger penalty recomputation; an earlier-arriving AC replaces `acceptedAtMs` and re-derives penalty. Exercised by `ScoringFunctionTest` (3 new correction-flavor cases). |
| **HTTP verdict fallback** (Part 9) | Resolved: `GET /api/v1/submissions/{id}/verdict` in `api-gateway` reads from `verdict:{submissionId}` in Redis, where `VerdictPushConsumer` caches every verdict with a 24-hour TTL. Returns 200 with the payload or 404 with a `PENDING` placeholder. |
| **CockroachDB changefeed CDC** (Part 6) | Resolved (opt-in): `database/changefeed-setup.sql` defines a real `CREATE CHANGEFEED ... INTO 'kafka://...'` on v24.1+ (free-tier Kafka sink). The polling publisher is gated by `app.outbox.publisher.enabled` and the consumer's `unwrapEnvelope()` handles both formats — switch is a config flag + one `cockroach sql` invocation. |
| **Regional Redis read replicas** (Part 9) | Resolved (mechanism, not geography): docker-compose now ships a `redis-replica` service replicating from `redis`. `RedisConfig` defines a `readRedisTemplate` bean that targets `app.redis.replica.host` when set, falls back to the primary otherwise. `LeaderboardService` uses `@Qualifier("readRedisTemplate")` so every ZREVRANGE/ZREVRANK/ZSCORE/ZCARD goes through it; writes (verdict-cache `SET`, Pub/Sub) stay on the primary. Geographic regions can't be modelled on a single host, but the read-write split and async replication are real. |
| **Score-range sharding wired end-to-end** (Part 8) | Resolved: `ScoreRangeShardRouter` (now in `common/`) is used by `RedisLeaderboardSink` to route ZADDs to shard-specific keys, and by `LeaderboardService` to read across shards. Still a single Redis node, but the routing logic is active. |
| **Phase 2 system-test pipeline** (Part 7) | Resolved: `submissions.system` topic + `consumeSystem` consumer group; Phase 1 ACCEPTED auto-promotes to Phase 2. Idempotency keys are phase-scoped. |
| **Atomic per-user + per-IP rate limit** (Part 3) | Resolved: `RateLimitService` runs an atomic Lua script over `rate_limit:user:{userId}:{m}` and `rate_limit:ip:{sourceIp}:{m}` buckets. |
| **Verdict push** (Part 9) | Resolved (minimal): `VerdictPushConsumer` in `leaderboard-service` consumes `evaluated_results` on group `leaderboard-verdict-push` and pushes to per-user STOMP destinations. |
| **Push Service co-partitioning + reconnect-resume** (Part 9) | Resolved (logic, not multi-instance): `PartitionAssigner` (in `common/sharding/`) encapsulates the FNV-1a `userId → instance` hash that both the WebSocket LB and the Kafka consumer-group partitioner would share in production. `VerdictConnectionRegistry` tracks live STOMP sessions per instance, populated by a `WebSocketConfig` channel interceptor on STOMP CONNECT/DISCONNECT. `VerdictPushConsumer` consults the registry before pushing — if the user isn't connected here, it skips the live push and the HTTP fallback (`GET /api/v1/submissions/{id}/verdict`) serves the cached verdict on reconnect. The local demo runs a single instance, so partition #0 holds every user; spinning up `N` instances behind a hash-aware LB would activate the routing without code changes. |
| **Firecracker MicroVMs + cgroup v2 + Seccomp-BPF** (Part 7) | Resolved (Linux-only paths gated by config; Docker is the cross-platform default). `ExecutionBackend` is the interface both sandbox runtimes implement. `FirecrackerExecutionService` (selected by `app.sandbox.backend=firecracker`) runs each submission in a per-VM Firecracker microVM via the Firecracker REST API — Linux + `/dev/kvm` required, constructor refuses to start otherwise. Operator setup in `infra/firecracker/README.md`. **Hardware-validated on a Raspberry Pi 5** running Debian Bookworm with the `rpt-rpi-2712` kernel: all 267 tests green, `FirecrackerExecutionServiceTest.execute_bootsRealMicroVMAndCleanlyReturnsAfterTimeout` drives a real microVM through the REST sequence end-to-end. Two production-bound bugs were caught by that real-hardware run and fixed: (1) the API-socket directory was hardcoded to `/run` which fails for non-root, non-jailered users — now configurable via `app.sandbox.firecracker.api-sock-dir` (default `/tmp`); (2) the kernel `boot_args` defaulted to `init=/init` which silently 0-exited every microVM against the public Firecracker CI rootfs (no `/init` → kernel panic → `panic=1 reboot=k` → exit 0 → Java thought "OK") — now configurable via `app.sandbox.firecracker.boot-args` with a default that works on any standard rootfs. On the Docker side, `app.sandbox.linux-hardening.enabled=true` adds Seccomp-BPF (profile in `infra/seccomp/sandbox-seccomp.json`), capability drop, `no-new-privileges`, and a private cgroup namespace — the same kernel facilities Firecracker uses internally. Hardening flags are silently skipped on non-Linux hosts so the setting is safe to ship across a mixed-OS fleet. |
| **JWT authentication** (Part 3) | Resolved: `JwtTokenProvider` signs/verifies HS256 tokens via JJWT (Apache 2.0); `JwtAuthenticationFilter` is a `OncePerRequestFilter` that reads `Authorization: Bearer …` and installs the JWT subject as the authenticated principal; `SecurityConfig` wires Spring Security with stateless sessions, CSRF disabled, only `/actuator/health` + `/api/v1/submissions/health` + `/api/v1/auth/token` are public. `SubmissionController` reads `@AuthenticationPrincipal String userId` instead of trusting a body field — the userId column on `submissions`, the rate-limit key, and the outbox payload all come from the verified token. `AuthController` exposes a dev-only `POST /api/v1/auth/token` to mint a token for any user-id (production uses an external IdP). |
| **Protobuf serialization** (Part 2) | Resolved: the Kafka wire format on every cross-service topic is now Protobuf. `common/src/main/proto/events.proto` defines `SubmissionEvent`, `VerdictEvent`, `AnalyticsEvent` (each carrying `region` and `phase`). The `api-gateway` outbox publisher transcodes its JSON-stored payload into a `SubmissionEvent` before shipping to Kafka; `execution-worker` reads `SubmissionEvent` proto (with a JSON fallback so the CRDB changefeed envelope path still works), publishes `VerdictEvent` + `AnalyticsEvent` proto; `scoring-pipeline` Flink job reads `VerdictEvent` proto; `analytics-pipeline` reads `AnalyticsEvent` proto. The only JSON leg left is the WebSocket push from `VerdictPushConsumer` to the browser (intentional — the blog's Part 9 specifies JSON over WS so the client-side parsing stays simple). |
| **Kafka cross-cluster replication** (Part 8) | Resolved with **MirrorMaker 2** (Apache 2.0) instead of the proprietary Confluent Cluster Linking the blog originally referenced. docker-compose runs two brokers (`kafka` = regional, `kafka-global` = global) plus a `mirrormaker2` service running `connect-mirror-maker /etc/mm2/mm2.properties`. Topics land on the global cluster as `regional.evaluated_results`, `regional.submissions.pretest`, etc. (MM2's `DefaultReplicationPolicy`). The architectural shape (regional → global async replication with sub-second lag) matches the blog; zero vendor lock-in. |
| **CockroachDB REGIONAL BY ROW** (Part 6) | Resolved (schema + application; locality is multi-node only): `submissions` and `outbox_events` carry a `region` column (Flyway `V2__add_region.sql`). `RegionResolver` in `api-gateway` reads the `X-Region` header or falls back to `app.region`; `SubmissionService` stamps both rows and the outbox payload with the resolved region in the same `@Transactional` block. `database/multi-region-setup.sql` is the canonical production script that declares regions, re-types the column to `crdb_internal_region`, and applies `LOCALITY REGIONAL BY ROW AS region` — running it on a multi-region cluster activates locality with zero application changes. Single-node locally can't pin replicas, but every other piece of the contract is in place. |
