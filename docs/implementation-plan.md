# Online Judge at Scale — Implementation Plan

## What We Are Building

A working, locally-runnable multi-service online judge that demonstrates every architectural concept from the blog.
All components run via a single `docker-compose up` plus gradle `bootRun` per service.

---

## Module Map

| Module | Type | Port | Responsibility |
|--------|------|------|---------------|
| `api-gateway` | Spring Boot | 8080 | Submission ingest: T0 stamp, rate limit, contest window, Transactional Outbox |
| `execution-worker` | Spring Boot | - | Kafka consumer: idempotency check, Docker code execution, verdict publish |
| `scoring-pipeline` | Flink job (fat JAR) | - | Stateful scoring: KeyedProcessFunction, ZADD via Lua, analytics sink |
| `leaderboard-service` | Spring Boot | 8082 | Redis ZSET reads, WebSocket differential push, rank queries |
| `analytics-pipeline` | Spring Boot | - | Kafka consumer, ClickHouse batch writer |
| `common` | Library | - | Protobuf events, shared DTOs |

---

## Kafka Topic Design

| Topic | Partitioned by | Consumers | Purpose |
|-------|---------------|-----------|---------|
| `submissions.pretest` | user_id | execution-worker (T1 group) | Phase 1 fast evaluation |
| `submissions.system` | user_id | execution-worker (T2 group) | Phase 2 exhaustive (deferred) |
| `evaluated_results` | user_id | scoring-pipeline (Flink) | Score computation |
| `analytics_events` | contest_id | analytics-pipeline | ClickHouse ingestion |
| `score_updates` (Redis Pub/Sub) | - | leaderboard-service | WebSocket differential push |

---

## What Is Executable (runs locally)

- **API Gateway**: Full submission ingest with Transactional Outbox (poll-based publisher, not CDC)
- **Execution Worker**: Docker-based code execution with CPU/memory resource limits
- **Scoring Pipeline**: Real Apache Flink 1.18 job with KeyedProcessFunction + Redis Lua ZADD
- **Leaderboard Service**: Redis ZSET reads + STOMP WebSocket push via Redis Pub/Sub
- **Analytics Pipeline**: ClickHouse HTTP batch insert from Kafka

---

## Gaps vs Production (documented, not implemented)

| Production Component | Local Substitute | Why |
|---------------------|-----------------|-----|
| Firecracker MicroVMs | Docker containers | Firecracker requires KVM; not available on macOS |
| CDC (Debezium) outbox | Poll-based outbox publisher (@Scheduled) | Debezium requires Kafka Connect cluster |
| CockroachDB REGIONAL BY ROW | Single-node CockroachDB | Multi-region requires 3+ nodes |
| Confluent Cluster Linking | Not implemented | Requires Confluent Cloud / multi-cluster |
| cgroup v2 + Seccomp-BPF | Docker resource constraints | Host-OS Linux only |
| Warm Pool Daemon | Cold Docker start per submission | Pool management is infra-layer |
| Encrypted problem delivery | Not implemented | Out of scope for judge core |
| Score-range sharding (3 Redis nodes) | Single Redis node | Docker Compose single instance |

---

## Tech Stack

- **Java 21** — Virtual Threads (enabled via `spring.threads.virtual.enabled=true`)
- **Spring Boot 3.2.4** — REST, JPA, Kafka, WebSocket, Redis
- **Apache Flink 1.18** — streaming scoring job (fat JAR submitted to local Flink cluster)
- **CockroachDB** — submissions, outbox_events, idempotency_keys
- **Apache Kafka** — all inter-service messaging (Confluent CP 7.5)
- **Redis 7** — ZSET leaderboard + Pub/Sub
- **ClickHouse 23.8** — analytics store
- **Docker** — local code execution sandbox (gap: production uses Firecracker)
- **Protobuf 3.25** — Kafka message schema (SubmissionEvent, VerdictEvent, AnalyticsEvent)

---

## Build Order

1. `common` — shared Protobuf + DTOs (already has events.proto)
2. `api-gateway` — submission ingest + outbox
3. `execution-worker` — Kafka consumer + Docker exec + verdict publish
4. `scoring-pipeline` — Flink job (fat JAR)
5. `leaderboard-service` — Redis ZSET + WebSocket
6. `analytics-pipeline` — ClickHouse writer

---

## Phase Code Maps

### Phase 1 Code Map — API Gateway (Submission Ingest + Transactional Outbox)

- `api-gateway/src/main/java/.../controller/SubmissionController.java` — POST `/api/v1/submissions` endpoint, rate limit check, returns 202 Accepted
- `api-gateway/src/main/java/.../service/SubmissionService.java` — `@Transactional` outbox write: persists `Submission` + `OutboxEvent` in one ACID transaction, T0 stamp via `System.currentTimeMillis()`
- `api-gateway/src/main/java/.../model/Submission.java` — JPA entity for `submissions` table: id, userId, problemId, contestId, language, s3CodeUrl, status, gatewayTsMs
- `api-gateway/src/main/java/.../model/OutboxEvent.java` — JPA entity for `outbox_events` table: id, submissionId, eventType, payload (JSON), published flag
- `api-gateway/src/main/java/.../service/OutboxPublisherJob.java` — `@Scheduled` poll-based CDC: reads unpublished rows, publishes to Kafka keyed by userId, marks published
- `api-gateway/src/main/java/.../service/RateLimitService.java` — Atomic Lua dual-bucket rate limiter: `INCR` + conditional `EXPIRE` over both `rate_limit:user:{userId}:{m}` AND `rate_limit:ip:{sourceIp}:{m}`. Returns `OK` / `USER_LIMIT` / `IP_LIMIT`. Defaults: 10/min/user, 60/min/IP, 70s TTL.
- `api-gateway/src/main/java/.../dto/SubmissionRequest.java` — Request DTO: `@NotBlank` userId, problemId, language, code (`@Size(max=65536)`)
- `api-gateway/src/main/java/.../dto/SubmissionResponse.java` — Response DTO: submissionId, status, gatewayTsMs, message
- `api-gateway/src/main/java/.../repository/OutboxEventRepository.java` — JPA repo with `findUnpublished(limit)` query
- `api-gateway/src/main/java/.../repository/SubmissionRepository.java` — JPA repo for `Submission` entity
- `api-gateway/src/main/java/.../config/KafkaConfig.java` — Creates `submissions.pretest` and `submissions.system` topics (12 partitions each)
- `api-gateway/src/main/resources/db/migration/V1__init.sql` — Flyway migration: `submissions`, `outbox_events`, `idempotency_keys` tables
- `api-gateway/src/main/resources/application.yml` — Spring Boot config: CockroachDB datasource, Kafka bootstrap, Redis, outbox settings
- `api-gateway/src/test/java/.../service/SubmissionServiceTest.java` — Tests transactional outbox correctness
- `api-gateway/src/test/java/.../service/OutboxPublisherJobTest.java` — Tests poll-based publisher picks up and marks events

### Phase 2 Code Map — Execution Worker (Kafka Consumer + Docker Execution)

- `execution-worker/src/main/java/.../consumer/SubmissionConsumer.java` — Two `@KafkaListener` methods sharing one `processSubmission` helper: `consumePretest` on `submissions.pretest` (concurrency 4) and `consumeSystem` on `submissions.system` (concurrency 2). Idempotency is scoped per phase. Verdicts are tagged with a `phase` field. On `ACCEPTED` in Phase 1, the original submission event is republished to `submissions.system` to trigger Phase 2. Manual `Acknowledgment.acknowledge()` after verdict publish.
- `execution-worker/src/main/java/.../service/IdempotencyService.java` — `INSERT ... ON CONFLICT DO NOTHING` on `idempotency_keys` with a composite key (`submissionId:phase`). `claimSubmission(submissionId, phase)` returns false on duplicate; `markCompleted(submissionId, phase)` updates status to 'completed'. The same submission can legitimately run twice — once per phase.
- `execution-worker/src/main/java/.../service/DockerExecutionService.java` — Runs code in Docker: `--rm --network none --memory {limit}m --cpus 0.5 --pids-limit 64 --read-only --tmpfs /tmp:size=64m`. Returns `ExecutionResult(status, output, executionTimeMs, memoryUsedMb)`. Language configs for python, java, cpp.
- `execution-worker/src/main/java/.../model/IdempotencyKey.java` — JPA entity: key (PK), submissionId, status, createdAt
- `execution-worker/src/main/resources/application.yml` — Kafka consumer config, execution timeout/memory settings
- `execution-worker/src/test/java/.../service/IdempotencyServiceTest.java` — Tests duplicate detection and skip-on-duplicate
- `execution-worker/src/test/java/.../service/DockerExecutionServiceTest.java` — Tests timeout enforcement, resource limits, output capture

### Phase 3 Code Map — Scoring Pipeline (Flink Stateful Scoring)

- `scoring-pipeline/src/main/java/.../ScoringJobApplication.java` — Flink main: `KafkaSource` → `keyBy(userId)` → `ScoringFunction` → `RedisLeaderboardSink`. Watermark: `BoundedOutOfOrderness(5 min)` with `gatewayTsMs` extractor. ABS checkpointing every 30s.
- `scoring-pipeline/src/main/java/.../function/ScoringFunction.java` — `KeyedProcessFunction<String, byte[], ScoreUpdate>`: `ValueState<ScoringState>` per user. ICPC rules: ACCEPTED adds points + 20min × wrong attempts penalty. WRONG_ANSWER increments wrong attempts. Ignores re-submission after acceptance. Emits `ScoreUpdate` on state change.
- `scoring-pipeline/src/main/java/.../util/ScoreEncoder.java` — `encode(totalPoints, penaltyMinutes)` → `(points × 10_000_000) - penalty`. Packs two dimensions into one ZSET float for correct ordering.
- `scoring-pipeline/src/main/java/.../sink/RedisLeaderboardSink.java` — `RichSinkFunction<ScoreUpdate>`: Lua script atomically does `ZREVRANK` (old) → `ZADD` → `ZREVRANK` (new) → `PUBLISH` score update notification. Jedis connection pool.
- `scoring-pipeline/src/main/java/.../model/ScoreUpdate.java` — Record: userId, contestId, totalScore, penaltyMinutes, zsetScore, eventTsMs
- `scoring-pipeline/src/main/java/.../model/ScoringState.java` — Serializable per-user state: totalScore, totalPenaltyMinutes, wrongAttemptsPerProblem (Map), acceptedProblems (Map)
- `scoring-pipeline/src/test/java/.../function/ScoringFunctionTest.java` — Tests ICPC scoring rules, penalty calculation, state transitions
- `scoring-pipeline/src/test/java/.../util/ScoreEncoderTest.java` — Tests composite encoding, ordering correctness
- `scoring-pipeline/src/test/java/.../sink/RedisLeaderboardSinkTest.java` — Tests Lua script atomic ZADD + PUBLISH

### Phase 4 Code Map — Leaderboard Service (Redis ZSET Reads + WebSocket Push)

- `leaderboard-service/src/main/java/.../service/LeaderboardService.java` — `ZREVRANGE` for paginated leaderboard, `ZREVRANK` for user rank, `ZCARD` for participant count. Decodes composite ZSET score back to points + penalty.
- `leaderboard-service/src/main/java/.../service/ScoreUpdateSubscriber.java` — `MessageListener` for Redis Pub/Sub `score_updates:*`: parses score update, pushes to `/topic/leaderboard/{contestId}` via STOMP.
- `leaderboard-service/src/main/java/.../controller/LeaderboardController.java` — `GET /api/v1/leaderboard/{contestId}` (paginated), `GET /api/v1/leaderboard/{contestId}/rank/{userId}`
- `leaderboard-service/src/main/java/.../config/WebSocketConfig.java` — STOMP at `/ws`, simple broker on `/topic`, app destination `/app`
- `leaderboard-service/src/main/java/.../config/RedisConfig.java` — `RedisMessageListenerContainer` subscribing to `score_updates:*` pattern topic
- `leaderboard-service/src/main/resources/application.yml` — Redis connection, WebSocket config, page size defaults
- `leaderboard-service/src/test/java/.../service/LeaderboardServiceTest.java` — Tests ZREVRANGE, ZREVRANK, score decoding

### Phase 5 Code Map — Analytics Pipeline (ClickHouse Batch Writer)

- `analytics-pipeline/src/main/java/.../consumer/AnalyticsConsumer.java` — `@KafkaListener` on `analytics_events`: parses verdict JSON, buffers to `ClickHouseWriter`. Non-critical: acks even on error.
- `analytics-pipeline/src/main/java/.../service/ClickHouseWriter.java` — HTTP batch insert to ClickHouse. `CopyOnWriteArrayList` buffer, flushes at batch-size (100) or interval (5s). TabSeparated format. Re-buffers on failure.
- `analytics-pipeline/src/main/resources/application.yml` — ClickHouse URL, database, batch settings, Kafka consumer config

### Phase 0 Code Map — Common (Shared Definitions)

- `common/build.gradle` — Protobuf plugin, generates Java from `events.proto`
- `common/build/generated/source/proto/main/java/.../events/Events.java` — Generated Protobuf classes (currently unused; modules use JSON)

### Infrastructure Code Map

- `docker-compose.yml` — CockroachDB, Kafka (Confluent CP 7.5), Zookeeper, Redis 7, ClickHouse 23.8, Flink jobmanager + taskmanager
- `database/init.sql` — CockroachDB database and table initialization
- `database/clickhouse-init.sql` — ClickHouse `submission_analytics` MergeTree table
- `build.gradle` — Root Gradle: Java 17+, Spring Boot 3.2.4, Lombok, dependency management
- `settings.gradle` — Includes: common, api-gateway, execution-worker, scoring-pipeline, leaderboard-service, analytics-pipeline

---

## Running Locally

```bash
# Start infrastructure
docker-compose up -d

# Submit to Flink cluster (after building scoring-pipeline)
./gradlew :scoring-pipeline:shadowJar
curl -X POST http://localhost:8081/jars/upload -H "Expect:" \
  -F "jarfile=@scoring-pipeline/build/libs/scoring-pipeline-all.jar"

# Start each service (separate terminals)
./gradlew :api-gateway:bootRun
./gradlew :execution-worker:bootRun
./gradlew :leaderboard-service:bootRun
./gradlew :analytics-pipeline:bootRun

# Submit a test submission
curl -X POST http://localhost:8080/api/v1/submissions \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","problemId":"prob-1","contestId":"contest-1","language":"python","code":"print(1+1)"}'
```

---

## Key Design Decisions

1. **Poll-based outbox over CDC**: Simpler to run locally. Adds ~1s latency vs near-instant with Debezium — acceptable for dev.
2. **Docker over Firecracker**: Same isolation boundary conceptually (process in container), different security model. Clearly documented.
3. **Real Flink**: docker-compose includes Flink jobmanager + taskmanager. The scoring-pipeline is a genuine Flink job, not a simulation.
4. **Single Redis node**: Score-range sharding is demonstrated in the Lua script and Flink sink; single-node is fine for local dev.
5. **CockroachDB single-node insecure**: Same SQL dialect as production multi-region. Schema identical. `REGIONAL BY ROW` syntax documented but not enforced locally.
