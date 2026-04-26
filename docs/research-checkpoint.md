# Online Judge at Scale — Research Checkpoint

Design rationale, foundational concepts, and recommended defaults for the global online judge system.

---

## Direction

Build a **global-scale online judge** capable of handling 1M concurrent users across multiple geographic regions. The system must accept untrusted code safely, execute it fairly, score it with exactly-once semantics, and serve leaderboard rankings in under 10ms — all while absorbing a 5-10x traffic spike in the final minutes of a contest.

The architecture targets the hardest distributed systems constraints simultaneously: sub-100ms write acknowledgment, hardware-isolated code execution, event-time-correct scoring, and million-QPS leaderboard reads.

---

## Foundation

The system rests on four foundational pillars:

### 1. Submission Pipeline

The write path from contestant to durable storage. A regional API Gateway stamps the authoritative ingestion timestamp (T0), applies rate limiting, and forwards to the Submission Service. The Submission Service persists the submission and an outbox event in a single ACID transaction (Transactional Outbox pattern), eliminating the dual-write problem between the database and Kafka.

**Key source files:**
- `api-gateway/.../service/SubmissionService.java` — transactional outbox write
- `api-gateway/.../service/OutboxPublisherJob.java` — poll-based CDC substitute
- `api-gateway/.../resources/db/migration/V1__init.sql` — schema

### 2. Code Execution Sandbox

Untrusted code runs in isolated environments with strict resource constraints. Production uses Firecracker MicroVMs (hardware-assisted KVM isolation, dedicated guest kernel per submission). Local development substitutes Docker containers with `--network none`, `--memory`, `--cpus`, `--pids-limit`, `--read-only`.

**Key source files:**
- `execution-worker/.../service/DockerExecutionService.java` — Docker-based execution
- `execution-worker/.../consumer/SubmissionConsumer.java` — Kafka consumer + orchestration
- `execution-worker/.../service/IdempotencyService.java` — exactly-once guard

### 3. Scoring Pipeline

A stateful Apache Flink job consumes the global verdict stream, computes per-user ICPC scores using `KeyedProcessFunction` with managed state, and sinks to Redis ZSET via atomic Lua scripts. Event-time processing with BoundedOutOfOrderness watermarks ensures fair penalty calculation regardless of execution latency or Kafka lag.

**Key source files:**
- `scoring-pipeline/.../function/ScoringFunction.java` — ICPC stateful scoring
- `scoring-pipeline/.../util/ScoreEncoder.java` — composite score encoding
- `scoring-pipeline/.../sink/RedisLeaderboardSink.java` — atomic Lua ZADD + Pub/Sub

### 4. Leaderboard

Redis ZSET serves as the live leaderboard store. Composite score encoding `(solved * 10,000,000) - penalty` packs two ranking dimensions into one comparable float. The Leaderboard Service translates client requests into `ZREVRANGE`, `ZREVRANK`, and `ZCARD` operations against regional read replicas.

**Key source files:**
- `leaderboard-service/.../service/LeaderboardService.java` — ZSET read operations
- `leaderboard-service/.../service/ScoreUpdateSubscriber.java` — Pub/Sub → WebSocket push
- `leaderboard-service/.../config/WebSocketConfig.java` — STOMP endpoint

---

## Going Deeper

### Transactional Outbox

The Submission Service writes the submission row and an outbox event row in one database transaction. A separate process (CDC or poll-based publisher) tails the outbox table and publishes events to Kafka. This eliminates the dual-write problem: the database is the sole authority on what happened; Kafka is a derived view.

**Implementation:** `SubmissionService.accept()` uses `@Transactional` to persist both `Submission` and `OutboxEvent`. `OutboxPublisherJob` polls unpublished rows every 1 second (~1s latency vs near-instant with Debezium CDC, acceptable for local dev).

### Idempotent Consumers

Two layers of idempotency protect against different failure modes:

1. **Client-side** (HTTP boundary): The `idempotency_keys` table in CockroachDB catches duplicate HTTP requests from browser retries. (Not yet implemented in local code — documented gap.)
2. **Consumer-side** (Kafka boundary): `IdempotencyService.claimSubmission()` uses `INSERT ... ON CONFLICT DO NOTHING` before execution. Kafka at-least-once redelivery after a consumer crash cannot produce duplicate executions.

### Tiered Evaluation

Phase 1 (pretests): 5-10 curated test cases, consumed from `submissions.pretest` topic, verdict in <2 seconds. Eliminates ~70% of submissions before full evaluation.

Phase 2 (system tests): Hundreds of adversarial test cases, consumed from `submissions.system` topic, deferred until after contest close. The two-topic separation gives clean priority isolation — during the surge, all capacity goes to Phase 1.

**Implementation:** Kafka topics `submissions.pretest` and `submissions.system` are defined in `KafkaConfig.java`. Currently only Phase 1 is consumed by `SubmissionConsumer`.

### Event-Time Scoring

The gateway timestamp (`gatewayTsMs`) is the sole authoritative timestamp for scoring. Flink reads it via `withTimestampAssigner((event, ts) -> gatewayTsMs)`. Processing time would make penalties a function of system load; Kafka record timestamps would make penalties a function of execution speed. Only the gateway timestamp reflects when the user actually submitted.

**Implementation:** `ScoringJobApplication.java` configures `BoundedOutOfOrderness(5 min)` with a custom timestamp assigner extracting `gatewayTsMs` from the JSON payload.

### Gateway Timestamping

The T0 timestamp is captured at `System.currentTimeMillis()` in `SubmissionService.accept()` — the earliest server-controlled point — and propagated as an immutable field through every downstream system. It lives in the Kafka message payload (not Kafka's record timestamp), because the record timestamp is set by the producer at publish time, which can be hundreds of milliseconds after the gateway received the request under backpressure.

---

## At Scale

### Cross-Region Kafka Replication

Each region's `evaluated_results` topic is replicated to a central global Kafka cluster via Confluent Cluster Linking (sub-second async replication). A single global Flink job consumes the unified stream — eliminating the need for per-region scoring with a second aggregation layer.

**Local substitute:** Single Kafka cluster. Documented gap.

### Score-Range Sharding

At 1M users, a single Redis ZSET becomes a hot shard. Score-range partitioning distributes the ZSET across Redis nodes:
- Shard 0: scores 0-29,999,999 (0-2 problems solved)
- Shard 1: scores 30,000,000-59,999,999 (3-5 problems solved)
- Shard 2: scores 60,000,000+ (6+ problems solved)

Global rank = `ZCARD(higher shards) + ZREVRANK(user's shard, userId)`.

**Local substitute:** Single Redis node, single ZSET per contest. Documented gap.

### Firecracker Warm Pools

Per-language pre-warmed VM pools (cpp20, python310, java21, js_node20) with a 5-state lifecycle: PROVISIONING → READY → LEASED → DIRTY → TERMINATED. Destroy-never-reuse is a security invariant (prevents data leakage between submissions). Host-side watchdog timer kills the VMM process on timeout — inaccessible to guest code.

**Local substitute:** Cold Docker start per submission. Documented gap.

### Merkle Repair

Not yet applicable to the local implementation. In production, Merkle tree-based repair would detect and reconcile divergence between the Redis ZSET state and the ClickHouse source-of-truth after disaster recovery scenarios.

---

## Recommended Defaults

| Parameter | Default | Rationale |
|---|---|---|
| Kafka replication factor | 3 (across AZs) | Tolerates any single broker failure without data loss |
| Kafka partition count (submissions) | 12 per topic | Good parallelism locally; production sizes to expected consumer count |
| Outbox poll interval | 1000ms | Balances latency vs. DB load for poll-based CDC substitute |
| Outbox batch size | 50 | Processes 50 events per poll cycle |
| Flink checkpoint interval | 30s | Balances recovery time vs. checkpoint I/O |
| Flink watermark out-of-orderness | 5 minutes | Absorbs late verdicts from in-flight VMs after contest close |
| Flink state backend | RocksDB | Incremental checkpointing; full-state backend uploads 200MB every 30s |
| Redis ZSET score multiplier | 10,000,000 | Separates point tiers; supports up to 999 total points per contest |
| ICPC penalty per wrong attempt | 20 minutes | Standard ICPC rule |
| Rate limit | 10 submissions/user/minute | Sliding window via Redis INCR with 60s TTL |
| Docker execution timeout | 5 seconds | Time limit for Phase 1 pretest execution |
| Docker memory limit | 256 MB | Per-submission memory cap |
| Docker PID limit | 64 | Fork bomb defense |
| ClickHouse flush interval | 5000ms | Batch writes for ClickHouse efficiency |
| ClickHouse batch size | 100 records | Minimum batch before force-flush |
| WebSocket endpoint | `/ws` (STOMP) | Clients connect for score updates |
| Leaderboard default page size | 100 | Capped at 500 per request |

---

## Wiki Sources

The architecture and design decisions draw from the following research pages:

| Wiki Page | What It Covers |
|---|---|
| `global-oj-system-design` | High-level system design: service boundaries, data flow, capacity estimates |
| `building-global-oj-java` | Java implementation specifics: Spring Boot 3.2, Flink 1.18, Virtual Threads |
| `global-oj-architecture-deep-research` | Deep dives: Firecracker vs Docker, CockroachDB locality, Kafka backpressure |
| `global-scale-online-judge` | End-to-end architecture guide: all 9 parts of the blog |

---

## Design Principles

1. **Decouple the three workloads.** Ingest (sub-100ms), compute (1-30s), and read (sub-10ms) cannot share infrastructure. The Kafka spine is what separates them.

2. **Database is the source of truth; everything else is derived.** Redis ZSET is derived from Flink output. Flink state is derived from Kafka events. ClickHouse is derived from Kafka events. Any derived store can be rebuilt by replay.

3. **Every consumer is idempotent.** Duplicate delivery never produces duplicate effect. This makes Kafka replay and retry safe — the foundation of the resilience model.

4. **Match locality to workload.** CockroachDB `REGIONAL BY ROW` for write-heavy tables, `GLOBAL` for read-heavy tables. The same cluster, different policies per table.

5. **Defense in depth for adversarial code.** Four concentric layers: VM isolation (Firecracker/KVM), resource caps (cgroups v2), syscall filtering (Seccomp-BPF), filesystem + network isolation. No known exploit chain bypasses all four.

6. **Event time, not processing time.** The gateway T0 timestamp is the only fair basis for scoring. Processing time makes penalties a function of system load; Kafka timestamps make them a function of execution speed. Both are fairness bugs.
