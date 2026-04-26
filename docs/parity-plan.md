# Online Judge at Scale — Parity Plan

Bring the Online Judge project to the same quality bar as the distributed-key-value-store project.

## Gold Standard (what KV store has)

| Artifact | KV Store | OJ (current) | Gap |
|---|---|---|---|
| `docs/adr/0001-*.md` | ✅ Why Dynamo over Raft/FoundationDB | ❌ | Missing |
| `docs/implementation-plan.md` | ✅ 7-phase plan with per-phase code map | ⚠️ Exists but no code map | Needs code map per phase |
| `docs/code-companion.md` | ✅ Maps every blog section → source file | ❌ | Missing |
| `docs/research-checkpoint.md` | ✅ Design rationale, recommended defaults | ❌ | Missing |
| Test files | 30 test files across 6 modules | 0 test files | Missing entirely |
| Blog references actual code | ✅ Every part shows real code from repo | ⚠️ Some parts do, most are pure architecture | Needs update |
| Module contracts (interfaces) | ✅ `StorageEngine`, `ReplicaWriter`, `ReplicaReader` | ⚠️ Service boundaries exist but no formal contracts | Consider adding |
| Documented gaps | ✅ In impl plan + code companion | ✅ In impl plan | Already there |

## Tasks

### 1. Create `docs/adr/0001-multi-service-event-driven.md`

**What**: Architecture Decision Record explaining why this architecture.
**Content**:
- Decision: 10-service event-driven architecture with Kafka spine
- Alternatives considered: monolith judge, serverless (Lambda per submission), single-queue RPC
- Why Kafka over RabbitMQ for the submission pipeline (pull-based backpressure, partition ordering)
- Why CockroachDB over PostgreSQL (multi-region locality, REGIONAL BY ROW vs GLOBAL)
- Why Firecracker over Docker in production (shared kernel = security defect for adversarial code)
- Why Flink over Kafka Streams for scoring (managed state, event-time watermarks, ABS checkpointing)

**File**: `/Users/hemantkgupta/code-all/online-judge-at-scale/docs/adr/0001-multi-service-event-driven.md`

### 2. Create `docs/code-companion.md`

**What**: Map every blog part to the source files that implement it.
**Format** (follow KV store's format exactly):

```
| Blog Part | Code Location | Status |
|---|---|---|
| Part 3: API Gateway | `api-gateway/` — SubmissionController, RateLimitService | Implemented (poll-based outbox, not CDC) |
| Part 5: Submission Service | `api-gateway/` — SubmissionService, OutboxEvent, OutboxPublisherJob | Implemented |
| Part 6: Execution Service | `execution-worker/` — SubmissionConsumer, DockerExecutionService, IdempotencyService | Implemented (Docker, not Firecracker) |
| Part 7: Scoring Pipeline | `scoring-pipeline/` — ScoringFunction, RedisLeaderboardSink, ScoreEncoder | Implemented (real Flink 1.18) |
| Part 8: Leaderboard + Push | `leaderboard-service/` — LeaderboardService, ScoreUpdateSubscriber, WebSocketConfig | Implemented |
| Part 9: Analytics | `analytics-pipeline/` — AnalyticsConsumer, ClickHouseWriter | Implemented |
```

Include a "Sync Rule" section (same as KV store): *"When the blog claims a mechanism exists, this companion must point to the file or test that implements it."*

**File**: `/Users/hemantkgupta/code-all/online-judge-at-scale/docs/code-companion.md`

### 3. Create `docs/research-checkpoint.md`

**What**: Design rationale and foundational concepts, linking to wiki sources.
**Content**:
- Direction: global-scale online judge for 1M concurrent users
- Foundation: submission pipeline, code execution sandbox, scoring pipeline, leaderboard
- Going Deeper: transactional outbox, idempotent consumers, tiered evaluation, event-time scoring, gateway timestamping
- At Scale: cross-region Kafka replication, score-range sharding, Firecracker warm pools, Merkle repair
- Recommended defaults: (4,2) erasure coding for storage, QUORUM for consistency, etc.
- Link to wiki sources: `global-oj-system-design`, `building-global-oj-java`, `global-oj-architecture-deep-research`, `global-scale-online-judge`

**File**: `/Users/hemantkgupta/code-all/online-judge-at-scale/docs/research-checkpoint.md`

### 4. Update `docs/implementation-plan.md` — add per-phase code maps

**What**: Add code map sections after each phase (same format as KV store).
**Example for Phase 1 (API Gateway)**:
```
## Phase 1 Code Map
- `api-gateway/src/main/java/.../controller/SubmissionController.java` — POST /submit endpoint, T0 stamp
- `api-gateway/src/main/java/.../service/SubmissionService.java` — transactional outbox write
- `api-gateway/src/main/java/.../model/OutboxEvent.java` — outbox event row
- `api-gateway/src/main/java/.../service/OutboxPublisherJob.java` — poll-based CDC substitute
- `api-gateway/src/main/java/.../service/RateLimitService.java` — per-user rate limiting
- `api-gateway/src/main/resources/db/migration/V1__init.sql` — submissions + outbox schema
```

Do this for every phase/module.

**File**: `/Users/hemantkgupta/code-all/online-judge-at-scale/docs/implementation-plan.md` (update existing)

### 5. Add tests — minimum viable test suite

**Priority order** (most architecturally important first):

| Test file | Module | What it tests | Why critical |
|---|---|---|---|
| `SubmissionServiceTest.java` | api-gateway | Transactional outbox: submission + outbox row in one TX, idempotency key conflict | Core write-path correctness |
| `IdempotencyServiceTest.java` | execution-worker | Duplicate submission_id detection, skip-on-duplicate | Exactly-once semantics |
| `ScoringFunctionTest.java` | scoring-pipeline | ICPC scoring rules, penalty calculation, accepted/rejected state transitions | Scoring correctness |
| `ScoreEncoderTest.java` | scoring-pipeline | Composite score encoding `(solved × 10M) − penalty`, ordering correctness | Leaderboard ordering |
| `RedisLeaderboardSinkTest.java` | scoring-pipeline | Lua script atomic ZADD + PUBLISH | Leaderboard consistency |
| `LeaderboardServiceTest.java` | leaderboard-service | ZREVRANGE, ZREVRANK, score-range shard routing | Read path correctness |
| `DockerExecutionServiceTest.java` | execution-worker | Timeout enforcement, resource limits, output capture | Execution correctness |
| `OutboxPublisherJobTest.java` | api-gateway | Poll-based publisher: picks up unpublished rows, marks published | Outbox reliability |

Minimum: first 4. Ideal: all 8.

### 6. Update blog — tie Parts 5–8 to actual code

**What**: Update the blog at `CSE-Raw/raw-blog/online-judge-at-scale.md` to show actual code from the repo where the blog currently uses generic/pseudocode snippets.

| Blog Part | Current state | What to change |
|---|---|---|
| Part 5 (Submission Service) | SQL schema shown, transaction shown | Add: actual `SubmissionService.java` transactional write, actual `OutboxPublisherJob.java` poll loop |
| Part 6 (Execution Service) | 6-step flow described | Add: actual `SubmissionConsumer.java` Kafka consumer, actual `DockerExecutionService.java` Docker execution, actual `IdempotencyService.java` dedup check |
| Part 7 (Scoring Pipeline) | Flink `ScoringFunction` shown | Already good — verify the code matches the actual `ScoringFunction.java` in the repo |
| Part 7 (Redis ZSET) | Lua script shown | Add: actual `RedisLeaderboardSink.java` sink code, actual `ScoreEncoder.java` |
| Part 8 (Leaderboard) | Architecture described | Add: actual `LeaderboardService.java` ZREVRANGE calls, actual `ScoreUpdateSubscriber.java` Pub/Sub handler |

**Note**: Do NOT change Parts 1–4 or Part 9 — they are architecture-level and don't need code references.

**File**: `/Users/hemantkgupta/CSE-Raw/raw-blog/online-judge-at-scale.md` (update existing)

### 7. Verify blog ↔ code consistency

After steps 1–6, do a final pass:
- Every code excerpt in the blog must match the actual file in the repo (class name, method name, field names).
- Every file referenced in `code-companion.md` must exist.
- Every test referenced in the plan must exist and pass.
- The `implementation-plan.md` gaps table must match reality.

## File Inventory (what gets created/modified)

| File | Action | Repo |
|---|---|---|
| `docs/adr/0001-multi-service-event-driven.md` | **Create** | online-judge-at-scale |
| `docs/code-companion.md` | **Create** | online-judge-at-scale |
| `docs/research-checkpoint.md` | **Create** | online-judge-at-scale |
| `docs/implementation-plan.md` | **Update** (add code maps) | online-judge-at-scale |
| 4–8 test files (see table above) | **Create** | online-judge-at-scale |
| `raw-blog/online-judge-at-scale.md` | **Update** (tie code to Parts 5–8) | CSE-Raw |

## Order of Operations

1. Read all existing source files to understand current state.
2. Create `docs/adr/`, `docs/code-companion.md`, `docs/research-checkpoint.md`.
3. Update `docs/implementation-plan.md` with code maps.
4. Write tests (start with `SubmissionServiceTest`, `IdempotencyServiceTest`, `ScoringFunctionTest`, `ScoreEncoderTest`).
5. Update blog to reference actual code.
6. Final consistency check.

## What NOT to Change

- Do not restructure the codebase modules — they're already well-organized (6 modules matching the blog's service boundaries).
- Do not change the blog's architecture content (Parts 1–4, Part 9) — it's excellent.
- Do not add modules that don't exist yet (Sandbox Manager, Contest Service, Problem Service) — the implementation plan already documents these as gaps.
