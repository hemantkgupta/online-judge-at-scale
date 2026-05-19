# scoring-pipeline

> **Owner page.** Last reconciled with the repo on **2026-05-19**.
>
> The single source of truth for the scoring-pipeline module. Cross-cutting concerns (proto schema, Kafka topic catalogue) live in [`../tech-spec.md`](../tech-spec.md). The simpler stand-in calculation that runs in production today (until this Flink job is productionised) is documented at [`./leaderboard-service.md`](./leaderboard-service.md) §3.6.
>
> Read this page if you are: (a) about to stand up a Flink JobManager and submit this job for the first time, (b) changing scoring rules in `scoring-pipeline/`, (c) trying to understand why two services appear to write to the same Redis sorted-set keys.

> **Status (2026-05-19): job code is ready; productionising (Flink JM/TM in compose, fat-JAR submission, smoke harness) tracked in PR #8.** This module compiles and has unit tests (including `ScoringEndToEndTest` driving the scoring algorithm directly), but is NOT deployable as a Spring container. `build.gradle` declares Flink as `compileOnly` and `main()` calls `StreamExecutionEnvironment.getExecutionEnvironment()`, which expects an external Flink runtime. Deploying scoring-pipeline = standing up a Flink JobManager + TaskManager pair (in compose, or moving to managed Cloud Dataflow), uploading the fat JAR via `POST /jars/upload`, and submitting the job. **Until PR #8 lands**, leaderboard-service's stand-in `LeaderboardWriter` (§3.6 of its owner page; landed 2026-05-19) is the live writer — a simpler ZADD-by-points implementation with penalty = 0. See [§11 Follow-up](#11-follow-up-leaderboard-service-writer-cutover) for the cutover plan.

---

## 1. Purpose

The Flink DataStream job that — once deployed — will be the official scorer for contest leaderboards. It consumes `evaluated_results`, applies stateful ICPC-style scoring (first-AC-wins on event-time, twenty-minute penalty per pre-AC wrong attempt, partial credit reserved for the future), and writes composite-score ZADDs to the score-range-sharded Redis sorted set that powers the read-side leaderboard.

The job exists separately from leaderboard-service because real contest scoring is order-sensitive in event-time, requires per-(user, problem) state, and must self-correct when a late verdict arrives with an earlier event-time than the current accepted-at. That does not fit in a Spring `@KafkaListener`; Flink's keyed state, watermarks, and exactly-once checkpointing are the right primitives. Today the code captures the intent — `Scorer` is a pure-function port of the ICPC rules, exercised end-to-end by `ScoringEndToEndTest` — but until PR #8 productionises the Flink runtime, the live math is leaderboard-service's stand-in `LeaderboardWriter` (`totalPoints * 10_000_000`, penalty = 0) running off the same `@KafkaListener` that drives the WebSocket push. The stand-in and this Flink job share the same shard router, the same Lua key/arg shape, and the same composite-score encoding — only the penalty arithmetic differs.

---

## 2. External interfaces

scoring-pipeline is **not** a Spring Boot service. No REST, no `/actuator/health`, no listening port. It is a Flink job — a fat JAR submitted to a JobManager. Its interfaces are its Kafka source and its Redis sink.

**Kafka input.** Topic `regional.evaluated_results` (configurable via `SCORING_INPUT_TOPIC` / `SCORING_KAFKA_TOPIC`); default targets the MM2-mirrored global topic, unmirrored regional name is `evaluated_results`. Consumer group `scoring-pipeline`. Starting offset `OffsetsInitializer.earliest()` on first run; thereafter Flink's checkpoint state owns the offsets, not `__consumer_offsets`. Wire format: binary protobuf `VerdictEvent` (see `common/src/main/proto/events.proto`), deserialised lazily inside the watermark assigner and `keyBy` extractor — the Flink record type is `byte[]`.

**Redis output.** Keys `leaderboard:{contestId}:shard:{shardIndex}` ZSETs, plus a `score_updates:{contestId}` Pub/Sub channel. A single Lua script (`LUA_UPDATE_SCORE` in `RedisLeaderboardSink`) does `ZREM` from non-target shards + `ZADD` to the target + `ZREVRANK` + `PUBLISH`, atomically. Shard index comes from `ScoreRangeShardRouter.shardForScore(zsetScore)` in `:common`. Score encoding: `ScoreEncoder.encode(totalPoints, penaltyMinutes) = totalPoints * 10_000_000 - penaltyMinutes`. Both this service and leaderboard-service's stand-in writer target the same ZSET key pattern with byte-identical Lua bodies; once scoring-pipeline is deployed and verified at rate-parity, the operator flips `app.leaderboard.writer.enabled=false` and the stand-in bean drops out (see [§11 Follow-up](#11-follow-up-leaderboard-service-writer-cutover)).

**Submission surface (deployment-time).** The job has no listening port; the Flink cluster does. Submission:

```
curl -X POST http://<flink-jm>:18081/jars/upload -F "jarfile=@build/libs/scoring-pipeline-all.jar"
curl -X POST 'http://<flink-jm>:18081/jars/<jar-id>/run?entry-class=com.onlinejudge.scoring.ScoringJobApplication'
```

There is no JobManager at `<flink-jm>` today.

---

## 3. Internal design

`ScoringJobApplication.main()` wires four pieces.

**Source + watermark + keying.** `KafkaSource<byte[]>` returns raw protobuf bytes. Watermark: `BoundedOutOfOrderness(5 min)`, event-time = `event_ts_ms` from the parsed `VerdictEvent` falling back to `gateway_ts_ms`. Five minutes absorbs a slow VM emitting its verdict well after Submit; late records past the watermark are not side-outputted today, `Scorer.apply` self-corrects. `keyBy(bytes -> VerdictEvent.parseFrom(bytes).getUserId())` — parse repeated so the stream stays `byte[]` and avoids a deserialiser on every record in transit.

**ScoringFunction + Scorer.** `ScoringFunction extends KeyedProcessFunction<String, byte[], ScoreUpdate>` is intentionally thin: it owns a single `ValueState<ScoringState>` keyed by userId, parses each `VerdictEvent`, delegates to `Scorer.apply(state, ...)` — a pure static function unit-testable without a Flink runtime.

`ScoringState` holds `totalScore`, `totalPenaltyMinutes`, and a `Map<problemId, ProblemScoreState>`. `ProblemScoreState` stores `acceptedAtMs` (event-time of the first ACCEPTED, defaulting to `Long.MAX_VALUE`) and a `TreeSet<Long> wrongAttemptTimes`. Penalty is computed *lazily* as "WAs whose event-time is strictly before `acceptedAtMs`, times twenty" — making the scorer order-independent in event-time:

- An ACCEPTED with `eventTsMs < acceptedAtMs` replaces the existing accepted-at (earlier acceptance wins, per ICPC).
- A late WA / RTE / TLE with event-time before the accepted-at correctly increments the penalty.
- Replays are no-ops: TreeSet absorbs duplicates; an ACCEPTED at the same event-time fails the `<` check.

Verdicts whose `phase` is not `system` or `final` are dropped before state mutation.

**RedisLeaderboardSink.** A `RichSinkFunction<ScoreUpdate>` with a lazy `JedisPool`. Each `invoke()` runs `LUA_UPDATE_SCORE` with keys `[pubsubChannel, targetShardKey, ...otherShardKeys]` and args (user id, zset score, contest id, total score, penalty). The Lua does `ZREM` cleanup (user lives in exactly one shard), `ZADD` to target, `ZREVRANK`, `PUBLISH` to the channel driving the WebSocket fan-out. `ScoreRangeShardRouter.defaultIcpcRouter()` from `:common` is shared with leaderboard-service's reader.

**Checkpointing.** `enableCheckpointing(30_000)` with `setCheckpointStorage(checkpointDir)` (default `file:///tmp/flink-checkpoints`). Production must be S3 / GCS / HDFS. Recovery relies on the Kafka source committing offsets only at checkpoint barriers plus Redis idempotency — the Lua script is idempotent.

---

## 4. Data ownership

| Resource | Lifetime | Where |
|---|---|---|
| Kafka offsets for `scoring-pipeline` | per-checkpoint | Flink checkpoint state (not `__consumer_offsets`) |
| `ScoringState` per userId | running job | Flink keyed state (heap today; RocksDB in prod) |
| Checkpoints | per-interval | `CHECKPOINT_DIR` — local FS in dev, object store in prod |
| `leaderboard:{contestId}:shard:{i}` ZSETs | contest lifetime | Redis (overlaps with leaderboard-service's stand-in writer until cutover) |
| `score_updates:{contestId}` Pub/Sub | transient | Redis (consumed by leaderboard-service's WebSocket fan-out) |

Not touched: CRDB. The ZSET overlap is the architecture shift the deployment requires — when the Flink job comes online and is observed at rate-parity, the operator flips `app.leaderboard.writer.enabled=false` on leaderboard-service and the stand-in writer drops out. Its reader + WebSocket fan-out stays.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| **No Flink runtime in production** (PR #8 productionising) | Job never submitted; `scoring-pipeline` group has no consumers | leaderboard-service's stand-in `LeaderboardWriter` is live (ZADD-by-points, penalty = 0); ICPC penalty math is not in effect until Flink takes over |
| JM / TM crash | Supervisor restarts job from last checkpoint | Kafka source rewinds to checkpointed offsets; replayed ZADDs are idempotent |
| Redis sink unreachable | Jedis throws; Flink retries via job restart strategy | Job stalls at the failed barrier; Kafka-side lag visible |
| Late VerdictEvent (event-time before watermark) | Bytes flow through; `Scorer.apply` consumes them | Order-independent: late WA before accepted raises penalty; earlier-event-time ACCEPTED lowers `acceptedAtMs`. Correct under cross-region Cluster-Linking lag |
| Duplicate VerdictEvent | Same submission replayed after non-graceful failover | Idempotent: TreeSet absorbs duplicate WA times; ACCEPTED at same event-time fails `<`. Lua sink replay is a no-op |
| `phase` not in `{system, final}` | `Scorer.isScoreablePhase` returns false | Record dropped before state mutation — pretest never reaches the leaderboard |
| Score crosses shard boundary | `ScoreRangeShardRouter` returns a new shard index | Lua `ZREM`s the user from every non-target shard before `ZADD`ing to target — single-shard invariant preserved atomically |
| Checkpoint storage unavailable | Flink fails the checkpoint | No exactly-once recovery; monitor `numFailedCheckpoints` |

The most important entry is the first: today the job is not running. Everything below it describes behaviour that only manifests after deployment.

---

## 6. Configuration reference

Read from environment variables in `ScoringJobApplication.main()`. There is no `application.yml`. Defaults shown.

| Env var | Default | Purpose |
|---|---|---|
| `SCORING_KAFKA_BOOTSTRAP_SERVERS` (or `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_BOOTSTRAP`) | `localhost:9093` | Kafka bootstrap. Three names accepted for compose / k8s compatibility |
| `SCORING_INPUT_TOPIC` (or `SCORING_KAFKA_TOPIC`) | `regional.evaluated_results` | Source topic. Default targets the MM2-mirrored global topic; for single-region, override to `evaluated_results` |
| `SCORING_GROUP_ID` | `scoring-pipeline` | Kafka consumer group |
| `REDIS_HOST` | `localhost` | Redis sink host |
| `REDIS_PORT` | `6379` | Redis sink port |
| `CHECKPOINT_DIR` | `file:///tmp/flink-checkpoints` | Checkpoint storage URI. Must be a durable store in production |

Job-level Flink settings (30 s checkpoint interval, 5 min `BoundedOutOfOrderness`) are hardcoded; externalising via `ParameterTool.fromArgs(args)` is a TODO before real submission. Parallelism defaults to the JobManager's `parallelism.default`; the keyBy on userId is the only natural parallelism boundary.

---

## 7. Metrics emitted

scoring-pipeline emits no custom metrics today. Once attached to a Flink runtime, the standard catalogue (numRecordsIn/Out per operator, `lastCheckpointDuration`, `numberOfCompletedCheckpoints`, watermark, backpressure, `numRestarts`) becomes available. Custom names that *should* exist before this is treated as the official scorer:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.scoring.events_total` | counter | `result`, `phase` | Per-verdict counter, partitioned by accepted/wrong and pretest/system/final |
| `oj.scoring.score_updates_total` | counter | `contest_id` | Emitted `ScoreUpdate` count — cardinality of actual ZADDs |
| `oj.scoring.late_events_total` | counter | (none) | Records with event-time before current watermark. No side output today; this is the minimum observability |
| `oj.scoring.event_to_zadd_latency_seconds` | histogram | (none) | Event-time to ZADD latency; pairs with leaderboard-service's read latency for the full SLO |
| `oj.scoring.penalty_correction_total` | counter | (none) | Late wrong-verdict triggered a penalty increase on an already-accepted problem. Near-zero single-region; non-zero under cross-region replication lag |

---

## 8. Runbook

Future-tense — without a Flink cluster, none of these fire today; they are placeholders for the on-call once the deployment lands.

**Job not consuming / lag growing.** Check job state at the JobManager UI; for `FAILED` / `RESTARTING` read `<flink-jm>:18081/jobs/<jobid>/exceptions`. For `RUNNING` with lag, TaskManager logs for backpressure or Redis sink stalls. Common fixes: Redis unreachable from TM; `JedisPool` exhausted (raise `maxTotal` from 10); for crash-loops, `CHECKPOINT_DIR` unreachable or corrupt.

**Checkpoint backlog growing.** Local FS falls over once keyed state passes tens of MB. Fix: move to S3 / GCS; switch to RocksDB backend (`env.setStateBackend(new EmbeddedRocksDBStateBackend())`).

**Redis sink lag — `ScoreUpdate`s emit but ZSET doesn't change.** Verify the Lua script is current and the shard router is `defaultIcpcRouter()` — writer/reader router mismatch is the silent-bug class. `redis-cli MONITOR` reveals ZADDs landing on the wrong key.

**Score values diverging from leaderboard-service's stand-in math.** Expected. The stand-in encodes `totalPoints * 10_000_000` (penalty = 0); this Flink job encodes `totalPoints * 10_000_000 - penaltyMinutes`. Per-user values differ; only the relative rank within a points-tier and the rate of writes are comparable. The cutover criterion is **rate parity** (`oj.scoring.score_updates_total` vs `oj.leaderboard.writer.writes_total`), not value parity. Right cutover: deploy this job; observe rate parity for ≥5 contests; *then* flip `app.leaderboard.writer.enabled=false` on leaderboard-service. The two writers share the same Lua, same shard router, same score encoding — brief overlap during the flip is no-op in score-encoding space.

**Submitting the fat JAR.** `./gradlew :scoring-pipeline:shadowJar` → `scoring-pipeline/build/libs/scoring-pipeline-all.jar`. Submission curl in §2. Verify at `<flink-jm>:18081/jobs/overview`.

---

## 9. Tests & verification

**Unit tests** (`scoring-pipeline/src/test/java/`):

| File | Coverage |
|---|---|
| `ScoringEndToEndTest` | Multi-user multi-problem contest simulation; pretest-then-system gating; resubmission-after-AC no-op; WA-after-AC no-penalty; user-isolation; ZSET ordering across 1000 random users. Does NOT spin up the Flink runtime — drives `Scorer.apply` directly against an in-memory `Map<userId, ScoringState>`, which is what the `KeyedProcessFunction` does modulo the state backend |
| `ScoringFunctionTest` | Flink wrapper with a mocked `ValueState` harness |
| `RedisLeaderboardSinkTest` | Lua keys-and-args shape against a mock Jedis |
| `ScoreEncoderTest` | Composite encoding rank-preserving property |

Run with `./gradlew :scoring-pipeline:test`. The `--add-opens` jvmArgs in `build.gradle` are needed because Flink test-classpath classes require them on JDK 17+.

**Integration + smoke.** End-to-end submission against a real cluster is not automated. Once a Flink stand-up exists, the smoke is: `./gradlew :scoring-pipeline:shadowJar`, submission curl from §2, produce synthetic `VerdictEvent`s into `evaluated_results`, then `redis-cli ZRANGE leaderboard:contest-1:shard:0 0 -1 WITHSCORES`. Until then, `ScoringEndToEndTest.fullContestScenario_correctLeaderboardOrder` *is* the de-facto contract.

### Local end-to-end smoke

`scripts/scoring-smoke.sh` drives the real wire path against the local compose stack: produces protobuf `VerdictEvent`s onto `evaluated_results` at `kafka:9092`, lets MirrorMaker 2 mirror them to `regional.evaluated_results` on `kafka-global:29093`, lets the Flink job consume + score + write to Redis via atomic Lua ZADD, then asserts `ZSCORE` on each shard of `leaderboard:<contest>:shard:{0..2}`.

```
docker compose up -d kafka zookeeper kafka-global mirrormaker2 redis jobmanager taskmanager
./scripts/scoring-smoke.sh
```

The driver builds and submits the shadowJar only when no `RUNNING` Flink job is present; otherwise it reuses the existing job. Re-running back-to-back is idempotent — cleanup deletes only the contest's shard keys, never cancels the job. The producer source lives at `scoring-pipeline/src/test/java/com/onlinejudge/scoring/smoke/SmokeProducer.java` and is invoked via the `runSmokeProducer` Gradle task; it emits one watermark-sentinel verdict to every Kafka partition so an idle partition cannot stall `BoundedOutOfOrderness(5min)` watermark advance.

Expected ICPC math, verified against [`Scorer.java`](../../scoring-pipeline/src/main/java/com/onlinejudge/scoring/function/Scorer.java) + [`ScoreEncoder.java`](../../scoring-pipeline/src/main/java/com/onlinejudge/scoring/util/ScoreEncoder.java) (`PENALTY_PER_WRONG_ATTEMPT_MINUTES = 20`):

| User | Events                                                                  | totalPoints | penalty (min) | zsetScore       | Shard |
|------|-------------------------------------------------------------------------|-------------|---------------|-----------------|-------|
| userA | AC P1 @ t=0; WA P2 @ t+10m; AC P2 @ t+15m                              | 200         | 20            | 1,999,999,980   | 0     |
| userB | AC P1 @ t=0                                                            | 100         | 0             | 1,000,000,000   | 0     |

---

## 10. Relevant design docs

There is no dedicated design doc for scoring-pipeline today. The deployment blocker is documented inline in [`../tech-spec.md`](../tech-spec.md) §4.7 and §11.4 (the prod-readiness gap table). The path forward divides cleanly into two options:

- **Stand up Flink in compose alongside the control plane.** Add a `flink-jobmanager` + `flink-taskmanager` block to `infra/gcp/compose/control-plane-compose.yml` (or a dedicated `flink-compose.yml`). The control-plane VM is memory-tight (`tech-spec.md` §10.1); either bump to `e2-standard-2` or host Flink on a separate VM.
- **Move to managed Cloud Dataflow.** Recompile against the Dataflow runner, submit via `gcloud dataflow flex-template run`. Avoids running Flink in the control plane; pays managed-cloud pricing.

Cross-reference: [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md) covers the Kafka cluster layout this job's source depends on. [`./leaderboard-service.md`](./leaderboard-service.md) is the present-day stand-in and the writer that must be disabled on cutover.

---

## 11. Follow-up: leaderboard-service writer cutover

The full M1 closure (tech-spec §14) is three pieces:

1. **Stand-in writer in leaderboard-service.** Landed 2026-05-19. ZADD-by-points (penalty = 0) gated by `app.leaderboard.writer.enabled` (default `true`). See [`./leaderboard-service.md`](./leaderboard-service.md) §3.6.
2. **Flink productionisation.** PR #8 (smoke harness + JM/TM compose + fat-JAR submission flow + MM2 cross-region wiring).
3. **Cutover flip.** Operator action, AFTER (1) and (2) both land.

**Why both pieces ship together (rather than gating either on the other).** The stand-in closes M1 with working code today: `LeaderboardWriter` writes the ZSETs, the read API returns non-empty pages, the WebSocket `/topic/leaderboard/{contestId}` channel actually fires. Flink (PR #8) then takes over with real ICPC penalty math, exactly-once semantics, and event-time correctness. Decoupling the two lets each PR review on its own merits without the M1 close being held hostage to the Flink runtime productionisation.

**Cutover procedure** (operator action, no code change required):

1. Submit a synthetic contest. Confirm `oj.leaderboard.writer.writes_total{contest_id=...}` and `oj.scoring.score_updates_total{contest_id=...}` both increment on each ACCEPTED system verdict.
2. Watch the operator parity tile across ≥5 contests over ≥24 h. The two counter rates should match (not the absolute values — penalty math differs).
3. Uncomment `APP_LEADERBOARD_WRITER_ENABLED: "false"` in [`infra/gcp/compose/region.yml`](../../infra/gcp/compose/region.yml) for the `oj-leaderboard-service` block and `docker compose up -d oj-leaderboard-service`. The stand-in `LeaderboardWriter` bean drops out of leaderboard-service's context at startup; the `@KafkaListener` keeps running.
4. Verify `oj.leaderboard.writer.writes_total` flatlines while `oj.scoring.score_updates_total` continues. Verify the leaderboard reads still return non-empty pages (now backed by Flink's writes).
5. Optional: `DEL processed:{contestId}` and `leaderboard:state:{contestId}` for active contests after the flip — these are stand-in scratch keys and have a 24 h TTL anyway.

**If something goes wrong** (Flink stops emitting, the ZSETs go cold): comment the env var back out and bounce leaderboard-service. The stand-in resumes from the next ACCEPTED verdict. Both writers are idempotent at the ZSET level (same Lua, equal scores ZADD as no-op).

**Cross-reference.** Audit that surfaced the premise mismatch (LB-service had never actually been the writer, despite docs assuming it was): handled by agent `af394dca` (2026-05-19).

---

## 12. Code map

| Concern | File |
|---|---|
| Job entry point (Flink topology wiring) | `scoring-pipeline/src/main/java/com/onlinejudge/scoring/ScoringJobApplication.java` |
| Keyed process function | `.../function/ScoringFunction.java` |
| Pure-function scoring rules | `.../function/Scorer.java` |
| Per-user / per-problem state | `.../model/{ScoringState,ProblemScoreState}.java` |
| Emitted record + encoding | `.../model/ScoreUpdate.java`, `.../util/ScoreEncoder.java` |
| Redis sink (Lua + Jedis) | `.../sink/RedisLeaderboardSink.java` |
| Shared score-range shard router | `common/src/main/java/com/onlinejudge/common/sharding/ScoreRangeShardRouter.java` |
| End-to-end contest simulation | `.../test/java/com/onlinejudge/scoring/integration/ScoringEndToEndTest.java` |
| Gradle build (shadowJar, Flink `compileOnly`) | `scoring-pipeline/build.gradle` |
| Dockerfile | **none** — Flink job, not a Spring container; excluded from the docker build matrix per `tech-spec.md` §6.7 |
| Compose entry | **none** — a Flink JobManager + TaskManager block is the missing deployment prerequisite |
