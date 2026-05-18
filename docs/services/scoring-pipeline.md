# scoring-pipeline

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the scoring-pipeline module. Cross-cutting concerns (proto schema, Kafka topic catalogue) live in [`../tech-spec.md`](../tech-spec.md). The simpler stand-in calculation that runs in production today is documented at [`./leaderboard-service.md`](./leaderboard-service.md).
>
> Read this page if you are: (a) about to stand up a Flink JobManager and submit this job for the first time, (b) changing scoring rules in `scoring-pipeline/`, (c) trying to understand why two services appear to write to the same Redis sorted-set keys.

> **Status (2026-05-18): BLOCKED.** This module compiles and has unit tests (including `ScoringEndToEndTest` driving the scoring algorithm directly), but is NOT deployable as a Spring container. `build.gradle` declares Flink as `compileOnly` and `main()` calls `StreamExecutionEnvironment.getExecutionEnvironment()`, which expects an external Flink runtime. Deploying scoring-pipeline = standing up a Flink JobManager + TaskManager pair (in compose, or moving to managed Cloud Dataflow), uploading the fat JAR via `POST /jars/upload`, and submitting the job. Until then, [`./leaderboard-service.md`](./leaderboard-service.md) performs a simpler ZADD-by-points calculation as a stand-in.

---

## 1. Purpose

The Flink DataStream job that — once deployed — will be the official scorer for contest leaderboards. It consumes `evaluated_results`, applies stateful ICPC-style scoring (first-AC-wins on event-time, twenty-minute penalty per pre-AC wrong attempt, partial credit reserved for the future), and writes composite-score ZADDs to the score-range-sharded Redis sorted set that powers the read-side leaderboard.

The job exists separately from leaderboard-service because real contest scoring is order-sensitive in event-time, requires per-(user, problem) state, and must self-correct when a late verdict arrives with an earlier event-time than the current accepted-at. That does not fit in a Spring `@KafkaListener`; Flink's keyed state, watermarks, and exactly-once checkpointing are the right primitives. Today the code captures the intent — `Scorer` is a pure-function port of the ICPC rules, exercised end-to-end by `ScoringEndToEndTest` — but no Flink cluster runs anywhere, so the rules are not in effect on the live Redis keys. The live math is whatever leaderboard-service's `KafkaListener` calculates.

---

## 2. External interfaces

scoring-pipeline is **not** a Spring Boot service. No REST, no `/actuator/health`, no listening port. It is a Flink job — a fat JAR submitted to a JobManager. Its interfaces are its Kafka source and its Redis sink.

**Kafka input.** Topic `regional.evaluated_results` (configurable via `SCORING_INPUT_TOPIC` / `SCORING_KAFKA_TOPIC`); default targets the MM2-mirrored global topic, unmirrored regional name is `evaluated_results`. Consumer group `scoring-pipeline`. Starting offset `OffsetsInitializer.earliest()` on first run; thereafter Flink's checkpoint state owns the offsets, not `__consumer_offsets`. Wire format: binary protobuf `VerdictEvent` (see `common/src/main/proto/events.proto`), deserialised lazily inside the watermark assigner and `keyBy` extractor — the Flink record type is `byte[]`.

**Redis output.** Keys `leaderboard:{contestId}:shard:{shardIndex}` ZSETs, plus a `score_updates:{contestId}` Pub/Sub channel. A single Lua script (`LUA_UPDATE_SCORE` in `RedisLeaderboardSink`) does `ZREM` from non-target shards + `ZADD` to the target + `ZREVRANK` + `PUBLISH`, atomically. Shard index comes from `ScoreRangeShardRouter.shardForScore(zsetScore)` in `:common`. Score encoding: `ScoreEncoder.encode(totalPoints, penaltyMinutes) = totalPoints * 10_000_000 - penaltyMinutes`. Both this service and leaderboard-service target the same ZSET key pattern; once scoring-pipeline is deployed, leaderboard-service's writer path must be disabled or the two will race. Today, only leaderboard-service writes; the overlap is latent.

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
| `leaderboard:{contestId}:shard:{i}` ZSETs | contest lifetime | Redis (overlaps with leaderboard-service today) |
| `score_updates:{contestId}` Pub/Sub | transient | Redis (consumed by leaderboard-service's WebSocket fan-out) |

Not touched: CRDB. The ZSET overlap is the architecture shift the deployment requires — when the Flink job comes online, leaderboard-service's writer turns off; its reader + WebSocket fan-out stays.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| **No Flink runtime today** | Job never submitted; `scoring-pipeline` group has no consumers | leaderboard-service's ZADD-by-points is live; ICPC penalty math is not in effect |
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

**Score values diverging from leaderboard-service's fallback math.** Expected during cutover. Disable leaderboard-service's writer in the same change as the scoring-pipeline deploy — otherwise both writers contend on the same ZSETs and final state depends on which write landed last. Right cutover: deploy scoring-pipeline, observe parity on `oj.scoring.score_updates_total`, *then* flip leaderboard-service's writer off.

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

## 11. Code map

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
