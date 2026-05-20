# leaderboard-service

> **Owner page.** Last reconciled with the repo on **2026-05-19**.
>
> The single source of truth for the leaderboard-service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the Redis key catalogue, the multi-region story) live in [`../tech-spec.md`](../tech-spec.md). The upstream verdict producer is documented at [`./execution-worker.md`](./execution-worker.md).
>
> **Status.** Dockerfile + image + compose entry shipped (commit `6be38f4`). Stand-in leaderboard writer (M1 closure) landed 2026-05-19 — ON by default; flips OFF when scoring-pipeline (PR #8) takes over. Not yet deployed live on the GCP environment; the next control-plane VM bounce will bring the writer up.
>
> Read this page if you are: (a) on-call for the live leaderboard or the verdict-push WebSocket, (b) changing anything in `leaderboard-service/`, (c) thinking about how Redis keys are shaped for the scoring story.

---

## 1. Purpose

The fan-out tier between the verdict stream and the contestant's browser. Four responsibilities:

1. **Kafka consumer.** Pulls `VerdictEvent` from `evaluated_results` and caches it in Redis under `verdict:{submissionId}` (TTL 24 h). The api-gateway HTTP-fallback path reads this key when a client polls `GET /api/v1/submissions/{id}/verdict` after a WebSocket drop. See [`./api-gateway.md#5-failure-modes`](./api-gateway.md#5-failure-modes) for the coupling.
2. **WebSocket push.** Holds a STOMP-over-SockJS session per contestant on `/ws` and forwards each verdict to `/topic/verdicts/{userId}` — _but only when that user's session is on this instance_. The "is this user connected here" gate is the local `VerdictConnectionRegistry`; if not, the push is skipped and the HTTP fallback covers them on reconnect. "WebSocket fast path + HTTP reliable fallback".
3. **Leaderboard reads.** Serves `GET /api/v1/leaderboard/{contestId}` from Redis sorted-sets — `ZREVRANGE` for the page, `ZREVRANK` + per-shard `ZCARD` for "where am I". Reads route through a read-replica template when configured; writes (verdict-cache SET, Pub/Sub) always go to the primary.
4. **Stand-in leaderboard writer (M1 closure).** On each ACCEPTED `system`/`final` verdict, ZADDs the user into the score-range ZSETs and PUBLISHes on `score_updates:{contestId}`. Penalty = 0; the proper ICPC penalty math lives in scoring-pipeline. Behind feature flag `app.leaderboard.writer.enabled` — flipped OFF when scoring-pipeline (Flink) takes over the writes. See [§3.6 Stand-in writer (M1 cutover prep)](#36-stand-in-writer-m1-cutover-prep).

Historical note (audited 2026-05-19 by agent `af394dca`): an earlier version of this page asserted that "this service does NOT compute scores or ZADD; scoring-pipeline is the writer". That was aspirational — the code had never implemented either side. With M1 closing, the stand-in writer above is now live in code; scoring-pipeline (PR #8 productionising the Flink job) remains the target architecture and takes over when its parity is observed. The two writers share the same shard router, Lua key/arg shape, and score encoding, so brief overlap during cutover is no-op.

---

## 2. External interfaces

### 2.1 Kafka

| Direction | Topic | Group / role | Wire format |
|---|---|---|---|
| Consume | `evaluated_results` | group `leaderboard-verdict-push`, default concurrency 1 | proto `VerdictEvent` (Part 2 closure — was JSON via CRDB changefeed pre-2026-05-08) |

`auto-offset-reset: latest` — only deliver new verdicts to live clients. Missed verdicts during an outage are deliberately not replayed; the owning tab has long since reconnected and pulled the cached verdict via HTTP. `enable-auto-commit: true`: push is fire-and-forget. The durable record is `submissions.verdict` in CRDB + the Redis cache.

Co-partitioned routing matters with multiple instances: the WebSocket LB hashes user-id the same way the Kafka consumer-group partitioner does, so the verdict and the live WebSocket land on the same instance. See `common/sharding/PartitionAssigner`.

### 2.2 WebSocket

- **Endpoint.** `WS /ws` (SockJS-wrapped STOMP). Allowed origins `*` today; tighten via CORS when behind a real frontend.
- **Auth.** STOMP `CONNECT` carries a `userId` native header set by the browser after JWT handshake. CONNECT without it is allowed (anonymous leaderboard spectators) but no per-user push lands — `VerdictConnectionRegistry` only registers when `userId` is present.
- **Destinations.** `/topic/verdicts/{userId}` (per-user push, JSON transcoded from proto) and `/topic/leaderboard/{contestId}` (score-update broadcast from `ScoreUpdateSubscriber`).

Wire shape on `/topic/verdicts/{userId}`: `{submissionId, userId, problemId, contestId, result, executionTimeMs, memoryUsedMb, gatewayTsMs, points, phase, region}`. Identical to what `GET /api/v1/submissions/{id}/verdict` returns from the Redis cache, so the browser parses one shape either way.

### 2.3 REST API

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/leaderboard/{contestId}?page=N&size=M` | Paged leaderboard. Default size 100, hard cap 500. Pure Redis ZREVRANGE across score-range shards — no DB. |
| `GET /api/v1/leaderboard/{contestId}/rank/{userId}` | One user's current rank + decoded points + penalty. |
| `GET /actuator/health` | Spring Boot actuator; UP once Lettuce can reach the primary. |

### 2.4 Listening surface

- `:8082` — Tomcat (REST + the SockJS handshake).
- No additional ports. The Kafka consumer + Pub/Sub subscriber are outbound clients only.

---

## 3. Internal design

### 3.1 The verdict-push pipeline

`VerdictPushConsumer.onVerdict` is the hot path. Per record:

1. `VerdictEvent.parseFrom(record.value())`. If `userId` is empty, drop and log.
2. Transcode proto → JSON (`ObjectNode`). Browser sees JSON; only the Kafka wire format changed in Part 2.
3. **Cache then push.** `SET verdict:{submissionId}` (TTL 24 h) runs FIRST. Avoids the race where the client reconnects, hits the HTTP fallback, and sees 404 because the push happened before the cache write.
4. `connectionRegistry.isConnectedLocally(userId)`. If false, return — HTTP fallback handles it.
5. `messagingTemplate.convertAndSend("/topic/verdicts/" + userId, payload)`.

**Note the disagreement with tech-spec §5.4**, which says `verdict-cache:{submissionId}` with a 1 h TTL. The code uses `verdict:{submissionId}` with 24 h TTL. The code is the source of truth.

### 3.2 Connection registry

`VerdictConnectionRegistry` is two `ConcurrentHashMap`s: `userId → sessionId` (gate the push consults) and `sessionId → userId` (cheap unregister). Driven by the STOMP CONNECT/DISCONNECT interceptor in `WebSocketConfig.configureClientInboundChannel`.

Second CONNECT for the same user silently replaces the first — last-CONNECT-wins. Whether that's the right product policy (push to all tabs or just the latest?) is open; today the simpler choice.

### 3.3 Score-update Pub/Sub fan-out

`RedisConfig.redisListenerContainer` subscribes to `score_updates:*` on the **primary** (deliberately, not the replica — failing closed on the delivery-critical channel). `ScoreUpdateSubscriber.onMessage` extracts the contestId from the channel name and forwards the body to `/topic/leaderboard/{contestId}`.

The producer of `score_updates:{contestId}` is the stand-in `LeaderboardWriter` (§3.6) today, and becomes scoring-pipeline's `RedisLeaderboardSink` after cutover. Both writers publish the same JSON shape (`{userId, contestId, newScore, penaltyMinutes, newRank}`) via the same Lua, so the subscriber doesn't change. The `score_updates:user:{userId}` row in tech-spec §5.4 is still forward-looking — the pattern subscription would match it, but nothing publishes yet.

### 3.4 Score-range sharding for leaderboard reads

`LeaderboardService` does NOT read a single ZSET per contest. It reads **score-range shards** via `ScoreRangeShardRouter` (ICPC default: 3–5 shards by score band). Shard key: `leaderboard:{contestId}:shard:{shardIdx}`. Both this service (reader + stand-in writer) and scoring-pipeline (target writer) share the router so shard boundaries agree.

`getLeaderboardPage`: `ZCARD` each shard (3–5 RTTs); walk top-down accumulating counts; issue at most one `ZREVRANGE` per shard the page window crosses. Composite ZSET score is `points * 10_000_000 - penaltyMinutes`; decode via `Math.round(score / 10_000_000.0)` to avoid the integer-division boundary bug noted in `decodePoints`.

`getUserRank`: `ZSCORE` each shard until found, sum `ZCARD` of higher shards, `ZREVRANK` within the user's shard. Total O(S + log N).

### 3.5 Read-replica routing

`RedisConfig.readRedisTemplate` is a separate bean. When `app.redis.replica.host`/`port` are set, it talks to the replica; otherwise it delegates to the primary factory. `LeaderboardService` injects the read template; `VerdictPushConsumer` and `ScoreUpdateSubscriber` use the auto-configured primary. Pub/Sub stays on the primary so live updates never lag the read.

`spring.threads.virtual.enabled: true` — Java 21 virtual threads for Tomcat workers + STOMP message handling. Idle WebSocket sessions cost effectively nothing.

### 3.6 Stand-in writer (M1 cutover prep)

`LeaderboardWriter` is a `@Component` gated by `@ConditionalOnProperty(name="app.leaderboard.writer.enabled", havingValue="true", matchIfMissing=true)`. When the flag is true (default), the bean is wired into `VerdictPushConsumer` as an `ObjectProvider<LeaderboardWriter>` and consulted after the verdict-cache write, before the WebSocket push. When false, the bean drops out of the context entirely and `ObjectProvider.getIfAvailable()` returns `null` — the cache + push paths keep running.

**Per-verdict steps** (only on ACCEPTED with `phase ∈ {system, final}` and `points > 0`):

1. **Idempotency.** `SADD processed:{contestId} {submissionId}` — if it returns 0, the verdict is a Kafka redelivery; skip the rest. TTL refreshed to 24 h on every successful add.
2. **Accumulate.** `HINCRBY leaderboard:state:{contestId} {userId} {points}` returns the user's new total points across the contest. TTL refreshed to 24 h.
3. **Encode + route.** `zsetScore = totalPoints * 10_000_000 - 0` (penalty = 0 in the stand-in). `targetShard = ScoreRangeShardRouter.defaultIcpcRouter().shardForScore(zsetScore)` — same router instance `LeaderboardService` reads from and scoring-pipeline's `RedisLeaderboardSink` writes to.
4. **Atomic Lua.** KEYS = `[score_updates:{contestId}, leaderboard:{contestId}:shard:{target}, ...otherShardKeys]`. ARGV = `[userId, zsetScore, contestId, totalPoints, "0"]`. The Lua body is **byte-identical** to `RedisLeaderboardSink.LUA_UPDATE_SCORE` in scoring-pipeline — it ZREMs the user from non-target shards (boundary-cross cleanup keeps the user in exactly one shard), ZADDs to the target, ZREVRANKs, and PUBLISHes the score-change message on the Pub/Sub channel.
5. **Metric.** `oj.leaderboard.writer.writes_total{contest_id, verdict}` ++.

**Cutover procedure** (after scoring-pipeline / PR #8 is up):

| Step | Operator action | What to watch |
|---|---|---|
| 1 | Submit a couple of submissions on a synthetic contest. | Verify both writers are running: `oj.leaderboard.writer.writes_total` (this service) and `oj.scoring.score_updates_total` (Flink) both increment. |
| 2 | Compare the rates on the operator dashboard. Target: same rate for ≥5 contests over ≥24 h. | The two writers compute the same composite score from the same `(totalPoints, 0)` pair (stand-in) vs `(totalPoints, penaltyMinutes)` (Flink). Penalty math differs, so ZSET scores can diverge per user; rate parity is the criterion, not value parity. |
| 3 | Uncomment `APP_LEADERBOARD_WRITER_ENABLED: "false"` in [`infra/gcp/compose/region.yml`](../../infra/gcp/compose/region.yml) and `docker compose up -d oj-leaderboard-service`. | The `LeaderboardWriter` bean drops out at startup; the `@KafkaListener` keeps running. Tail logs for any `BeanCreationException`; verify `oj.leaderboard.writer.writes_total` flatlines while `oj.scoring.score_updates_total` continues. |
| 4 | (Optional cleanup) `DEL processed:{contestId}` and `leaderboard:state:{contestId}` for closed contests. | These keys are scratch state for the stand-in only; Flink doesn't use them. They TTL out 24 h after the last write, so manual cleanup is optional. |

**Idempotency / replay story.** The Kafka consumer is `auto-commit=true`, `auto-offset-reset=latest` — under nominal operation each verdict is delivered once. Rebalances and partition-revoke retries can redeliver. The SADD-based dedupe guards against this without coordinating with Flink: the dedupe key namespace (`processed:{contestId}`) is local to this service. After cutover, Flink uses its own keyed-state idempotency (a re-applied ScoreUpdate produces the same ZSET state because WA-times are stored in a TreeSet); the dedupe set is only a stand-in concern.

**Why penalty = 0 and not real ICPC math?** Real first-AC-wins + pre-AC-WA × 20 min penalty needs the keyed per-(user, problem) state that scoring-pipeline already implements (`ScoringState`, `ProblemScoreState`). Replicating it inside a `@KafkaListener` would mean two divergent implementations of contest scoring with subtly different bug surface. The stand-in is documented as "simple ZADD-by-points" — it gets ranks ordered correctly in points-tier space; penalty disambiguation arrives with Flink.

**Code map.** `leaderboard-service/src/main/java/com/onlinejudge/leaderboard/writer/LeaderboardWriter.java`; tests at `.../writer/LeaderboardWriterTest.java` and `LeaderboardWriterDisabledTest.java`.

---

## 4. Data ownership

| Resource | Lifetime | Where | This service |
|---|---|---|---|
| `verdict:{submissionId}` | per submission, TTL 24 h | Redis primary | **WRITES** (consumer) — also read by api-gateway on the HTTP-fallback path |
| `leaderboard:{contestId}:shard:{i}` | per contest, no TTL | Redis primary (writes) / replica (reads) | **WRITES** via the stand-in `LeaderboardWriter` (flag-gated; default on); also **READS** for the leaderboard / rank endpoints. Will become read-only when scoring-pipeline / Flink takes over and `app.leaderboard.writer.enabled=false`. |
| `score_updates:{contestId}` | Pub/Sub channel | Redis primary | **WRITES** via the Lua PUBLISH inside the stand-in writer; **SUBSCRIBES** via `ScoreUpdateSubscriber` for the `/topic/leaderboard/{contestId}` fan-out. After cutover, scoring-pipeline writes; this service stays the subscriber. |
| `processed:{contestId}` (SET) | per contest, TTL 24 h | Redis primary | **WRITES** — Kafka-redelivery dedupe set used by the stand-in writer only. Goes away when the stand-in is flipped off. |
| `leaderboard:state:{contestId}` (HASH) | per contest, TTL 24 h | Redis primary | **WRITES** — per-user running points hash used by the stand-in writer only (penalty-less running total). Scratch state; not consumed by anything else. |
| `score_updates:user:{userId}` | Pub/Sub channel | Redis primary | Wired via pattern; no producer yet |
| Kafka consumer offsets | per group | Kafka `__consumer_offsets` | Owned by group `leaderboard-verdict-push`; committed via auto-commit |
| `userId → sessionId` map | process lifetime | JVM heap (`VerdictConnectionRegistry`) | Per-instance, ephemeral |

leaderboard-service does **NOT** touch:
- CRDB (no JDBC driver on the classpath; submissions / users / contests are someone else's data).
- Kafka producers (consume only).
- GCS.
- Any other service's REST API.

Cross-reference: [`../tech-spec.md#54-redis-keys`](../tech-spec.md#54-redis-keys) for the full key catalogue.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| Redis primary down | Lettuce throws; consumer logs `RedisConnectionFailureException` | Consumer keeps consuming (auto-commit advances the offset); cache writes drop for the duration. Live WebSocket pushes still land. Reconnects in the outage window see 404 from the HTTP fallback. Verdict is not lost — `submissions.verdict` in CRDB is authoritative; api-gateway's reconciliation scanner backfills. |
| Redis replica down (when configured) | Lettuce on the read template fails; primary still serves writes | Leaderboard `GET` returns 500. Tactical fix: clear `app.redis.replica.host`, restart. |
| Replication lag visible to user | Read against replica hasn't seen the scoring-pipeline's ZADD yet | User sees the previous-cycle rank. Self-heals within a second. Leaderboard is eventually-consistent by design. |
| WebSocket client disconnects | SockJS heartbeat times out → STOMP DISCONNECT → `unregisterBySession` | Registry cleared. Next verdict for this user goes via HTTP fallback until reconnect. |
| Connection registry stale (DISCONNECT race vs push) | `convertAndSend` to a dead session | Spring drops silently. The cache write already happened; HTTP fallback covers it. |
| Pub/Sub fan-out missed (subscriber dropped) | `ScoreUpdateSubscriber.onMessage` skipped | WebSocket clients miss the live broadcast; next REST poll sees the truth. Leaderboard is "best effort live", never the source of truth. |
| Contest with millions of users on a single shard | `ZADD` O(log N), `ZREVRANGE [0,100]` O(log N + 100) — fine | Memory is the limit. 1M users × ~40 B/entry ≈ 40 MB per shard. Reshard if any shard exceeds 5M. |
| Kafka broker down | Consumer pauses | On return, consumer resumes from last committed offset. `auto-offset-reset: latest` means backlog is dropped — acceptable; HTTP fallback covers. |
| Container won't start | `BeanCreationException` from `LettuceConnectionFactory` (replica unreachable) | Crash-loop. Compose `depends_on: redis` covers primary; replica is separate. |
| api-gateway HTTP fallback miss | Cache key absent — TTL elapsed (24 h) or consumer was down | api-gateway falls back to `submissions.verdict` in CRDB. Slower, still correct. Cache is an optimisation, not a contract. |

---

## 6. Configuration reference

`leaderboard-service/src/main/resources/application.yml`; env overrides via Spring relaxed binding. Defaults shown.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8082` | Tomcat port. Override via `SERVER_PORT`. |
| `spring.threads.virtual.enabled` | `true` | Java 21 virtual threads for Tomcat + STOMP. |
| `spring.data.redis.host` / `.port` | `localhost` / `6379` | Primary Redis. Compose sets `redis:6379`. |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Compose sets `kafka:29092`. |
| `spring.kafka.consumer.group-id` | `leaderboard-verdict-push` | Group for `evaluated_results`. |
| `spring.kafka.consumer.auto-offset-reset` | `latest` | Never replay history. Missed verdicts route through HTTP fallback. |
| `spring.kafka.consumer.enable-auto-commit` | `true` | Fire-and-forget; durable record is CRDB + Redis. |
| `spring.kafka.consumer.value-deserializer` | `ByteArrayDeserializer` | Proto bytes; parsed by `VerdictEvent.parseFrom`. |
| `app.kafka.topic.evaluated-results` | `evaluated_results` | Verdict source. |
| `app.leaderboard.default-page-size` | `100` | Hard cap 500 in `LeaderboardService`. |
| `app.leaderboard.writer.enabled` | `true` (override: `APP_LEADERBOARD_WRITER_ENABLED`) | Stand-in `LeaderboardWriter` bean toggle. `true` (default) = this service writes the ZSETs; `false` = scoring-pipeline (Flink) owns the writes. See §3.6 for cutover. |
| `app.redis.replica.host` / `.port` | empty / `0` | Optional read-replica. Empty → reads go to primary. |
| `JAVA_TOOL_OPTIONS` | `-Xmx256m -XX:+ExitOnOutOfMemoryError` (compose) | Heap. Dockerfile default `-XX:MaxRAMPercentage=70`; compose overrides explicitly because the control-plane VM is memory-tight. |
| `OTEL_JAVAAGENT_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` | `false` / `http://oj-otel-collector:4317` | OTLP gRPC via java agent. |
| `app.region` | `${REGION:-asia-south1}` | Stamped on emitted metrics. |

---

## 7. Metrics emitted

Today the service emits Spring Boot defaults (Tomcat / JVM / Lettuce) plus the writer counter below. The rest of the catalogue is the **proposed** `oj.leaderboard.*` namespace; broader instrumentation is a roadmap item alongside the OTEL collector deploy ([`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md)).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.leaderboard.writer.writes_total` | counter | `contest_id`, `verdict` | **Live.** Stand-in `LeaderboardWriter` writes (one increment per ZADD-via-Lua). The cutover-parity tile compares this rate against `oj.scoring.score_updates_total` from Flink. Flatlines when `app.leaderboard.writer.enabled=false`. |
| `oj.leaderboard.verdicts.consumed_total` | counter | `phase`, `result` | One per `VerdictEvent` consumed off `evaluated_results`. Sustained zero in a live contest = consumer wedged. |
| `oj.leaderboard.verdict_cache.write_latency_seconds` | histogram | (none) | Latency of the `SET verdict:{id}` op. P99 above 50 ms = Redis hot. |
| `oj.leaderboard.ws.sessions_active` | gauge | (none) | `VerdictConnectionRegistry.localConnectionCount()` snapshot. Per-instance. |
| `oj.leaderboard.ws.push_total` | counter | `outcome` | `outcome` ∈ `{pushed, skipped_not_local, skipped_no_user_id}`. The `skipped_not_local` count is the rate at which HTTP fallback will be used. |
| `oj.leaderboard.pubsub.broadcast_total` | counter | (none) | One per `score_updates:*` message forwarded to `/topic/leaderboard/{contestId}`. Should track the scoring-pipeline ZADD rate. |
| `oj.leaderboard.read.latency_seconds` | histogram | `endpoint` | End-to-end handler latency for the two REST endpoints. Drives the leaderboard-load dashboard panel. |
| `oj.leaderboard.shard.read_count` | histogram | (none) | Per-page-request count of `ZREVRANGE`s issued — should be 1 most of the time, 2 when the window spans a shard boundary. |

---

## 8. Runbook

### 8.1 "Leaderboard shows stale rank for one user"

**Symptom.** Verdict landed (CRDB row updated; `verdict:{id}` present in Redis) but `GET /leaderboard/{contestId}/rank/{userId}` shows the old rank.

**Diagnose.**
```sh
sudo docker exec oj-redis redis-cli GET "verdict:<submissionId>"
for i in 0 1 2 3 4; do
  sudo docker exec oj-redis redis-cli ZSCORE "leaderboard:<contestId>:s$i" "<userId>"
done
sudo docker exec oj-redis redis-cli INFO replication | grep -E "role|offset"
```

**Fix.** With the stand-in writer ON (default, pre-Flink): if `oj.leaderboard.writer.writes_total` is flatlining, the writer is misfiring — check logs for `[lb-writer]` warnings and confirm `app.leaderboard.writer.enabled=true`. Verify the verdict actually reached this service (`oj.leaderboard.verdicts.consumed_total`). Common cause: the verdict had `phase=pretest`, which is intentionally non-scoreable — the rank updates only on the later `system` verdict. With the stand-in OFF (post-cutover): if the ZADD never landed, scoring-pipeline is wedged (see [`./scoring-pipeline.md`](./scoring-pipeline.md) §8). If replica lag is the cause, clear `REDIS_REPLICA_HOST` and restart.

### 8.2 "WebSocket sessions dropping en masse"

**Symptom.** `oj.leaderboard.ws.sessions_active` goes to zero across the fleet.

**Diagnose.** `sudo docker logs oj-leaderboard-service --tail 200 | grep -E "DISCONNECT|HEARTBEAT|interrupted"`.

**Likely cause & fix.** Container restart (`docker inspect` for exit code); Tomcat connector overload (bump `server.tomcat.max-connections`); GCP VM networking flap (SockJS clients heartbeat-timeout, auto-reconnect on the next interval — no action).

### 8.3 "Redis OOM — sorted-set too large"

**Symptom.** `MEMORY USAGE leaderboard:{contestId}:shard:{idx}` is huge; Redis OOMs.

**Diagnose.** `sudo docker exec oj-redis redis-cli --bigkeys` + `INFO memory`.

**Fix.** Per-shard cardinality > 5M → resize the score router (tighten boundaries in `ScoreRangeShardRouter`; needs coordinated change with scoring-pipeline). Stale contests not expired → add explicit `DEL` on contest close from contest-service (shard keys have no TTL today).

### 8.4 "Replica drift / read-after-write inconsistency"

**Symptom.** User submits, gets ACCEPTED on the WebSocket, refreshes the leaderboard immediately and doesn't see their new score.

**Cause.** Expected — async replication. ZADD goes to primary; REST read goes to replica. Tolerance ~hundreds of ms. UX fix: let the controller pick the primary template on a post-submit query via a "consistency hint" header (future change; today the route is hard-coded to the replica).

### 8.5 "Container won't start"

**Symptom.** `docker logs oj-leaderboard-service` shows `LettuceConnectionFactory` exception or `BeanCreationException`.

**Likely cause & fix.** Replica configured but unreachable → clear the env var or stand up the replica. Kafka broker down at boot → container should still come up; consumer re-poll loops. Memory cap too tight (`-Xmx256m` in compose) → bump to `-Xmx384m` under load.

### 8.6 "verdict-cache miss on api-gateway status query"

**Symptom.** api-gateway logs `verdict-cache miss; falling back to CRDB`.

**Diagnose.** `redis-cli GET "verdict:<submissionId>"` + `TTL`.

**Likely cause.** Verdict landed while leaderboard-service was down (`auto-offset-reset: latest` skipped it); cache TTL elapsed (24 h); wrong submissionId. CRDB is the source of truth — fallback is correct, just slower.

---

## 9. Tests & verification

### 9.1 Unit tests (`leaderboard-service/src/test/java/`)

| File | Coverage |
|---|---|
| `VerdictPushConsumerTest` | Proto parse; cache-then-push ordering; not-connected-locally branch; missing-userId drop |
| `LeaderboardWriterTest` | Lua KEYS / ARGV shape; scoreable-phase filter (system / final yes, pretest no); ACCEPTED-only; missing-id drops; zero-points skip; SADD-based idempotency on Kafka redelivery |
| `LeaderboardWriterDisabledTest` | With `app.leaderboard.writer.enabled=false`: the writer bean is absent, the verdict-push consumer bean is still present |
| `VerdictConnectionRegistryTest` | Register / unregister; tab-refresh replacement; `isConnectedLocally` truth table; concurrent register from multiple sessions |
| `LeaderboardServiceTest` | Multi-shard page assembly; cross-shard window; user-rank composition across shards; participant-count sum; composite-score decode round-trip |
| `RedisConfigTest` | Replica template binds to replica host/port when configured; falls through to primary otherwise |
| `VerdictPushIntegrationTest` | Embedded Kafka + Redis testcontainer end-to-end: produce VerdictEvent → assert cache write + STOMP push |
| `LeaderboardShardingIntegrationTest` | Multi-shard read against a real Redis testcontainer; rank arithmetic across shard boundaries |

Run via `./gradlew :leaderboard-service:test`.

### 9.2 Manual smoke

Subscribe to `/topic/verdicts/<userId>` from a browser console connected to `ws://oj-control-plane:8082/ws`, then publish a synthetic VerdictEvent to `evaluated_results` via `kafka-console-producer`. Confirm `redis-cli GET "verdict:<submissionId>"` returned the cached JSON; the browser should log the same payload within hundreds of ms.

For the leaderboard read path: `curl http://oj-control-plane:8082/api/v1/leaderboard/<contestId>?page=0&size=10` and `…/rank/<userId>`. With the stand-in writer ON (default), submitting an ACCEPTED system-test verdict on a problem with non-zero `points` populates the user's rank within milliseconds. Pretest-only ACCEPTED verdicts do **not** show up — that's by design (mirrors scoring-pipeline's phase filter).

---

## 10. Relevant design docs

- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — the regional Redis story. Each region has its own primary; cross-region leaderboard merge is a roadmap item.
- [`../design-docs/react-spa-and-websockets.md`](../design-docs/react-spa-and-websockets.md) — the SockJS + STOMP client design; CORS hardening; auth header injection.
- [`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md) — defines the proposed `oj.leaderboard.*` metric catalogue in §7.
- The scoring-pipeline coupling is documented inline in [`../tech-spec.md#47-scoring-pipeline`](../tech-spec.md) and in [`./scoring-pipeline.md`](./scoring-pipeline.md). The §3.6 stand-in writer is the M1 closure; when scoring-pipeline deploys and the flag flips, §3.6 collapses to a "historical" subsection and §4's `leaderboard:{contestId}:shard:{i}` row goes back to READ-only.

---

## 11. Code map

| Concern | File |
|---|---|
| Kafka consumer + cache-then-push | `leaderboard-service/src/main/java/com/onlinejudge/leaderboard/service/VerdictPushConsumer.java` |
| Stand-in ZADD-by-points writer (flag-gated) | `.../writer/LeaderboardWriter.java` |
| Per-instance userId→session registry | `.../service/VerdictConnectionRegistry.java` |
| Pub/Sub subscriber → WebSocket broadcast | `.../service/ScoreUpdateSubscriber.java` |
| Leaderboard read service (multi-shard) | `.../service/LeaderboardService.java` |
| REST controller | `.../controller/LeaderboardController.java` |
| Redis primary/replica + Pub/Sub bean wiring | `.../config/RedisConfig.java` |
| STOMP endpoint + CONNECT/DISCONNECT interceptor | `.../config/WebSocketConfig.java` |
| Score-range shard router (shared with scoring-pipeline) | `common/src/main/java/com/onlinejudge/common/sharding/ScoreRangeShardRouter.java` |
| Spring main class | `.../LeaderboardApplication.java` |
| Dockerfile | `leaderboard-service/Dockerfile` (multi-stage; gradle build → eclipse-temurin runtime) |
| Compose entry | `infra/gcp/compose/control-plane-compose.yml` (the `leaderboard-service:` block at line 253) |
