# leaderboard-service

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the leaderboard-service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the Redis key catalogue, the multi-region story) live in [`../tech-spec.md`](../tech-spec.md). The upstream verdict producer is documented at [`./execution-worker.md`](./execution-worker.md).
>
> **Status.** Dockerfile + image + compose entry shipped (commit `6be38f4`); not yet deployed live on the GCP environment as of 2026-05-18. The compose block in [`infra/gcp/compose/control-plane-compose.yml`](../../infra/gcp/compose/control-plane-compose.yml) is in place; the next control-plane VM bounce will bring it up.
>
> Read this page if you are: (a) on-call for the live leaderboard or the verdict-push WebSocket, (b) changing anything in `leaderboard-service/`, (c) thinking about how Redis keys are shaped for the scoring story.

---

## 1. Purpose

The fan-out tier between the verdict stream and the contestant's browser. Three responsibilities:

1. **Kafka consumer.** Pulls `VerdictEvent` from `evaluated_results` and caches it in Redis under `verdict:{submissionId}` (TTL 24 h). The api-gateway HTTP-fallback path reads this key when a client polls `GET /api/v1/submissions/{id}/verdict` after a WebSocket drop. See [`./api-gateway.md#5-failure-modes`](./api-gateway.md#5-failure-modes) for the coupling.
2. **WebSocket push.** Holds a STOMP-over-SockJS session per contestant on `/ws` and forwards each verdict to `/topic/verdicts/{userId}` — _but only when that user's session is on this instance_. The "is this user connected here" gate is the local `VerdictConnectionRegistry`; if not, the push is skipped and the HTTP fallback covers them on reconnect. "WebSocket fast path + HTTP reliable fallback".
3. **Leaderboard reads.** Serves `GET /api/v1/leaderboard/{contestId}` from Redis sorted-sets — `ZREVRANGE` for the page, `ZREVRANK` + per-shard `ZCARD` for "where am I". Reads route through a read-replica template when configured; writes (verdict-cache SET, Pub/Sub) always go to the primary.

What this service **doesn't** do: it does NOT compute scores or `ZADD`. The scoring-pipeline (Flink, blocked on a runtime — see [`../tech-spec.md#47-scoring-pipeline-blocked`](../tech-spec.md)) is the writer. Until that ships, the leaderboard read path returns empty. leaderboard-service is the read side and the push side; the scoring math lives elsewhere. The earlier inline tech-spec §4.6 described a simpler "ZADD-by-points-on-verdict-ingest" fallback model that the service _could_ do trivially but doesn't today. Reconcile against this page.

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

Today nothing publishes to `score_updates:{contestId}` — the producer is the not-yet-deployed scoring-pipeline. The subscriber is idle. The `score_updates:user:{userId}` row in tech-spec §5.4 is also forward-looking — the pattern subscription would match it, but nothing publishes yet.

### 3.4 Score-range sharding for leaderboard reads

`LeaderboardService` does NOT read a single ZSET per contest. It reads **score-range shards** via `ScoreRangeShardRouter` (ICPC default: 3–5 shards by score band). Shard key: `leaderboard:{contestId}:s{shardIdx}`. Both this service (reader) and scoring-pipeline (writer) share the router so shard boundaries agree.

`getLeaderboardPage`: `ZCARD` each shard (3–5 RTTs); walk top-down accumulating counts; issue at most one `ZREVRANGE` per shard the page window crosses. Composite ZSET score is `points * 10_000_000 - penaltyMinutes`; decode via `Math.round(score / 10_000_000.0)` to avoid the integer-division boundary bug noted in `decodePoints`.

`getUserRank`: `ZSCORE` each shard until found, sum `ZCARD` of higher shards, `ZREVRANK` within the user's shard. Total O(S + log N).

### 3.5 Read-replica routing

`RedisConfig.readRedisTemplate` is a separate bean. When `app.redis.replica.host`/`port` are set, it talks to the replica; otherwise it delegates to the primary factory. `LeaderboardService` injects the read template; `VerdictPushConsumer` and `ScoreUpdateSubscriber` use the auto-configured primary. Pub/Sub stays on the primary so live updates never lag the read.

`spring.threads.virtual.enabled: true` — Java 21 virtual threads for Tomcat workers + STOMP message handling. Idle WebSocket sessions cost effectively nothing.

---

## 4. Data ownership

| Resource | Lifetime | Where | This service |
|---|---|---|---|
| `verdict:{submissionId}` | per submission, TTL 24 h | Redis primary | **WRITES** (consumer) — also read by api-gateway on the HTTP-fallback path |
| `leaderboard:{contestId}:s{idx}` | per contest, no TTL | Redis primary (writes by scoring-pipeline once deployed) / replica (reads) | **READS ONLY** today |
| `score_updates:{contestId}` | Pub/Sub channel | Redis primary | **SUBSCRIBES** (no producer yet — will be the scoring-pipeline) |
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
| `app.redis.replica.host` / `.port` | empty / `0` | Optional read-replica. Empty → reads go to primary. |
| `JAVA_TOOL_OPTIONS` | `-Xmx256m -XX:+ExitOnOutOfMemoryError` (compose) | Heap. Dockerfile default `-XX:MaxRAMPercentage=70`; compose overrides explicitly because the control-plane VM is memory-tight. |
| `OTEL_JAVAAGENT_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` | `false` / `http://oj-otel-collector:4317` | OTLP gRPC via java agent. |
| `app.region` | `${REGION:-asia-south1}` | Stamped on emitted metrics. |

---

## 7. Metrics emitted

Today the service emits only the Spring Boot defaults (Tomcat / JVM / Lettuce). The catalogue below is the **proposed** `oj.leaderboard.*` namespace; instrumentation is a roadmap item alongside the OTEL collector deploy ([`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md)).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
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

**Fix.** Until scoring-pipeline deploys this is the expected state — leaderboard is empty. Once it deploys: if the ZADD never landed, scoring-pipeline is wedged (its runbook). If replica lag is the cause, clear `REDIS_REPLICA_HOST` and restart.

### 8.2 "WebSocket sessions dropping en masse"

**Symptom.** `oj.leaderboard.ws.sessions_active` goes to zero across the fleet.

**Diagnose.** `sudo docker logs oj-leaderboard-service --tail 200 | grep -E "DISCONNECT|HEARTBEAT|interrupted"`.

**Likely cause & fix.** Container restart (`docker inspect` for exit code); Tomcat connector overload (bump `server.tomcat.max-connections`); GCP VM networking flap (SockJS clients heartbeat-timeout, auto-reconnect on the next interval — no action).

### 8.3 "Redis OOM — sorted-set too large"

**Symptom.** `MEMORY USAGE leaderboard:{contestId}:s{idx}` is huge; Redis OOMs.

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
| `VerdictConnectionRegistryTest` | Register / unregister; tab-refresh replacement; `isConnectedLocally` truth table; concurrent register from multiple sessions |
| `LeaderboardServiceTest` | Multi-shard page assembly; cross-shard window; user-rank composition across shards; participant-count sum; composite-score decode round-trip |
| `RedisConfigTest` | Replica template binds to replica host/port when configured; falls through to primary otherwise |
| `VerdictPushIntegrationTest` | Embedded Kafka + Redis testcontainer end-to-end: produce VerdictEvent → assert cache write + STOMP push |
| `LeaderboardShardingIntegrationTest` | Multi-shard read against a real Redis testcontainer; rank arithmetic across shard boundaries |

Run via `./gradlew :leaderboard-service:test`.

### 9.2 Manual smoke

Subscribe to `/topic/verdicts/<userId>` from a browser console connected to `ws://oj-control-plane:8082/ws`, then publish a synthetic VerdictEvent to `evaluated_results` via `kafka-console-producer`. Confirm `redis-cli GET "verdict:<submissionId>"` returned the cached JSON; the browser should log the same payload within hundreds of ms.

For the leaderboard read path: `curl http://oj-control-plane:8082/api/v1/leaderboard/<contestId>?page=0&size=10` and `…/rank/<userId>`. Both return empty `entries` / `rank: -1` until scoring-pipeline starts writing.

---

## 10. Relevant design docs

- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — the regional Redis story. Each region has its own primary; cross-region leaderboard merge is a roadmap item.
- [`../design-docs/react-spa-and-websockets.md`](../design-docs/react-spa-and-websockets.md) — the SockJS + STOMP client design; CORS hardening; auth header injection.
- [`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md) — defines the proposed `oj.leaderboard.*` metric catalogue in §7.
- The scoring-pipeline coupling is documented inline in [`../tech-spec.md#47-scoring-pipeline-blocked`](../tech-spec.md). When scoring-pipeline deploys, this page's §3 will lose the "fallback ZADD" hedge and become read-side-only.

---

## 11. Code map

| Concern | File |
|---|---|
| Kafka consumer + cache-then-push | `leaderboard-service/src/main/java/com/onlinejudge/leaderboard/service/VerdictPushConsumer.java` |
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
