# analytics-pipeline

> **Owner page.** Last reconciled with the repo on **2026-05-18**. Cross-cutting concerns (proto schema, Kafka topic catalogue, multi-region rollout) live in [`../tech-spec.md`](../tech-spec.md). The producer side — execution-worker — is at [`./execution-worker.md`](./execution-worker.md).

> **Status (2026-05-18): NOT DEPLOYED.** The module compiles and has unit-test coverage. The `analytics_events` Kafka topic is produced by execution-worker on every verdict (fire-and-forget, separate consumer group). No consumer runs today: no `Dockerfile`, no entry in `infra/gcp/compose/control-plane-compose.yml`, no ClickHouse instance provisioned. Events accumulate on the topic and are eventually deleted per its 7-day retention. Wiring this in is a prerequisite for reporting / BI integration; not on the v1 critical path (`../tech-spec.md` "Known gaps" — *"analytics-pipeline not deployed — Low — produced topic data is buffered for later"*).

---

## 1. Purpose

Consume the `analytics_events` Kafka topic and persist one long-lived row per submission into a columnar store, so offline reporting (verdict mix by language, per-problem ACCEPTED rate, p99 execution-time per language) runs as SQL without joining live operational tables in CRDB.

Wire schema: `AnalyticsEvent` in [`common/src/main/proto/events.proto`](../../common/src/main/proto/events.proto), field tags 1–11 (`submission_id`, `user_id`, `problem_id`, `contest_id`, `language`, `verdict`, `execution_time_ms`, `memory_used_mb`, `event_ts_ms`, `region`, `phase`). Slimmer than `VerdictEvent` — no per-test breakdown, no `stdout_hash`. Decoupling from the verdict topic lets the analytics sink evolve independently of the hot path.

§2 onwards describes intended behaviour once the service is wired in.

---

## 2. External interfaces

**Inbound.** Kafka topic `analytics_events`. Key: `submission_id`. Value: protobuf-serialised `AnalyticsEvent`. Consumer group: `analytics-pipeline`. `enable-auto-commit: false`; manual ack after buffer enqueue. `auto-offset-reset: earliest`. Topic config (8 partitions, 7-day retention) in `../tech-spec.md` "Kafka topic catalogue".

**Outbound.** ClickHouse via HTTP. `POST {clickHouseUrl}/` with body `INSERT INTO {database}.submission_analytics (submission_id, user_id, problem_id, contest_id, language, verdict, execution_time_ms, memory_used_mb, event_time) FORMAT TabSeparated\n<rows>`. `clickhouse-jdbc:0.6.0` is on the classpath but unused at runtime; the writer uses `java.net.http.HttpClient`. No CRDB, Redis, or GCS.

**Listening surface.** `:8083` — Spring Boot actuator only. Not reached by any other service.

Intended production pattern: *Kafka Engine + Materialized View* — ClickHouse pulls directly from Kafka, no JVM consumer. The Spring-Boot-plus-HTTP-insert shape here is the simpler equivalent; `ClickHouseWriter`'s Javadoc states this.

---

## 3. Internal design

Spring Boot, not Flink — distinct from scoring-pipeline. Two classes carry all behaviour.

**`AnalyticsConsumer`.** `@KafkaListener(topics = "${app.kafka.topic.analytics}", groupId = "analytics-pipeline", concurrency = "2")`. Each `consume(record, ack)`: `AnalyticsEvent.parseFrom(...)` → flatten into `Map<String,Object>` keyed by column name → `clickHouseWriter.buffer(record)` → `ack.acknowledge()`. The `catch (Exception)` branch **still acks**. A poison record must not block the consumer group. Contrast the verdict path in execution-worker, which retries + DLQs.

**`ClickHouseWriter`.** `CopyOnWriteArrayList<Map<String,Object>>` buffers rows. Size-based trigger: `buffer.size() >= batchSize` (default 100) calls `flush()` synchronously inside `buffer()`. Time-based: `@Scheduled(fixedDelayString = "${app.clickhouse.flush-interval-ms:5000}")` every 5 s.

`flush()` is `synchronized`. Snapshots and clears the buffer, serialises rows as TSV (`event_ts_ms` via `Instant.ofEpochMilli(...)`), POSTs the `INSERT ... FORMAT TabSeparated` query. On non-200 or exception, **the batch is re-buffered**. At-least-once at the sink on top of at-least-once at Kafka — duplicates expected; the downstream table is meant to deduplicate via `ReplacingMergeTree` on `submission_id`, or the production `AggregatingMergeTree` pattern in the Javadoc. Batching matters because ClickHouse is built for large batch inserts.

Spring Boot fits because every event is an independent row insert: no joins, no windows, no state. Once the Kafka-Engine pattern lands, the JVM process disappears entirely.

---

## 4. Data ownership

Read: `analytics_events` topic. Write: `submission_analytics` table in ClickHouse (default database `onlinejudge`). Plus an in-memory row buffer on the JVM heap, lost on restart. Does NOT touch CRDB, Redis, GCS, any other Kafka topic, or `evaluated_results` / `verdicts` / leaderboard.

The in-memory buffer is the only ephemeral state. Kafka offsets are acked after `buffer()`, not after `flush()` — so a crash between ack and the next successful flush loses rows. **Known gap.** Mitigations: keep `batch-size` small, or move to the Kafka-Engine pattern (gives ClickHouse direct offset ownership and eliminates the gap).

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| **Not deployed (today)** | No consumer-group offset advance | Topic accumulates; deleted at 7-day retention; rows lost. Acceptable for v1. |
| ClickHouse sink down | `HttpClient.send` throws or non-200 | Batch re-buffered. Kafka offsets safe; backlog recovers on sink recovery. |
| ClickHouse schema mismatch | 400 `Unknown identifier` | Same re-buffer loop. Catch via `[clickhouse] Insert failed` log alerts. |
| AnalyticsEvent additive change | `parseFrom` ignores unknown fields (proto3) | Row written; new column absent. Safe. |
| AnalyticsEvent tag re-used | `parseFrom` parses wrong type | Garbage values. Mitigation: CI guard against tag changes. |
| Malformed record | `InvalidProtocolBufferException` | Logged, ack'd, dropped. |
| JVM crash with pending buffer | Process exit | Buffered rows lost; offsets already acked. |

---

## 6. Configuration reference

`analytics-pipeline/src/main/resources/application.yml`; env override via Spring relaxed binding.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8083` | Actuator port. |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Override `SPRING_KAFKA_BOOTSTRAP_SERVERS=oj-kafka:29092` in compose. |
| `spring.kafka.consumer.group-id` | `analytics-pipeline` | Distinct from `scoring-pipeline` and `leaderboard-service`. |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | First-run replays from beginning. |
| `spring.kafka.consumer.enable-auto-commit` | `false` | Manual ack. |
| `spring.threads.virtual.enabled` | `true` | Virtual threads for the listener container. |
| `app.kafka.topic.analytics` | `analytics_events` | Source topic. Must match execution-worker's `app.kafka.topic.analytics`. |
| `app.clickhouse.url` | `http://localhost:8123` | ClickHouse HTTP endpoint. **Production target TBD — no ClickHouse provisioned today.** |
| `app.clickhouse.database` | `onlinejudge` | Target database. |
| `app.clickhouse.batch-size` | `100` | Flush threshold (rows). |
| `app.clickhouse.flush-interval-ms` | `5000` | Time-based flush cadence. |

---

## 7. Metrics emitted

Proposed (none implemented today beyond Spring Boot defaults). All prefixed `oj.analytics.*`.

- `oj.analytics.events.consumed_total` (counter, labels: `verdict`, `language`) — one per record; sustained zero with non-zero producer = consumer down.
- `oj.analytics.events.parse_errors_total` (counter) — malformed proto records dropped; > 0 = upstream proto drift.
- `oj.analytics.buffer.size` (gauge) — in-memory buffer depth; sustained > `batch-size` = sink struggling.
- `oj.analytics.flush.latency_seconds` (histogram) — HTTP-POST latency.
- `oj.analytics.flush.failed_total` (counter, label: `status_code`) — non-200 + IOExceptions; alert on rate > 0.
- `oj.analytics.kafka.consumer_lag` (gauge, label: `partition`) — standard Kafka lag.

Once deployed, one "consumer-lag" tile on the ops dashboard suffices — analytics is not on the hot path.

---

## 8. Runbook

### 8.1 "Topic backlog growing — no consumer running"

Expected today. `kafka-consumer-groups.sh --describe --group analytics-pipeline` returns "consumer group not found". **Fix.** Wire the service in: author a `Dockerfile` mirroring `leaderboard-service/Dockerfile`; add an `analytics-pipeline:` block to `infra/gcp/compose/control-plane-compose.yml`; provision ClickHouse and create `submission_analytics`; set `APP_CLICKHOUSE_URL`; deploy. Backlog drains from `earliest` — up to 7 days of buffered events on first run.

### 8.2 "ClickHouse writes failing — buffer growing unbounded"

`[clickhouse] Insert failed status=` or `[clickhouse] Flush error:` in logs; heap growing. Check `docker logs` and `curl -v "${APP_CLICKHOUSE_URL}/?query=SELECT+1"`. Causes: schema mismatch (reapply DDL; restart JVM to clear stale buffer — offsets already acked); ClickHouse OOM / disk full (`ALTER TABLE ... DROP PARTITION`); network partition (bounce the JVM if heap is threatened).

### 8.3 "Container crashes on startup"

Spring Boot startup exception. Verify env vars (`APP_KAFKA_TOPIC_ANALYTICS=analytics_events`, `APP_CLICKHOUSE_URL`) and Kafka reachable.

### 8.4 "AnalyticsEvent format drift"

All records dropped with `InvalidProtocolBufferException`. Rebuild against the same `events.proto` generation that execution-worker ships. Proto3 additive evolution is safe; this only fires when a tag is repurposed.

---

## 9. Tests & verification

Unit tests (`analytics-pipeline/src/test/java/`):

- `AnalyticsConsumerTest` — proto-parse → buffer + ack happy path; missing `contestId` → empty-string substitution (proto3 default); malformed bytes → ack without buffering.
- `ClickHouseWriterTest` — below batch-size does not flush; reaching batch-size triggers flush; empty-buffer scheduled flush is no-op; non-200 HTTP re-buffers; payload formatted as `INSERT ... FORMAT TabSeparated` POST.

Run via `./gradlew :analytics-pipeline:test`. No TestContainers-backed integration test.

Manual smoke (once deployed): produce a synthetic `AnalyticsEvent` to `analytics_events`, wait ~5 s, then `curl -s "${APP_CLICKHOUSE_URL}/?query=SELECT+*+FROM+onlinejudge.submission_analytics+WHERE+submission_id='smoke-sub-1'+FORMAT+JSONEachRow"`. Canonical e2e: submit via api-gateway, wait, query by `submission_id`.

---

## 10. Relevant design docs

No dedicated design doc — the architecture is settled (Spring consumer + ClickHouse HTTP insert, eventually Kafka Engine + Materialized View). [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) is relevant for the future case: per-region ClickHouse + global aggregator. The `AnalyticsEvent.region` field is in place; the aggregation topology is not yet designed.

---

## 11. Code map

| Concern | File |
|---|---|
| Spring main class | `analytics-pipeline/.../AnalyticsPipelineApplication.java` |
| Kafka listener | `.../consumer/AnalyticsConsumer.java` |
| ClickHouse batch writer | `.../service/ClickHouseWriter.java` |
| Configuration | `analytics-pipeline/src/main/resources/application.yml` |
| Build | `analytics-pipeline/build.gradle` (Spring Boot + spring-kafka + clickhouse-jdbc:0.6.0) |
| Producer side | `execution-worker/.../consumer/SubmissionConsumer.java` (search `analyticsTopic`) |
| Proto wire format | `common/src/main/proto/events.proto` (`message AnalyticsEvent`, tags 1–11) |
| Dockerfile | **not present** — author before deployment |
| Compose entry | **not present** — no block in `infra/gcp/compose/control-plane-compose.yml` |
| ClickHouse DDL for `submission_analytics` | **not present** — author before deployment |
