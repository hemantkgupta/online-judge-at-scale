# ClickHouse Kafka Engine rollout for `analytics-pipeline`

> **Purpose.** Replace the not-yet-deployed Spring Boot `analytics-pipeline` module with a JVM-free path: ClickHouse's built-in **Kafka Engine** consumes `analytics_events` directly, a **Materialized View** pushes rows into `submission_analytics`. This is the production target already documented in [`../services/analytics-pipeline.md`](../services/analytics-pipeline.md) §2 (Javadoc) and §10.
>
> **Audience.** A handover document for an LLM (or engineer) picking this up cold. Self-contained: includes context, decisions, the exact DDL, the compose wiring, and the test plan.

---

## 1. Context (read this first)

### What exists today

- **Producer.** [`execution-worker/.../consumer/SubmissionConsumer.java`](../../execution-worker/src/main/java/com/onlinejudge/executionworker/consumer/SubmissionConsumer.java) emits one `AnalyticsEvent` per verdict, on its own Kafka topic.
- **Topic.** `analytics_events` — 8 partitions, key = `submission_id`, 7-day retention. Defined in `infra/gcp/compose/.../kafka-bootstrap`. Distinct from `evaluated_results` (which carries the full `VerdictEvent` for scoring + leaderboard).
- **Wire format.** Protobuf — `message AnalyticsEvent` in [`common/src/main/proto/events.proto`](../../common/src/main/proto/events.proto), tags 1–11: `submission_id, user_id, problem_id, contest_id, language, verdict, execution_time_ms, memory_used_mb, event_ts_ms, region, phase`. Slimmer than `VerdictEvent` — no per-test breakdown, no `stdout_hash`.
- **Consumer.** [`analytics-pipeline/`](../../analytics-pipeline/) — a Spring Boot module with [`AnalyticsConsumer`](../../analytics-pipeline/src/main/java/com/onlinejudge/analytics/consumer/AnalyticsConsumer.java) (Kafka listener) and [`ClickHouseWriter`](../../analytics-pipeline/src/main/java/com/onlinejudge/analytics/service/ClickHouseWriter.java) (in-memory batch buffer + HTTP `INSERT … FORMAT TabSeparated`). **Compiles, unit-tested, NOT DEPLOYED** — no `Dockerfile`, no compose block, no ClickHouse provisioned.
- **Result today.** `analytics_events` accumulates and ages out at 7d retention. Per [`../tech-spec.md`](../tech-spec.md) §11.4 *"analytics-pipeline not deployed — Low — produced topic data is buffered for later."*

### Why we are replacing the Spring path

The Spring path acks Kafka **before** flushing to ClickHouse. At-least-once at Kafka stacks on top of at-least-once at the HTTP sink, and a JVM crash with a non-empty buffer loses the buffered rows (offsets are already committed). Dedupe is deferred to the table engine.

The Kafka Engine path closes that gap: ClickHouse advances the offset only after the materialized view writes succeed. There is no JVM to crash, no buffer to lose, no separate container to operate.

### Visual

[`../../../CSE-Raw/raw-blog/images/online-judge-at-scale/analytics-pipeline-1.svg`](../../../CSE-Raw/raw-blog/images/online-judge-at-scale/analytics-pipeline-1.svg) — two-panel diagram (today vs. target).

### Open-source / licensing

ClickHouse is **Apache 2.0**. The Kafka Engine, all `*MergeTree` variants, and Materialized Views are part of the standard `clickhouse/clickhouse-server` image. No paid tier, no feature gating, no enterprise license. The `librdkafka` client is bundled.

---

## 2. Architecture target

```
execution-worker
   │ produces protobuf AnalyticsEvent (tags 1–11)
   ▼
Kafka topic: analytics_events  (8 partitions · key=submission_id · 7d retention)
   │
   ▼
ClickHouse Kafka Engine table  onlinejudge.analytics_kafka
   │ kafka_format = 'Protobuf', kafka_schema = 'proto/events.proto:AnalyticsEvent'
   │ kafka_num_consumers = 2, kafka_max_block_size = 8192
   │ ClickHouse owns the consumer group offsets (group: analytics-clickhouse)
   ▼
Materialized View  onlinejudge.analytics_mv
   │ SELECT … FROM analytics_kafka → INSERT TO submission_analytics
   ▼
Destination table  onlinejudge.submission_analytics
   ENGINE = ReplacingMergeTree(event_ts_ms)
   ORDER BY (contest_id, problem_id, user_id, submission_id)
```

Consumer group name `analytics-clickhouse` (distinct from the old Spring `analytics-pipeline` group) so both paths can run side-by-side during cutover if needed.

---

## 3. Implementation steps

### 3.1 Provision ClickHouse in compose

Add to `infra/gcp/compose/control-plane-compose.yml`:

```yaml
clickhouse:
  image: clickhouse/clickhouse-server:24.8   # LTS as of 2026-05
  container_name: oj-clickhouse
  hostname: oj-clickhouse
  ports:
    - "8123:8123"   # HTTP
    - "9000:9000"   # native TCP (for clickhouse-client)
  ulimits:
    nofile: { soft: 262144, hard: 262144 }
  volumes:
    - clickhouse-data:/var/lib/clickhouse
    - clickhouse-logs:/var/log/clickhouse-server
    - ./clickhouse/config.d:/etc/clickhouse-server/config.d:ro
    - ./clickhouse/users.d:/etc/clickhouse-server/users.d:ro
    # mount the canonical proto file straight from the repo — no codegen
    - ../../common/src/main/proto:/var/lib/clickhouse/format_schemas/proto:ro
  environment:
    CLICKHOUSE_DB: onlinejudge
  depends_on:
    oj-kafka:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8123/ping"]
    interval: 10s
    timeout: 3s
    retries: 12

volumes:
  clickhouse-data:
  clickhouse-logs:
```

Memory note: the control-plane VM is tight ([`../tech-spec.md`](../tech-spec.md) §10.1). ClickHouse fits in ~1 GB with default config and small caches. If pressure shows up, bump to `e2-standard-2` or move ClickHouse to a dedicated analytics VM.

### 3.2 Write the DDL

Create `analytics-pipeline/src/main/resources/schema/submission_analytics.sql` (idempotent — `IF NOT EXISTS` on every object):

```sql
CREATE DATABASE IF NOT EXISTS onlinejudge;

-- Destination table — long-lived rows, dedupes via ReplacingMergeTree
CREATE TABLE IF NOT EXISTS onlinejudge.submission_analytics (
    submission_id      String,
    user_id            String,
    problem_id         String,
    contest_id         String,
    language           LowCardinality(String),
    verdict            LowCardinality(String),
    execution_time_ms  UInt32,
    memory_used_mb     UInt32,
    event_ts_ms        Int64,
    region             LowCardinality(String),
    phase              LowCardinality(String),
    event_time         DateTime64(3) MATERIALIZED toDateTime64(event_ts_ms / 1000, 3),
    ingest_time        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(event_ts_ms)
ORDER BY (contest_id, problem_id, user_id, submission_id)
PARTITION BY toYYYYMM(event_time)
TTL event_time + INTERVAL 18 MONTH;

-- Kafka source — ClickHouse owns the consumer
CREATE TABLE IF NOT EXISTS onlinejudge.analytics_kafka (
    submission_id      String,
    user_id            String,
    problem_id         String,
    contest_id         String,
    language           String,
    verdict            String,
    execution_time_ms  UInt32,
    memory_used_mb     UInt32,
    event_ts_ms        Int64,
    region             String,
    phase              String
)
ENGINE = Kafka SETTINGS
    kafka_broker_list       = 'oj-kafka:29092',
    kafka_topic_list        = 'analytics_events',
    kafka_group_name        = 'analytics-clickhouse',
    kafka_format            = 'Protobuf',
    kafka_schema            = 'proto/events.proto:AnalyticsEvent',
    kafka_num_consumers     = 2,
    kafka_max_block_size    = 8192,
    kafka_handle_error_mode = 'stream';

-- Push trigger — runs whenever Kafka Engine produces a block
CREATE MATERIALIZED VIEW IF NOT EXISTS onlinejudge.analytics_mv
TO onlinejudge.submission_analytics AS
SELECT
    submission_id,
    user_id,
    problem_id,
    contest_id,
    language,
    verdict,
    execution_time_ms,
    memory_used_mb,
    event_ts_ms,
    region,
    phase
FROM onlinejudge.analytics_kafka;

-- Error stream — parse failures land here, not in the main table.
-- kafka_handle_error_mode = 'stream' exposes _error and _raw_message virtual columns.
CREATE TABLE IF NOT EXISTS onlinejudge.analytics_kafka_errors (
    raw_message     String,
    error_message   String,
    error_time      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY error_time
TTL error_time + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS onlinejudge.analytics_errors_mv
TO onlinejudge.analytics_kafka_errors AS
SELECT _raw_message AS raw_message, _error AS error_message
FROM onlinejudge.analytics_kafka
WHERE length(_error) > 0;
```

Notes on choices:

- **`ReplacingMergeTree(event_ts_ms)`** — at-least-once duplicates from Kafka collapse to the row with the highest `event_ts_ms` per `ORDER BY` tuple after merges (or with `FINAL` / `OPTIMIZE`). Same dedupe contract documented in the Spring path's `submission_analytics` Javadoc.
- **`ORDER BY (contest_id, problem_id, user_id, submission_id)`** — chosen for the canonical reporting queries (verdict mix per problem, ACCEPTED rate per user-problem). Adjust if BI workload differs.
- **`PARTITION BY toYYYYMM(event_time)`** — monthly partitions; cheap to drop old data, fast partition pruning on time-bounded queries.
- **`LowCardinality(String)`** on `language` / `verdict` / `region` / `phase` — these are small enums; dictionary encoding wins on disk and on filters.
- **`kafka_num_consumers = 2`** — matches the topic's 8 partitions / 2 = 4 partitions per consumer thread. Tune up if a single ClickHouse instance trails the topic.
- **`kafka_handle_error_mode = 'stream'`** — parse errors don't block the consumer; they land in `analytics_kafka_errors` for inspection. The Spring path's failure mode (logged + acked + dropped) is preserved, but inspectable.

### 3.3 Apply the DDL

Three options, ordered by preference:

1. **Init container.** Add a one-shot service to compose that runs `clickhouse-client --host oj-clickhouse --multiquery < /schema/submission_analytics.sql` and exits. Idempotent — safe to re-run.
2. **Operator step in the runbook.** Document a `docker compose exec oj-clickhouse clickhouse-client --multiquery < …` in [`../services/analytics-pipeline.md`](../services/analytics-pipeline.md) §8.
3. **Flyway / migration tool.** Overkill for v1; revisit if the schema starts changing per release.

Pick option 1 if you want one-command bring-up; option 2 if you want explicit human gating.

### 3.4 Backfill on first deploy

Default Kafka Engine starting offset is `latest`. To replay everything sitting in the topic (up to 7 days, per retention), drop in:

```sql
ALTER TABLE onlinejudge.analytics_kafka MODIFY SETTING kafka_thread_per_consumer = 1;
-- before the MV is attached, set the consumer to earliest by recreating the table:
DROP VIEW IF EXISTS onlinejudge.analytics_mv;
DROP TABLE IF EXISTS onlinejudge.analytics_kafka;
CREATE TABLE onlinejudge.analytics_kafka ( … same as above … )
ENGINE = Kafka SETTINGS … , kafka_consumer_offset = 'earliest';
-- then recreate the MV
```

For first deploy this gets the bootstrap rows. After that, `latest` is fine — ClickHouse persists offsets in its own state (under `system.kafka_consumers`).

### 3.5 Decommission the Spring middleman

Once the Kafka Engine path is verified (see §4):

- Remove the `analytics-pipeline:` block from compose (none today — easier).
- Mark `analytics-pipeline/` deprecated in [`../tech-spec.md`](../tech-spec.md) §4.8 and §11.4 — keep the source for git history, exclude from the build-and-push matrix.
- Update [`../services/analytics-pipeline.md`](../services/analytics-pipeline.md): drop "Spring Boot, not Flink" framing, replace §3 with the Kafka Engine description, update §11 code map (DDL files / MV settings instead of Java classes).
- Topic name and proto schema are the contract — neither changes.

### 3.6 Multi-region — defer until v2

Per [`../services/analytics-pipeline.md`](../services/analytics-pipeline.md) §10 and [`./multi-region-rollout.md`](./multi-region-rollout.md): two viable shapes when the time comes —

- **Replicate at Kafka.** MirrorMaker 2 mirrors regional `analytics_events` into a global topic; one global ClickHouse runs Kafka Engine on the merged stream.
- **Replicate at ClickHouse.** Per-region ClickHouse with `ReplicatedMergeTree`; queries fan out via `Distributed` table or `clusterAllReplicas()`.

`AnalyticsEvent.region` is already populated, so either pattern preserves region attribution. Defer the choice — v1 ships single-region.

---

## 4. Test plan

The Spring module has `AnalyticsConsumerTest` and `ClickHouseWriterTest` (proto-parse, buffer, batch, HTTP shape). Those become irrelevant once Spring is decommissioned. The new test surface is the **DDL + Kafka Engine wiring**, exercised end-to-end against a real ClickHouse.

### 4.1 Schema apply (smoke)

| # | Test | How | Pass criterion |
|---|---|---|---|
| T1 | DDL applies cleanly on a fresh ClickHouse | `clickhouse-client --multiquery < submission_analytics.sql` against an empty container | Exit code 0; `SHOW TABLES FROM onlinejudge` lists all 4 objects |
| T2 | DDL is idempotent | Apply T1 twice | Second run is no-op, exit 0 |
| T3 | Compose stand-up | `docker compose up clickhouse`; wait for healthcheck | `/ping` returns 200; `SELECT 1` returns 1 |

### 4.2 Wire format

| # | Test | How | Pass criterion |
|---|---|---|---|
| T4 | Protobuf parse — happy path | Produce a synthetic `AnalyticsEvent` to `analytics_events` via execution-worker's test producer (or a one-off `kafka-protobuf-console-producer`). Wait ≤5 s | Row appears in `submission_analytics` with all 11 fields populated correctly |
| T5 | Proto3 additive evolution is safe | Append a new optional field at tag 12 to `AnalyticsEvent`, recompile producer, leave ClickHouse schema unchanged | Old + new producers both land rows; new field is silently dropped by ClickHouse (unknown tag) |
| T6 | Tag reuse is detected | Repurpose tag 5 to a different type in a branch (do NOT merge); produce; observe | Rows either fail-parse (land in `analytics_kafka_errors`) or write garbage. Either is the "alert" — CI guard against tag changes is the real fix |
| T7 | Malformed payload doesn't stall | Produce a random byte blob to the topic | Row appears in `analytics_kafka_errors` with `_error` populated; no main-table row; consumer offset advances |

### 4.3 Dedupe and ordering

| # | Test | How | Pass criterion |
|---|---|---|---|
| T8 | Duplicate submission_id with same event_ts_ms | Produce the same `AnalyticsEvent` twice; `OPTIMIZE TABLE submission_analytics FINAL` | Single row remains |
| T9 | Duplicate submission_id with newer event_ts_ms | Produce two events with same key, second has larger `event_ts_ms` (e.g. corrected verdict); `OPTIMIZE … FINAL` | Latest event_ts_ms wins (ReplacingMergeTree contract) |
| T10 | Sort order is correct for reporting | Insert 1k rows spanning multiple contests/problems; `SELECT verdict, count() FROM submission_analytics WHERE contest_id='X' GROUP BY verdict` | Subsecond response; counts match input |

### 4.4 Failure modes

| # | Test | How | Pass criterion |
|---|---|---|---|
| T11 | ClickHouse restart preserves offsets | Produce N rows; `docker compose restart clickhouse`; produce N more | Final row count = 2N; no duplicates beyond Kafka's at-least-once (verify via `submission_id` cardinality) |
| T12 | Kafka unavailable on startup | Stop `oj-kafka`; start ClickHouse; bring Kafka back up | ClickHouse waits / retries; rows flow once Kafka returns. No crash loop |
| T13 | Disk pressure | Fill `/var/lib/clickhouse` to 95%; produce rows | Inserts fail gracefully; consumer halts (no offset advance); rows replay after `ALTER TABLE … DROP PARTITION` |
| T14 | Schema mismatch | Drop `submission_analytics`; leave MV; produce rows | MV inserts fail with a clear error in `system.errors`; Kafka offset does not advance until table is recreated |

### 4.5 Operational

| # | Test | How | Pass criterion |
|---|---|---|---|
| T15 | Backfill from earliest | First-deploy variant with `kafka_consumer_offset = 'earliest'`; produce 7 days of events first, then start ClickHouse | Full topic backlog drains; final row count matches producer count |
| T16 | Two consumers don't double-write | Confirm consumer group `analytics-clickhouse` has exactly 2 members (= `kafka_num_consumers`); `SELECT * FROM system.kafka_consumers` | Each partition assigned to exactly one consumer; no row appears twice from a single Kafka offset |
| T17 | Reporting queries (the actual product) | Run each of these against 1M synthetic rows: <br>(a) `SELECT verdict, count() FROM submission_analytics WHERE contest_id=? GROUP BY verdict` <br>(b) `SELECT problem_id, countIf(verdict='ACCEPTED')/count() AS ac_rate FROM submission_analytics WHERE contest_id=? GROUP BY problem_id` <br>(c) `SELECT language, quantile(0.99)(execution_time_ms) FROM submission_analytics WHERE phase='system' GROUP BY language` | All return in < 1 s on a single-node ClickHouse with default settings |

### 4.6 Parity / cutover (only if Spring path is ever deployed in parallel)

| # | Test | How | Pass criterion |
|---|---|---|---|
| T18 | Spring vs. Kafka Engine parity | Run both consumers (different groups) for 1 hour against the same producer | Same row count, same `submission_id` set, same `event_ts_ms` distribution. If they diverge, the Spring path is the suspect (ack-before-flush gap) |

### 4.7 Soak

| # | Test | How | Pass criterion |
|---|---|---|---|
| T19 | 4-hour soak under load | Synthetic load at ~100 events/sec for 4 hours; monitor `system.kafka_consumers` lag, ClickHouse memory, `analytics_kafka_errors` count | Consumer lag stays bounded (< 1k); memory stable; error count = 0; final row count matches producer |

### 4.8 Test harness

- Per-test scripts under `analytics-pipeline/scripts/test/` (or `scripts/clickhouse/`).
- Reuse the existing producer wiring — `scoring-pipeline/src/test/java/.../smoke/SmokeProducer.java` is a close model for a `SmokeAnalyticsProducer`.
- CI: T1–T3 can run in CI against a `clickhouse/clickhouse-server` container. T4–T7 need a Kafka container as well — same pattern as TestContainers tests elsewhere in the repo. T11–T19 are local-dev / pre-prod gates, not CI.

---

## 5. Acceptance criteria (definition of done)

1. `docker compose up clickhouse` brings ClickHouse healthy.
2. The DDL file applies cleanly on a fresh container (T1, T2).
3. A real `AnalyticsEvent` produced by execution-worker lands in `submission_analytics` within 5 s of being acked at Kafka (T4).
4. Malformed payloads land in `analytics_kafka_errors`, do not block the main consumer (T7).
5. Consumer offsets survive a ClickHouse restart (T11).
6. The three canonical reporting queries in T17 return in < 1 s against ≥ 1M rows.
7. `docs/services/analytics-pipeline.md` is rewritten to reflect the Kafka Engine path; the Spring module is marked deprecated in `tech-spec.md` §4.8 and §11.4.

---

## 6. Open questions

- **Backfill on first deploy** — do we want to drain the 7-day buffer that's currently accumulating, or start fresh? Default plan above is `latest` (start fresh); §3.4 documents the backfill variant.
- **Multi-region timing** — when do we cross that bridge? Defer per §3.6, but flag if the multi-region rollout schedule changes.
- **BI integration** — what's the actual consumer of `submission_analytics`? If it's a SQL-over-HTTP BI tool, the HTTP port + read-only user need to be in scope. Not addressed in this plan.

---

## 7. References

- Owner page: [`../services/analytics-pipeline.md`](../services/analytics-pipeline.md) (esp. §2, §3, §10)
- Tech spec: [`../tech-spec.md`](../tech-spec.md) §4.8 (current state), §11.4 (gap table)
- Producer side: [`../services/execution-worker.md`](../services/execution-worker.md) (search `analyticsTopic`)
- Wire format: [`../../common/src/main/proto/events.proto`](../../common/src/main/proto/events.proto) — `message AnalyticsEvent`
- Diagram: [`../../../CSE-Raw/raw-blog/images/online-judge-at-scale/analytics-pipeline-1.svg`](../../../CSE-Raw/raw-blog/images/online-judge-at-scale/analytics-pipeline-1.svg)
- ClickHouse Kafka Engine docs: https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka
- ClickHouse Protobuf format: https://clickhouse.com/docs/en/interfaces/formats#protobuf
