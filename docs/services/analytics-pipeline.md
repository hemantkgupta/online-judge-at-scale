# analytics-pipeline

> **Owner page.** Last reconciled with the repo on **2026-05-18**. Cross-cutting concerns (proto schema, Kafka topic catalogue, multi-region rollout) live in [`../tech-spec.md`](../tech-spec.md). The producer side — execution-worker — is at [`./execution-worker.md`](./execution-worker.md).

> **Status (2026-05-18): KAFKA ENGINE PATH SHIPPED.** The Spring Boot module under `analytics-pipeline/` is deprecated and retained for git history only. Analytics is now a JVM-free ClickHouse path: `clickhouse/clickhouse-server:24.8` runs on every region VM as `oj-clickhouse`, subscribes to `analytics_events.${REGION}` directly via the Kafka Engine, and a Materialized View pushes parsed rows into `submission_analytics`. Design: [`../design-docs/clickhouse-kafka-engine-rollout.md`](../design-docs/clickhouse-kafka-engine-rollout.md).

---

## 1. Purpose

Consume the `analytics_events` Kafka topic and persist one long-lived row per submission into a columnar store, so offline reporting (verdict mix by language, per-problem ACCEPTED rate, p99 execution-time per language) runs as SQL without joining live operational tables in CRDB.

Wire schema: `AnalyticsEvent` in [`common/src/main/proto/events.proto`](../../common/src/main/proto/events.proto), field tags 1–11 (`submission_id`, `user_id`, `problem_id`, `contest_id`, `language`, `verdict`, `execution_time_ms`, `memory_used_mb`, `event_ts_ms`, `region`, `phase`). Slimmer than `VerdictEvent` — no per-test breakdown, no `stdout_hash`. Decoupling from the verdict topic lets the analytics sink evolve independently of the hot path.

---

## 2. External interfaces

**Inbound.** Kafka topic `analytics_events.${REGION}`. Key: `submission_id`. Value: protobuf-serialised `AnalyticsEvent`. Consumer group: `analytics-clickhouse` — owned by ClickHouse, *distinct* from the legacy Spring-era `analytics-pipeline` group so both paths can run side-by-side during any future cutover.

**Storage.** ClickHouse (`onlinejudge` database). One table — `submission_analytics`, `ReplacingMergeTree(event_ts_ms)` ordered by `(contest_id, problem_id, user_id, submission_id)`, monthly partitioning on `toYYYYMM(event_time)`, 18-month TTL.

**Listening surface.** `oj-clickhouse:8123` (HTTP) and `oj-clickhouse:9000` (native TCP) inside the docker network; both ports also bound on the host for `clickhouse-client` and BI tooling. No public exposure — the GCP firewall rule keeps these on the VPC.

---

## 3. Internal design

```
execution-worker --[AnalyticsEvent proto]--> Kafka: analytics_events.${REGION}
                                                       │
                                                       ▼
                          ClickHouse Kafka Engine table  onlinejudge.analytics_kafka
                          (kafka_format='Protobuf', kafka_schema='proto/events.proto:AnalyticsEvent',
                           kafka_num_consumers=2, kafka_handle_error_mode='stream')
                                                       │
                                       ┌───────────────┴────────────────┐
                                       ▼                                ▼
                          onlinejudge.analytics_mv          onlinejudge.analytics_errors_mv
                          SELECT … FROM analytics_kafka     WHERE length(_error) > 0
                                       │                                │
                                       ▼                                ▼
                          onlinejudge.submission_analytics  onlinejudge.analytics_kafka_errors
                          ReplacingMergeTree(event_ts_ms)   MergeTree, 7-day TTL
```

ClickHouse owns the consumer-group offset state. The Kafka Engine advances offsets only after the Materialized View's INSERT into `submission_analytics` succeeds, so the ack-before-flush gap that the Spring module had is closed structurally. At-least-once duplicates from Kafka collapse to one row per `ORDER BY` tuple at merge time (or with `OPTIMIZE … FINAL` / `SELECT … FINAL`).

Errors don't block the consumer. `kafka_handle_error_mode = 'stream'` exposes the `_error` and `_raw_message` virtual columns; a second Materialized View (`analytics_errors_mv`) siphons any row with a non-empty `_error` into `analytics_kafka_errors` so the main consumer keeps draining.

---

## 4. Data ownership

Read: `analytics_events.${REGION}` topic (consumer group `analytics-clickhouse`).
Write: `submission_analytics` (long-lived facts) and `analytics_kafka_errors` (7-day TTL, parse failures) in the `onlinejudge` database.
Does NOT touch CRDB, Redis, GCS, `evaluated_results`, `verdicts`, or any leaderboard state.

There is no in-process JVM buffer — ClickHouse stores the Kafka offsets in its own state (`system.kafka_consumers`), so a `docker compose restart oj-clickhouse` resumes from where it left off with no row loss beyond Kafka's at-least-once contract.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| ClickHouse down | `oj-clickhouse-init` healthcheck fails; Kafka consumer group `analytics-clickhouse` shows zero members | Topic accumulates on Kafka (7-day retention). Bring ClickHouse back up; the Kafka Engine resumes from the stored offset. |
| Kafka unavailable | ClickHouse `system.errors` logs; `system.kafka_consumers` shows disconnected | ClickHouse retries with backoff. No crash, no row loss — offsets are not advanced. |
| Malformed `AnalyticsEvent` payload | Row appears in `onlinejudge.analytics_kafka_errors` with `_error` populated; no row in `submission_analytics` | Consumer offset advances past the bad record; main stream is unaffected (T7 in the design doc). |
| `AnalyticsEvent` additive evolution (new tag) | Existing schema silently drops the new field (proto3 unknown-field tolerance) | Safe — both old and new producers coexist (T5). |
| `AnalyticsEvent` tag reuse | Either parse error into `analytics_kafka_errors`, or wrong-type data in the main table | Real fix is a CI guard on `events.proto` tag stability; the error stream is the alert (T6). |
| Schema mismatch (column dropped on `submission_analytics`) | MV insert fails; `system.errors` populated; Kafka offset stops advancing | Recreate the table → consumer resumes (T14). |
| Disk pressure | ClickHouse logs `Too many parts` / `Disk full`; consumer halts | Drop old partitions (`ALTER TABLE … DROP PARTITION`); offset has not advanced, so rows replay (T13). |

---

## 6. Configuration reference

ClickHouse settings live in `infra/gcp/compose/clickhouse/config.d/` and `users.d/`, mounted read-only at `/etc/clickhouse-server/config.d` and `/etc/clickhouse-server/users.d`. Kafka Engine settings are baked into the DDL at `analytics-pipeline/src/main/resources/schema/submission_analytics.sql`; the apply step substitutes the broker list, topic, group name, and starting offset.

| Token in DDL | Default in `apply-ddl.sh` | Purpose |
|---|---|---|
| `__KAFKA_BROKER_LIST__` | `oj-kafka:29092` | Kafka bootstrap inside the region docker network. |
| `__KAFKA_TOPIC__` | `analytics_events.${REGION}` (via compose env) | Per-region source topic. |
| `__KAFKA_GROUP_NAME__` | `analytics-clickhouse` | Distinct from the legacy `analytics-pipeline` group. |
| `__KAFKA_CONSUMER_OFFSET__` | `latest` | Set to `earliest` on first deploy to drain the 7-day Kafka backlog. |

Fixed in the DDL: `kafka_format = 'Protobuf'`, `kafka_schema = 'proto/events.proto:AnalyticsEvent'`, `kafka_num_consumers = 2` (8 topic partitions / 2 consumers = 4 partitions per consumer thread), `kafka_max_block_size = 8192`, `kafka_handle_error_mode = 'stream'`.

---

## 7. Metrics emitted

ClickHouse exposes these out of the box via `system.kafka_consumers`, `system.parts`, `system.errors`:

- `system.kafka_consumers` — per-consumer lag, last error, last error time. The "consumer lag" tile on the ops dashboard reads from this.
- `system.errors` — server-level error counters. Alert on rate of `EngineError` / `CANNOT_PARSE_PROTOBUF` > 0.
- `system.parts` filtered to `database='onlinejudge' AND table='submission_analytics'` — total bytes, row count, partition list.
- A `SELECT count() FROM onlinejudge.analytics_kafka_errors WHERE error_time > now() - INTERVAL 1 HOUR` is the parse-error rate.

OTel scraping of `system.metrics` is the same shape as for any ClickHouse — not yet wired into the regional OTel collector; tracked alongside the broader metrics-catalogue rollout in `../tech-spec.md` §9.

---

## 8. Runbook

### 8.1 First-time bring-up

1. `docker compose -f infra/gcp/compose/region.yml up -d oj-clickhouse` — wait for the healthcheck to go green.
2. Set `KAFKA_CONSUMER_OFFSET=earliest` in `/opt/oj/.env` *if* you want to drain whatever's in the 7-day topic backlog (skip on a brand-new cluster).
3. `docker compose -f infra/gcp/compose/region.yml up oj-clickhouse-init` — the one-shot init container runs `apply-ddl.sh` and exits. Re-running is a no-op (every object is `IF NOT EXISTS`).
4. Sanity: `docker exec oj-clickhouse clickhouse-client --query "SHOW TABLES FROM onlinejudge"` → 5 objects (`submission_analytics`, `analytics_kafka`, `analytics_mv`, `analytics_kafka_errors`, `analytics_errors_mv`).
5. Smoke: run `analytics-pipeline/scripts/test/02-protobuf-roundtrip.sh` against the live stack — produces one synthetic `AnalyticsEvent` and waits ≤ 5 s for it to land.

### 8.2 "Consumer lag growing — events not landing"

`SELECT * FROM system.kafka_consumers FORMAT Vertical` to see the live group state. Common causes:

- **`last_exception` populated** — usually a proto/schema drift. Check `analytics_kafka_errors` for the raw bytes.
- **Kafka unreachable** — `oj-kafka` container down, or DNS/iptables. Restore the broker; ClickHouse retries automatically.
- **Disk full** — `df -h /var/lib/clickhouse`. Drop the oldest partition: `ALTER TABLE onlinejudge.submission_analytics DROP PARTITION 'YYYYMM'`.

### 8.3 "Parse errors spiking"

`SELECT error_message, count() FROM onlinejudge.analytics_kafka_errors WHERE error_time > now() - INTERVAL 1 HOUR GROUP BY error_message`. The usual suspect is an `events.proto` tag change on the producer side without a matching review of the DDL columns. Roll the producer or recreate `analytics_kafka` after fixing the proto.

### 8.4 Schema mismatch — main table dropped

`MV inserts fail; offsets stop advancing` is the structural guarantee. Recreate the table by re-running `apply-ddl.sh`; the consumer resumes.

---

## 9. Tests & verification

The DDL is exercised end-to-end (and re-verified on every change) via the scripts under `analytics-pipeline/scripts/test/`:

| Script | Tests | What it does |
|---|---|---|
| `01-schema-apply.sh` | T1 + T2 (design doc §4.1) | Spin up a throwaway `clickhouse/clickhouse-server:24.8`, apply the DDL twice, assert all four (plus the errors MV → five) objects exist and the second apply is a no-op. |
| `02-protobuf-roundtrip.sh` | T4 (design doc §4.2) | Produce one synthetic `AnalyticsEvent` to `analytics_events.${REGION}`, poll `submission_analytics` for it. |
| `03-malformed-payload.sh` | T7 (design doc §4.4) | Produce 32 random bytes onto the topic, assert a row lands in `analytics_kafka_errors` (and *not* in `submission_analytics`), and that the main consumer keeps advancing. |

The remaining test matrix (T5/T6 proto evolution, T8–T10 dedupe/ordering, T11–T14 failure modes, T15–T17 operational and report queries, T19 soak) is documented in `../design-docs/clickhouse-kafka-engine-rollout.md` §4 and run as pre-deploy gates, not in CI.

---

## 10. Relevant design docs

- [`../design-docs/clickhouse-kafka-engine-rollout.md`](../design-docs/clickhouse-kafka-engine-rollout.md) — the implementation plan and test matrix for the Kafka Engine path. **Authoritative for this service.**
- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — single-region ships first; multi-region (MirrorMaker 2 vs. ReplicatedMergeTree) is §3.6 of the rollout doc, deferred to v2.

---

## 11. Code map

| Concern | File |
|---|---|
| ClickHouse Kafka Engine DDL (canonical) | `analytics-pipeline/src/main/resources/schema/submission_analytics.sql` |
| DDL apply script (host & init container) | `analytics-pipeline/scripts/apply-ddl.sh` |
| Test scripts (T1, T2, T4, T7) | `analytics-pipeline/scripts/test/` |
| Compose service definitions (`oj-clickhouse`, `oj-clickhouse-init`) | `infra/gcp/compose/region.yml` |
| ClickHouse server config (caches, listen host) | `infra/gcp/compose/clickhouse/config.d/01-listen.xml` |
| ClickHouse default user config | `infra/gcp/compose/clickhouse/users.d/01-default-user.xml` |
| Producer side | `execution-worker/.../consumer/SubmissionConsumer.java` (search `analyticsTopic`) |
| Proto wire format | `common/src/main/proto/events.proto` (`message AnalyticsEvent`, tags 1–11) |
| Spring Boot module (deprecated, retained for git history) | `analytics-pipeline/src/main/java/com/onlinejudge/analytics/` |
