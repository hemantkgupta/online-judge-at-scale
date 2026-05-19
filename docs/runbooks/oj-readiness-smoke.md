# Runbook — Pre-deploy readiness smoke

> Operator playbook for `scripts/oj-readiness-smoke.sh` — the end-to-end harness that exercises submission → verdict → leaderboard → analytics plus the feature surfaces tracked by the readiness checklist (PRs #5–#15). Companion docs: [`../tech-spec.md#13-testing-strategy`](../tech-spec.md#13-testing-strategy), [`./multi-region.md`](./multi-region.md) (sibling local-harness with the same bash style).

---

## 1. When to run

| Trigger | Expected outcome |
|---|---|
| **Before tagging a release** | All seven scenarios PASS under `--strict`. Any FAIL or SKIP blocks the release tag. |
| **After merging any PR from the #5–#15 cluster** | Re-run without `--strict` to confirm the affected scenario flipped from SKIP → PASS. Other scenarios stay green. |
| **After bumping image tags in `region.yml`** | Catch wire-protocol regressions before the operator runs `compose up -d` on the real VM. |
| **Weekly on `main`** | Drift gate — surfaces silent breakage in code paths the unit suite doesn't cover (Kafka topic naming, ClickHouse Kafka-Engine offset behaviour, leaderboard ZSET key shape). |

The smoke is **not** part of `ci.yml` today. It needs Docker + ~8 GB free RAM + ~10 min on a cold cache; CI runners would flake. Operator runs it locally before kicking the deploy.

---

## 2. How to run

```sh
# From the repo root
bash scripts/oj-readiness-smoke.sh

# Strict mode — SKIPs become FAILs (use at the release gate)
bash scripts/oj-readiness-smoke.sh --strict

# Keep the stack up after the run for manual poking
bash scripts/oj-readiness-smoke.sh --keep-up
docker compose -f infra/gcp/compose/region.yml down -v  # when done
```

Exit code = number of failed scenarios. `0` is green.

Expected runtime:
- **First run**: ~10–15 min — image pulls + Gradle Docker builds for the six JVM services + ClickHouse Kafka-Engine's first poll on the 7-day `analytics_events.<region>` retention window. The CH `apply-ddl.sh` step seeks to `KAFKA_CONSUMER_OFFSET=latest` by default (set via `region.yml`), so backfill is skipped — but the offsets-coordinator round-trip still costs ~20 s on a cold broker.
- **Subsequent runs**: ~3–5 min — cached docker layers, warm volumes, hot brokers.

The script is idempotent and self-contained: it tears down with `docker compose down -v` on `trap EXIT` (unless `--keep-up`), and it scopes its user/problem/contest IDs by the run timestamp so reruns against warm volumes don't collide (mirrors the `scoring-smoke.sh` idiom committed in `be61b7b`).

---

## 3. What each scenario validates

| Scenario | Path under test | Pass criterion | Owner PR |
|---|---|---|---|
| **S1 — Auth flow + rate-limit split** | `AuthController.signup/login` + `RateLimitService` with the two-bucket configuration. 11 logins from one IP within 60 s → 11th returns 429. 11 submissions in the same window → first 10 are 202, 11th is 429 (proving the buckets are independent). | Auth 11th = 429 AND submission 11th = 429 AND submission attempts 1–10 = 202. Probed for via `X-RateLimit-Bucket: auth` on the login response. | #9 |
| **S2 — Submission → verdict → leaderboard** | POST a python `sum-two-numbers` solution via a `data:` URL → verdict ACCEPTED on the worker → leaderboard ZSET advanced. End-to-end target < 10 s. | `GET /api/v1/submissions/<id>` shows `verdict=ACCEPTED` within 30 s AND `ZCARD leaderboard:<contest>` ≥ 1 (or the shard-routed `:shard:{0..2}` variant if the scoring-pipeline is wired). | existing core path |
| **S3 — Code URL schemes** | POST a submission with an `http://` source URL pointing at the in-process `tiny-http-server.py` fixture (`python3 -m http.server` style). The execution-worker fetches the source over HTTP and runs it. | `verdict=ACCEPTED` within 30 s. If the worker still throws `UnsupportedOperationException` on `http://`, the verdict surfaces as `RUNTIME_ERROR`/`COMPILE_ERROR` and the scenario SKIPs (PR #14 outstanding). `s3://` + `r2://` are gated by `APP_SOURCE_S3_ENABLED=true`; default skips them. | #14 |
| **S4 — Analytics ingest via CH Kafka Engine** | After S2's verdict, the `onlinejudge.analytics_kafka` Kafka-Engine table polls `analytics_events.<region>` and the materialised view inserts into `onlinejudge.submission_analytics`. | `SELECT count() FROM onlinejudge.submission_analytics WHERE submission_id='<sid>'` returns ≥ 1 within 35 s (5 s wait + 30 s polling window). | existing (commit `1a2b4b3`) |
| **S5 — DLQ + observability dashboard validate** | `bash infra/observability/scripts/validate.sh` parses every dashboard + alert JSON, asserts `x-otel-defaults` resolves on every JVM service block. PR #6 also adds a `dlq-*.json` dashboard. | Validator exits 0 AND a `dashboards/dlq*.json` file exists. Validator-green-but-no-dlq dashboard = SKIP. | #6 |
| **S6 — SPOT preemption drain endpoint** | `POST /actuator/preempt-drain` on the execution-worker with the `X-Preempt-Token` header. The worker stops claiming new pretest messages, drains in-flight executions, and logs `preemption drain complete`. | HTTP 200 AND log line within 30 s. The scenario also restarts the worker container at the end so subsequent runs start clean. | #13 |
| **S7 — Application metrics catalogue** | `curl ${OTEL_PROMETHEUS_URL}` (otel-collector's `:8888/metrics`) OR each service's `/actuator/prometheus`. Greps for the five aspirational metric names from `docs/tech-spec.md` §9.3. | All five metric names are registered (either dotted or underscored form, since the otel-prometheus exporter rewrites `.` → `_`). | #11 |

### 3.1 Forward-compat — SKIPs vs FAILs

Several scenarios target features that are listed as **open debt** in `docs/tech-spec.md` §14 ("Known limitations") at the time this runbook was written:

- §14: *Auth endpoints share the per-IP rate limit bucket with submission posts* — S1 unshipped surface.
- §14: *No DLQ dashboard for the poison topic* — S5's dlq-dashboard presence-check.
- §14: *No SPOT preemption shutdown script* — S6.
- `SubmissionConsumer.resolveSourceCode` throws `UnsupportedOperationException` on `http://`/`s3://`/`r2://` — S3.
- `docs/tech-spec.md` §9.3: *Counter + gauge + histogram names below are aspirational* — S7.

For each, the script does a **presence probe** before asserting. When the surface isn't shipped, the scenario emits `SKIP: <reason>` instead of `FAIL`. That lets you run the smoke today on substrate-only (S2 + S4 + S5-validator + part of S5) without false-negatives, then watch the SKIPs flip to PASS as PRs #5–#15 land.

At the release gate, run with `--strict`. That promotes every SKIP into a FAIL so the release tag is blocked unless every feature scenario is genuinely green.

---

## 4. Common failure modes

### 4.1 `[wait] api-gateway UP` times out after 300 s

**Symptom.** The compose stack starts but the gateway never reports `UP`.

**Cause.** Usually `oj-cockroachdb` hasn't been `cockroach init`'d in this volume — Flyway hangs on the first migration. Multi-region init is documented in `multi-region.md` §1, but for the **single-region** smoke (`region.yml` driven by this script), the simpler init is:

```sh
docker exec oj-cockroachdb cockroach init --insecure --host=oj-cockroachdb:26257
docker compose -f infra/gcp/compose/region.yml restart oj-api-gateway
```

Or just `docker compose -f infra/gcp/compose/region.yml down -v` to start fresh — the `oj-cockroachdb-init` step in compose creates the database, and CRDB's single-node startup auto-initialises.

### 4.2 S1 — auth 11th login returns 200 not 429

**Cause.** Either (a) the auth bucket isn't shipped yet (PR #9) — and the substrate's shared bucket has higher capacity than 10/min, so all 11 logins succeed — or (b) the bucket is shipped but tuned > 10 req/min.

**Diagnose.** Check the response headers:

```sh
curl -sI http://127.0.0.1:8088/api/v1/auth/login | grep -i ratelimit
```

If you see `X-RateLimit-Bucket: auth`, PR #9 is live and the tuning is the issue. If you don't see that header, the script should have emitted SKIP, not FAIL — verify the `has_split` probe matches the header the auth-rate-limit PR introduces.

### 4.3 S2 — verdict stays PENDING past 30 s

**Cause.** The execution-worker isn't consuming. Most common: the `oj-execution-worker` container is up but its Kafka consumer hasn't joined the group yet, or `submissions.<region>.pretest` was never created by `kafka-bootstrap-topics.sh`.

**Diagnose.**

```sh
docker exec oj-kafka kafka-topics --bootstrap-server oj-kafka:29092 --list
docker logs oj-execution-worker | tail -50
```

If the topic is missing, run:

```sh
DOCKER=docker bash infra/scripts/kafka-bootstrap-topics.sh \
  --region "${REGION:-asia-south1}" --container oj-kafka --broker oj-kafka:29092
```

### 4.4 S4 — `submission_analytics` count returns 0

**Cause.** The ClickHouse Kafka Engine consumer-group exists but is at `latest` and the verdict event was produced before the consumer subscribed. This happens on the **very first** smoke run after `down -v` because the analytics topic offset starts past the verdict. The script's 30 s polling window usually catches this, but if it doesn't:

```sh
# Force the analytics consumer to seek to earliest, then restart it
docker exec oj-clickhouse clickhouse-client --query "
  DETACH TABLE onlinejudge.analytics_kafka;
  ATTACH TABLE onlinejudge.analytics_kafka;
"
# Or change KAFKA_CONSUMER_OFFSET=earliest in /opt/oj/.env (region.yml line 178)
# and `docker compose up -d oj-clickhouse-init` to re-run the one-shot init.
```

### 4.5 S5 — `validate.sh` fails on `dashboards/*.json`

**Cause.** A dashboard JSON lacks `displayName` or `mosaicLayout`. The validator is strict.

**Diagnose.**

```sh
ls infra/observability/dashboards/
bash infra/observability/scripts/validate.sh   # prints which file failed
```

Fix the offending JSON, re-run.

### 4.6 S6 — `404 Not Found` on `/actuator/preempt-drain`

**Cause.** PR #13 hasn't landed — the endpoint doesn't exist. The script emits SKIP for this case. If you got FAIL not SKIP, the worker is returning some other 4xx/5xx; check `docker logs oj-execution-worker`.

### 4.7 S7 — `none of the 5 application metrics are registered`

**Cause.** `OTEL_JAVAAGENT_ENABLED=false` (the default in `region.yml`; see §9.2 of the tech spec). The agent gives us JVM + HTTP + Kafka metrics for free, but the application-specific ones in §9.3 require the agent to be enabled.

**Fix for the smoke**: export `OTEL_JAVAAGENT_ENABLED=true` and `OTEL_ENDPOINT=http://oj-otel-collector:4317` before running `docker compose up`. Or accept SKIP — substrate-only S7 doesn't gate a release.

---

## 5. Escalation

If the smoke fails in a way none of §4 covers:

1. Re-run with `--keep-up` so the stack persists for forensics.
2. Capture `docker compose -f infra/gcp/compose/region.yml logs > /tmp/oj-smoke.log` and the script's output.
3. File against `online-judge-at-scale` with the failing scenario number and the log.
4. **Do not deploy** — a smoke failure on `main` means the deployable image regressed against the readiness contract, and the issue could be live in production if you push past it.

For multi-region readiness (the wider local harness), see `multi-region.md` §5. The two harnesses are complementary: `oj-readiness-smoke.sh` exercises a single region's full feature surface; `test-multi-region-local.sh` exercises the cross-region failure paths.
