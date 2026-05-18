# contest-service

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the contest-service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the auth model, the system-wide reliability story) live in [`../tech-spec.md`](../tech-spec.md). The system-test replay design — the most novel mechanic in this service — lives in [`../design-docs/contest-close-system-tests-replay.md`](../design-docs/contest-close-system-tests-replay.md).
>
> **Status note.** Dockerfile, image, and `control-plane-compose.yml` entry shipped (commit `6be38f4`); the service has **not yet been deployed on the live GCP environment** as of 2026-05-18. The image is buildable and the compose block is wired; the next control-plane deploy picks it up.
>
> Read this page if you are: (a) on-call for contest lifecycle (when the service goes live), (b) onboarding into the contest-domain code, (c) changing anything in `contest-service/` or the `contests` / `contest_problems` schema.

---

## 1. Purpose

The contest lifecycle state machine. Smallest service by code volume; largest blast radius if it misbehaves — every api-gateway pod reads contest state to enforce the submission window, every leaderboard subscriber watches its transitions, and the system-test replay (Roadmap §4.23) is anchored on its ACTIVE → CLOSED edge.

Three responsibilities, in priority order:

1. **Contest lifecycle.** Own the `contests` row. Enforce `CREATED → REGISTRATION → ACTIVE → CLOSED → RESULTS`. Persist transitions under optimistic locking. Fan out state changes on `contest_events` so cache-holders invalidate.
2. **Encryption-key custody.** Generate a per-contest AES-256-GCM key at REGISTRATION, expose it via `GET /{id}/key` once the contest is ACTIVE. This is the T0 unlock primitive: clients pre-fetch the encrypted bundle during REGISTRATION and decrypt the instant the key endpoint flips from 404 to 200.
3. **System-test replay.** On ACTIVE → CLOSED, walk `contest_problems` and re-publish every ACCEPTED pretest submission as `phase="system"` on `submissions.system`. The deterministic counterpart to execution-worker's per-submission promotion — see [`../design-docs/contest-close-system-tests-replay.md`](../design-docs/contest-close-system-tests-replay.md).

What this service does **not** own: the `submissions` table (api-gateway writes), the `contest_problems` writes (api-gateway admin endpoints), the schema (api-gateway's Flyway V6 + V7 own the DDL; contest-service runs `ddl-auto: validate`). No Redis, no inbound Kafka, no auth today. The freeze-window enforcement lives in api-gateway's `ContestWindowFilter` — see [`./api-gateway.md`](./api-gateway.md); contest-service is the authoritative read source it's gated on.

---

## 2. External interfaces

### 2.1 REST API

All endpoints under `/api/v1/contests`. No auth today (Roadmap item; internal-only listening surface).

| Method | Path | Body / Response |
|---|---|---|
| `POST` | `/` | `{title, startTime, endTime}` → `201 Contest` |
| `POST` | `/{id}/register` | CREATED → REGISTRATION; generates AES-256 key + bundle URL placeholder |
| `POST` | `/{id}/activate` | REGISTRATION → ACTIVE; T0 — key becomes retrievable |
| `POST` | `/{id}/close` | ACTIVE → CLOSED; fires system-test replay scan |
| `POST` | `/{id}/results` | CLOSED → RESULTS; leaderboard frozen |
| `GET` | `/{id}` | `200 Contest` or `404` |
| `GET` | `/{id}/key` | `200 {"key": "<base64>"}` when ACTIVE/CLOSED/RESULTS; `404` otherwise. CDN-cacheable. |
| `GET` | `/{id}/accepting` | `200 {"accepting": <bool>}` — true iff state == ACTIVE. Called by api-gateway's ContestWindowFilter. |
| `GET` | `/actuator/health` | `200 {"status":"UP"}` once Hikari reaches CRDB. |

Transitions are admin-driven; `LifecycleWorker` (§3.4) also fires REGISTRATION → ACTIVE and ACTIVE → CLOSED on `startTime`/`endTime`. Invalid transitions → `IllegalStateException` → 500 (TODO: map to 409). Unknown `contestId` → 404.

### 2.2 Kafka topics

**Produced.**

| Topic | Key | Payload | When |
|---|---|---|---|
| `contest_events` | `contestId` | JSON `{contestId, oldState, newState, startTime, endTime, timestampMs[, decryptionKey]}` | Every transition. `decryptionKey` included only on REGISTRATION → ACTIVE (T0 fanout). |
| `submissions.system` | `userId` | Protobuf `SubmissionEvent` (`phase="system"`) | One per ACCEPTED pretest submission for the closing contest's problems on ACTIVE → CLOSED. Proto: [`../tech-spec.md#5-wire-formats-and-data-models`](../tech-spec.md#5-wire-formats-and-data-models). |

`acks=all` on both; replay sends are awaited synchronously per submission (see §3.3). **Consumed.** None today.

### 2.3 Outbound HTTP

None. Outbound traffic is JDBC (CRDB) and Kafka only. OTLP gRPC to `oj-otel-collector:4317` when the agent is enabled (default off).

### 2.4 Listening surface

- TCP `:8084` — REST API + actuator. Internal-only via the `oj-allow-internal` firewall rule.
- No public ingress, no vsock, no UDS.

---

## 3. Internal design

### 3.1 State machine

Five states, strictly forward, defined in `ContestState`. The transition graph is a static `Map<ContestState, Set<ContestState>>` — modify it carefully.

| From → To | Trigger | Preconditions | Side effects |
|---|---|---|---|
| CREATED → REGISTRATION | `POST /{id}/register` | `startTime` + `endTime` set; `durationMinutes > 0` | `EncryptionService.generateKey()` writes a fresh AES-256 key onto the row; `encryptedBundleUrl` is set (today: `local://bundles/{id}/problems.encrypted` placeholder — production overrides to the CDN URL once the bundle uploader lands). |
| REGISTRATION → ACTIVE | `POST /{id}/activate` OR `LifecycleWorker` at `now >= startTime` | `encryptionKey` + `encryptedBundleUrl` both non-blank | `contest_events` fanout includes the decryption key (T0 publication). `GET /{id}/key` flips from 404 → 200. api-gateway's ContestWindowFilter starts admitting submissions. |
| ACTIVE → CLOSED | `POST /{id}/close` OR `LifecycleWorker` at `now >= endTime` | (none — admin-forced close is always allowed) | `SystemTestReplayPublisher.replayContest(contest)` fires (see §3.3). api-gateway returns 410 Gone on new submissions. |
| CLOSED → RESULTS | `POST /{id}/results` (admin-only; not automated) | (today: none enforced. Future: Flink drain confirmed.) | Leaderboard frozen. RESULTS is terminal. |
| any → same state | any of the above | idempotent | logged at INFO; no-op. |

Illegal transitions (e.g. REGISTRATION → CLOSED, ACTIVE → REGISTRATION) throw `IllegalStateException` from `ContestStateMachine.transition()`.

Concurrency safety relies on the `@Version` field on `Contest`. Two concurrent transition attempts for the same contest from different threads/instances cannot both succeed — the second commit fails with `OptimisticLockException` and the caller retries (or surfaces the failure to the admin). This is the *only* concurrency control today; there is no distributed lock.

### 3.2 Encryption key + encrypted bundle

T0 mechanism in two beats:

1. **REGISTRATION.** `EncryptionService.generateKey()` produces a 256-bit AES key via `KeyGenerator("AES").init(256, SecureRandom)`, returns base64. Written to `contests.encryption_key` (CRDB encrypts at rest). `encryptedBundleUrl` is set to a placeholder; production registration would also encrypt the bundle (12-byte IV + AES-256-GCM + 128-bit auth tag, see `EncryptionService.encrypt()`), upload to R2/GCS, store the CDN URL.
2. **ACTIVE.** `getDecryptionKey(contestId)` starts returning the key. Clients that pre-fetched the encrypted bundle decrypt locally. A client that manipulated its clock can't cheat: the endpoint returns 404 until the row's state actually flips.

`EncryptionService` is stateless — no key cache, no in-memory secret beyond the in-flight request. The bundle upload + R2 wiring is a TODO; only the key half of the mechanism is shipped today.

### 3.3 System-test replay (ACTIVE → CLOSED hook)

The novel mechanic. `SystemTestReplayPublisher`, wired into `ContestStateMachine.transition()` via `@Autowired(required=false)` so unit tests without a Spring context still construct the state machine.

Trigger: a *real* ACTIVE → CLOSED transition (idempotent no-op does NOT fire). Replay failure does NOT block the transition — the contest is genuinely CLOSED regardless, and partial replay is recoverable because the worker's phase-scoped `IdempotencyService` (key `submissionId:system`) deduplicates on retry. The exception is caught + logged at ERROR; the state machine completes.

The scan iterates `problemContestRepository.findProblemIdsByContestId(contestId)` in ordinal order. For each problem, `SubmissionReplayRepository.pageAccepted(problemId, cursorCreatedAtMs, cursorId, PAGE_SIZE=1000)` runs a native-SQL select against `onlinejudge.submissions` (api-gateway-owned — we avoid a parallel JPA entity to sidestep Hibernate validation conflicts). Pagination is cursor-based on `(created_at, id)` so a hot problem doesn't suffer the increasing-prefix scan of `OFFSET`.

Each row becomes a `SubmissionEvent` with `phase="system"`, keyed on `userId` to mirror api-gateway's original partitioning. The send `Future` is awaited synchronously with a 30 s timeout — broker stall halts the scan rather than buffering unboundedly in the producer accumulator. The `contestId` on the replayed event is the *closing* contest's UUID, not the original, so downstream telemetry (analytics-pipeline) attributes the system-test verdict correctly even when the original submission was practice-mode (`contestId == null`).

No admin endpoint to re-drive replay today — gap noted in §8.2.

### 3.4 LifecycleWorker

A `@Scheduled(fixedDelay = 1000)` poller. Fires REGISTRATION → ACTIVE at `startTime`, ACTIVE → CLOSED at `endTime` (via `ContestRepository.findByStateAndStartTimeBefore` / `findByStateAndEndTimeBefore`). CLOSED → RESULTS is NOT automated — requires Flink-drain confirmation, not yet wired.

1 s granularity is enough for 90-minute contests; NTP slew across api-gateway pods is ±10 ms. In HA, `@Version` serialises concurrent transitions — only one instance wins per state edge. One contest's failure doesn't block others; the worker catches per-contest, logs at ERROR, continues.

### 3.5 Kafka producer config

`KafkaProducerConfig` exposes only a typed `KafkaTopics` record bean. The `KafkaTemplate<String, byte[]>` is auto-wired from `spring.kafka.*` (bootstrap servers, byte-array value serialiser, `acks=all`). `app.kafka.topic.system` must match execution-worker's `app.kafka.topic.system` (both default `submissions.system`) — see [`./execution-worker.md`](./execution-worker.md).

---

## 4. Data ownership

| Resource | Owner | Notes |
|---|---|---|
| `contests` row | contest-service writes | api-gateway reads for window enforcement |
| `contest_problems` rows | api-gateway admin endpoints write | contest-service reads (replay scan) |
| `submissions` rows | api-gateway writes | contest-service reads (native-SQL replay scan only) |
| Encryption key + bundle URL | contest-service writes at REGISTRATION | On the `contests` row, CRDB-encrypted at rest |
| In-memory state | — | Service is stateless across restarts |

**Schema ownership.** Flyway migrations live in api-gateway: `V6__contests.sql`, `V7__contest_problems.sql`. contest-service runs `ddl-auto: validate` — this is the §2.2 stance: api-gateway is the sole Flyway owner; every other JVM service is a read-validate-only JPA client.

**Kafka topic ownership.** `contest_events` is owned by contest-service (sole producer). `submissions.system` is co-produced by contest-service (on close) and execution-worker (per-submission Phase-1 ACCEPTED). The consumer doesn't care which producer originated the message; phase-scoped idempotency deduplicates either way.

Restart-safe: no in-memory state. The next `LifecycleWorker` tick re-discovers pending transitions. An incomplete replay is re-driven manually (§8.2).

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| Invalid transition (e.g. REGISTRATION → CLOSED) | `IllegalStateException` from state machine | 500 (TODO: 409). Row unchanged. No Kafka fanout. |
| Precondition unmet on REGISTRATION → ACTIVE (key/bundle URL blank) | `IllegalStateException` with explicit message | Same. Operator must complete bundle upload before retrying activate. |
| Optimistic lock conflict | `OptimisticLockException` on `save()` | Default 500. Loser re-reads + retries manually (no auto-retry today). |
| Replay partial failure (mid-scan Kafka send throws) | `RuntimeException` from `awaitSend()` propagates | Contest is still CLOSED. Unsent submissions NOT auto-retried — runbook §8.2 covers manual re-drive. Worker idempotency makes already-sent submissions safe to re-publish. |
| Kafka broker unreachable during replay | 30 s `TimeoutException` per send | Replay halts on first stalled send. Same recovery as partial failure. |
| CRDB unreachable | JPA / JdbcTemplate exception → 500 | All endpoints fail. `LifecycleWorker` logs ERROR per cycle; resumes when CRDB recovers. |
| Missing `contest_problems` rows on close | `findProblemIdsByContestId` returns empty | `replayContest` logs `nothing to replay` and returns 0. Contest still closes cleanly — can silently mask a data-loading bug. Verify problem count out of band before close. |
| Encryption key absent at activate-time | `IllegalStateException` from precondition check | Activate rejected; contest stays in REGISTRATION. Caused by manual state flips that skip key generation. Fix: `POST /{id}/register` is idempotent on already-REGISTRATION but does NOT re-mint the key — see §8.5. |
| `contest_events` Kafka send fails | Caught inside `publishStateChange`; logged ERROR | State change is durable in CRDB; cache-holders pick up via the 1 s TTL fallback in api-gateway. Lifecycle event is **lost** — no outbox backs this publisher (gap; see §10). |
| Schema validation fails at boot | Hibernate `ddl-auto: validate` throws | Container crash-loops with the column mismatch in the log. Fix: align entity with V6/V7 or land a follow-up Flyway migration in api-gateway. |
| LifecycleWorker fires twice in HA | Second commit fails `OptimisticLockException` | One transition wins per edge. Other instance logs ERROR + continues. |

---

## 6. Configuration reference

All properties from `contest-service/src/main/resources/application.yml`; env overrides via Spring relaxed binding (`SPRING_DATASOURCE_URL` → `spring.datasource.url`). Defaults shown.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8084` | Tomcat port. |
| `spring.threads.virtual.enabled` | `true` | Loom virtual threads on the request executor. |
| `spring.datasource.url` | `jdbc:postgresql://localhost:26257/onlinejudge?sslmode=disable` | CRDB JDBC. Production: `cockroachdb:26257`. |
| `spring.datasource.{username,password}` | `root` / (empty) | Override via env. |
| `spring.jpa.hibernate.ddl-auto` | `validate` | Must stay `validate`; api-gateway Flyway owns schema. |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Production: `kafka:29092`. |
| `spring.kafka.producer.value-serializer` | `ByteArraySerializer` | Both JSON + proto travel as bytes. |
| `spring.kafka.producer.acks` | `all` | Durable replay; tolerates broker rolling restart. |
| `app.kafka.topic.system` | `submissions.system` | Replay target. **Must match** execution-worker's `app.kafka.topic.system`. |
| `app.kafka.topic.contest-events` | `contest_events` | Lifecycle fanout topic. |
| `APP_REGION` (env) | `${REGION:-asia-south1}` | Stamped on logs; not behaviourally meaningful. |
| `JAVA_TOOL_OPTIONS` (compose) | `-Xmx256m -XX:+ExitOnOutOfMemoryError` | Tight heap — tighter than problem-service (256 vs 384 MiB) because this service is REST-only, no signing-key buffer. |
| `OTEL_JAVAAGENT_ENABLED` | `false` | Default off; operator flips on once collector is healthy. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://oj-otel-collector:4317` | Standard. |
| `OTEL_SERVICE_NAME` | `oj-contest-service` | — |

---

## 7. Metrics emitted

contest-service has **no metrics catalogue today** — there is no `ContestMetrics` bean. Proposed catalogue, names prefixed `oj.contest.*`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.contest.transitions_total` | counter | `from_state`, `to_state`, `outcome` (`success`/`invalid`/`oplock_conflict`) | One per attempted transition. |
| `oj.contest.lifecycle.poll_lag_ms` | histogram | — | LifecycleWorker poll wall-clock; < 100 ms healthy. |
| `oj.contest.replay.submissions_published_total` | counter | `contest_id` | Per-replay tally — confirms close fired the expected scan size. |
| `oj.contest.replay.duration_seconds` | histogram | — | End-to-end `replayContest` wall-clock. |
| `oj.contest.replay.send_failure_total` | counter | `contest_id`, `reason` (`timeout`/`broker_error`) | Sustained > 0 → manual re-drive (§8.2). |
| `oj.contest.events.publish_failure_total` | counter | `topic` | `contest_events` send failure (silent-loss path in §5). |
| `oj.contest.key.fetch_total` | counter | `state` (`active`/`pre_active`) | T0 read pressure on `GET /{id}/key`. |
| `oj.contest.key.fetch_latency_seconds` | histogram | — | < 10 ms p99 (single CRDB lookup). |

**Today vs future gap.** Until the catalogue lands, observability is via log prefixes (`[lifecycle]`, `[contest]`, `[replay]`). OTel agent auto-instrumentation gives Tomcat / JDBC / Kafka spans once `OTEL_JAVAAGENT_ENABLED=true` — sufficient bridge; the dedicated catalogue is the next step.

---

## 8. Runbook

Common incidents and the first diagnostic step + fix. Assumes SSH + `docker logs` access on `oj-control-plane`.

### 8.1 "Contest stuck in REGISTRATION past startTime"

**Symptom.** Contest didn't go ACTIVE at T0. `GET /{id}/key` still 404.

**Diagnose.**
```sh
sudo docker logs oj-contest-service --tail 200 | grep -E "lifecycle|Contest"
sudo docker exec oj-contest-service curl -s localhost:8084/api/v1/contests/<id> | jq .
```

**Likely causes & fixes.**
- *LifecycleWorker stalled.* Restart the container; verify by tailing for the 1 s poll cadence.
- *Activation precondition.* `encryptionKey`/`encryptedBundleUrl` blank on the row. Fix: `POST /{id}/register` to re-fire generation, then `POST /{id}/activate`.
- *Clock skew on control-plane VM.* `timedatectl`; `chronyc makestep` to accelerate NTP recovery.

### 8.2 "Replay failed; some accepted submissions not re-evaluated"

**Symptom.** Contest closed normally; the system-test consumer shows fewer `phase=system` events than expected.

**Diagnose.**
```sh
sudo docker logs oj-contest-service --tail 500 | grep "\\[replay\\]"
sudo docker exec oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute="
  SELECT count(*) FROM submissions s
   JOIN contest_problems cp ON cp.problem_id = s.problem_id
  WHERE cp.contest_id = '<id>' AND s.status = 'ACCEPTED';"
```

**Cause.** Mid-scan Kafka send failed; `RuntimeException` propagated and the loop halted.

**Fix.** No re-drive endpoint today (gap — see §10). Workaround: re-publish the missing submissions via `kafka-console-producer` with the `SubmissionEvent` proto bytes (`phase=system`). Worker idempotency dedupes already-sent submissions. Long-term fix: expose `replayContest(contestId)` via an admin POST.

### 8.3 "Container won't start — schema validation failure"

**Symptom.** Crash-loop. `Schema-validation: missing column [<col>] in table [contests]` or `relation "contests" does not exist`.

**Cause.** api-gateway's Flyway hasn't applied V6/V7, OR entity/migration drift.

**Fix.**
- `sudo docker logs oj-api-gateway --tail 200 | grep Flyway` — confirm V6 + V7 applied.
- If api-gateway is stale, redeploy it first then bounce contest-service.
- Race on clean stack-up (contest-service starts before api-gateway's Flyway): `sudo docker restart oj-contest-service`. Long-term: tighten compose `depends_on` once api-gateway exposes a Flyway-completion health probe.

### 8.4 "Encryption key rotation"

No rotation endpoint today. Pre-ACTIVE, `POST /{id}/register` is idempotent on already-REGISTRATION and does NOT re-mint. To force a rotation: direct CRDB `UPDATE contests SET encryption_key = NULL, encrypted_bundle_url = NULL WHERE id = '<id>'`, then re-fire registration on a code path that bypasses the idempotency check (one-line patch today). Re-upload the encrypted bundle. Rotation is a roadmap item; escalate if you hit this in production.

### 8.5 "Contest stuck CLOSED — won't go to RESULTS"

CLOSED → RESULTS is NOT automated by `LifecycleWorker` (requires Flink-drain confirmation, not wired). Admin-driven only: `curl -X POST http://localhost:8084/api/v1/contests/<id>/results`. The state machine accepts the edge unconditionally today (no precondition).

---

## 9. Tests & verification

### 9.1 Unit tests (`contest-service/src/test/java/`)

| File | Coverage |
|---|---|
| `ContestStateMachineTest` | Forward transitions per pair; preconditions (key + bundle URL gate REGISTRATION → ACTIVE); idempotent same-state transition; rejection of every backward / skip transition; terminal state has no valid next |
| `ContestStateMachineSystemTestReplayTest` | 5 scenarios — ACTIVE → CLOSED fires `replayContest` exactly once; idempotent same-state CLOSED → CLOSED does NOT fire; replay `RuntimeException` does not block the transition; null publisher (legacy constructor path) is tolerated; non-ACTIVE → CLOSED edges do not fire |
| `SystemTestReplayPublisherTest` | 5 scenarios — empty `contest_problems` returns 0; single-page scan; multi-page cursor pagination; Kafka `acks=all` send awaited synchronously; mid-scan send failure throws |
| `EncryptionServiceTest` | Round-trip encrypt → decrypt; tamper detection (GCM tag verification); fresh IV per encryption; wrong-key decrypt throws |
| `ContestLifecycleIntegrationTest` | Full lifecycle CREATED → REGISTRATION → ACTIVE → CLOSED → RESULTS with encryption: key generated at REGISTRATION decrypts the bundle at ACTIVE; the T0 moment validated end-to-end |

Run via `./gradlew :contest-service:test`. Listed inventory in [`../tech-spec.md`](../tech-spec.md) §14 test matrix line: `SystemTestReplayPublisherTest (5 scenarios), ContestStateMachineSystemTestReplayTest (5 scenarios)`.

### 9.2 Integration verification

No TestContainers-backed CRDB integration today. `ContestLifecycleIntegrationTest` is pure in-process (`ContestStateMachine` + `EncryptionService` only, no JPA, no Kafka). Real DB + Kafka integration is the deploy smoke (§9.3).

### 9.3 Manual smoke (post-deploy)

```sh
curl -sX POST http://localhost:8084/api/v1/contests \
  -H 'Content-Type: application/json' \
  -d '{"title":"Smoke","startTime":"2026-05-18T12:00:00Z","endTime":"2026-05-18T13:30:00Z"}' | jq .

sudo docker exec oj-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic contest_events --from-beginning --max-messages 5
```

Replay verification: seed an ACCEPTED submission against a problem in `contest_problems`, `POST /{id}/close`, confirm `submissions.system` receives the corresponding `SubmissionEvent`.

---

## 10. Relevant design docs

- [`../design-docs/contest-close-system-tests-replay.md`](../design-docs/contest-close-system-tests-replay.md) — full rationale for §3.3. Note: today the replay is in-process from the state machine, not via the `contests.lifecycle` Kafka round-trip the doc envisions; the proto `ContestLifecycleEvent` is not yet shipped here.
- [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md) — the 3-broker migration affects the replay's `acks=all` durability story. Single-broker dev compose halts the scan on broker restart; 3-broker ISR=2 tolerates one broker down.
- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — per-region contest-service with CRDB GLOBAL locality on `contests` (sub-10 ms reads from every region at the cost of slow cross-region writes — acceptable because writes are rare).

`contest_events` should eventually be outbox-backed (same pattern as api-gateway's submission outbox) to close the silent-loss gap in §5. No dedicated design doc yet; tracked in the prod-readiness roadmap.

---

## 11. Code map

| Concern | File |
|---|---|
| Spring main class | `contest-service/src/main/java/com/onlinejudge/contest/ContestServiceApplication.java` |
| REST endpoints | `.../controller/ContestController.java` |
| Service layer (lifecycle orchestration, Kafka fanout) | `.../service/ContestService.java` |
| State machine | `.../service/ContestStateMachine.java` |
| Lifecycle poller (REGISTRATION → ACTIVE, ACTIVE → CLOSED) | `.../service/LifecycleWorker.java` |
| System-test replay publisher | `.../service/SystemTestReplayPublisher.java` |
| Native-SQL replay query | `.../service/SubmissionReplayRepository.java` |
| AES-256-GCM encryption | `.../service/EncryptionService.java` |
| `Contest` entity (`contests` table) | `.../model/Contest.java` |
| `ContestState` enum + transition graph | `.../model/ContestState.java` |
| `ContestProblem` entity (`contest_problems` join) | `.../model/{ContestProblem,ContestProblemId}.java` |
| Repositories | `.../repository/{ContestRepository,ProblemContestRepository}.java` |
| Kafka topic names bean | `.../config/KafkaProducerConfig.java` |
| Replay row projection | `.../dto/AcceptedSubmissionRow.java` |
| Dockerfile (multi-stage; gradle build → eclipse-temurin runtime) | `contest-service/Dockerfile` |
| Compose entry | `infra/gcp/compose/control-plane-compose.yml` (the `contest-service:` block) |
| Flyway migrations (api-gateway-owned) | `api-gateway/src/main/resources/db/migration/V6__contests.sql`, `V7__contest_problems.sql` |
