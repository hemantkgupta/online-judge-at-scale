# api-gateway

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the api-gateway service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the auth model in spec form, the system-wide reliability story) live in [`../tech-spec.md`](../tech-spec.md). Forward-looking design lives at [`../design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md).
>
> Read this page if you are: (a) on-call for the public-facing HTTP API, (b) onboarding into the team that owns user-facing endpoints, (c) changing anything in `api-gateway/` or the Flyway migration set.

---

## 1. Purpose

The single public-facing component. Three responsibilities:

1. **User identity.** Signup / login / refresh / logout. Issues JWTs that the rest of the system trusts. Hashes passwords with Argon2id, stores refresh tokens as SHA-256(raw) so the raw value never persists.
2. **Submission acceptance.** `POST /api/v1/submissions` validates auth + size cap, persists the row in CRDB, drops an outbox event in the same transaction. A polling publisher reads the outbox and ships `SubmissionEvent` to Kafka.
3. **Submission status query.** `GET /api/v1/submissions/{id}` returns the latest verdict the gateway has observed (read from Redis cache populated by leaderboard-service, or fall through to CRDB).

It also OWNS the schema for the entire system — Flyway migrations under `api-gateway/src/main/resources/db/migration/` are the canonical source of truth for every JVM service's view of `onlinejudge.*`.

Cross-cutting reliability story (outbox + reconciliation + DLQ) lives in [`../tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms). This page describes how the api-gateway implements its share.

---

## 2. External interfaces

### 2.1 Authentication endpoints

All under `/api/v1/auth/`. Public (no JWT required) except `/logout` which needs a valid access token.

`POST /api/v1/auth/signup`

```http
POST /api/v1/auth/signup
Content-Type: application/json

{ "username": "alice", "password": "<plaintext>" }
```

Returns 201 `{ "userId": "<uuid>" }`. Idempotent on duplicate username: returns 409. Writes a row to `users` with `password_hash` = Argon2id PHC-formatted string and emits a `SIGNUP` row to `auth_events`.

`POST /api/v1/auth/login`

Body `{ username, password }`. Returns 200 with `{ accessToken, refreshToken, expiresIn }` on match; 401 + `LOGIN_FAIL` audit row on mismatch.

`POST /api/v1/auth/refresh`

Body `{ refreshToken }`. Looks up `SHA-256(rawToken)` in `refresh_tokens`. Returns 401 if not found, expired, or revoked. On match, REVOKES the old refresh token and issues a fresh pair. The forced rotation prevents a stolen refresh token from being reused after a legitimate refresh.

`POST /api/v1/auth/logout`

Requires `Authorization: Bearer <access>`. Body `{ refreshToken }`. Marks the refresh token revoked. Idempotent.

The dev-only `POST /api/v1/auth/token` from earlier in the project was DELETED in commit `1b82186`. Don't reintroduce.

### 2.2 Submission endpoints

`POST /api/v1/submissions`

Requires JWT. Body:

```json
{
  "problemId": "<uuid>",
  "contestId": "<uuid or omitted>",
  "language": "python|java|cpp",
  "code": "<source up to 64 KiB>"
}
```

`code` has `@Size(max=65536)`. Spring's validator returns 413 if exceeded. Tomcat also has `server.tomcat.max-swallow-size` + `max-http-form-post-size` capped at 64 KiB so a 50 MB upload doesn't even reach Jackson.

The `userId` is read from `SecurityContextHolder.getContext().getAuthentication().getPrincipal()` — never trust a client-supplied `userId`.

Returns 200:

```json
{
  "submissionId": "<uuid>",
  "status": "PENDING",
  "gatewayTsMs": 1716200000000
}
```

`GET /api/v1/submissions/{id}`

Returns 200 with the latest known verdict, 404 if the submission doesn't exist or doesn't belong to the requesting user.

```json
{
  "submissionId": "...",
  "problemId": "...",
  "language": "python",
  "status": "ACCEPTED" | "WRONG_ANSWER" | "PENDING" | ...,
  "phase": "pretest" | "system",
  "executionTimeMs": 66,
  "memoryUsedMb": 5,
  "perTest": [ ... ]
}
```

### 2.3 Outbound

- **Kafka producer (outbox publisher).** Writes `SubmissionEvent` to `submissions.pretest`. Wire format: protobuf via `KafkaTemplate<String, byte[]>` with `acks=all`. Keyed by `userId` (so Phase 2 verdicts for the same user land on the same partition in Flink).
- **Kafka producer (reconciliation scanner).** Republishes stuck submissions to `submissions.pretest`. Also writes to `submissions.dlq` for attempts-cap-exceeded rows.
- **CRDB.** Spring Data JPA against `onlinejudge`. Connection pool via HikariCP. Default pool size 10; tune via `spring.datasource.hikari.maximum-pool-size`.
- **Redis.** Spring Data Redis for the rate limit Lua script. The submission-status query also reads `verdict-cache:{submissionId}` (TTL 1 h) which leaderboard-service writes on each verdict.
- **OpenTelemetry collector** when `OTEL_JAVAAGENT_ENABLED=true` — OTLP gRPC at `http://oj-otel-collector:4317`.

### 2.4 Listening surface

- `POST/GET /api/v1/auth/*` + `/api/v1/submissions/*` on `:8088` (Tomcat).
- `GET /actuator/health`, `/actuator/info`, `/actuator/metrics`, `/actuator/prometheus` on `:8088`.
- No public ingress on GCP; the only external entry is via a future Cloud Load Balancer (not provisioned yet). On the developer dev loop the port is exposed via the compose `ports: - "8088:8088"`.

---

## 3. Internal design

### 3.1 Auth subsystem

`SecurityConfig` puts these paths on `permitAll`:
- `/actuator/health`
- `/api/v1/auth/signup`
- `/api/v1/auth/login`
- `/api/v1/auth/refresh`

Everything else requires a valid access token via `JwtAuthenticationFilter`. The filter:

1. Reads `Authorization: Bearer <token>` from the request.
2. Parses + verifies the JWT. The `kid` header chooses the key from `app.jwt.keys.{kid}`.
3. Rejects expired / future-dated / wrong-issuer tokens with 401.
4. Populates Spring's SecurityContext with `Authentication(principal=userId, credentials=null)`.

`JwtTokenProvider.issueAccessToken(userId)` returns a signed JWT with claims `{sub: userId, iss: "online-judge", iat, exp, typ: "access"}` and the current `kid` in the header. The signing algorithm is HS256; key length validation rejects keys < 32 bytes.

Refresh tokens are NOT JWTs. They're 32 raw bytes from `SecureRandom`, base64url-encoded. The server stores `SHA-256(rawToken)` in `refresh_tokens.token_hash` (PK). The raw token only exists in the response body; the server has no way to recover it after issuance.

`AuthService`:

```java
public TokenPairResponse login(LoginRequest req, String ip, String userAgent) {
  Optional<User> u = userRepo.findByUsername(req.username);
  if (u.isEmpty() || !argon2.verify(u.get().passwordHash, req.password)) {
    authEventRepo.save(new AuthEvent(null, "LOGIN_FAIL", ip, userAgent, "invalid credentials"));
    throw new BadCredentialsException("login failed");
  }
  String access = jwtProvider.issueAccessToken(u.get().getId());
  RefreshToken rt = new RefreshToken(sha256(raw), u.get().getId(), expiresIn7d);
  refreshRepo.save(rt);
  authEventRepo.save(new AuthEvent(u.get().getId(), "LOGIN_OK", ip, userAgent, null));
  return new TokenPairResponse(access, rawRefreshToken, accessTtlSeconds);
}
```

### 3.2 Outbox publisher

The hot path for submissions. `SubmissionController.create(...)` does:

```java
@Transactional
public SubmissionResponse create(SubmissionRequest req, Authentication auth) {
  validateBodySize(req.code);
  Submission s = new Submission(uuid, auth.userId, req.problemId, req.contestId,
                                 req.language, dataUrl(req.code), "PENDING", regionResolver.current(),
                                 nowMs());
  submissionRepo.save(s);
  OutboxEvent e = new OutboxEvent(uuid, s.getId(), "SUBMISSION_CREATED",
                                   serializeProto(buildSubmissionEvent(s)),
                                   regionResolver.current(), nowMs());
  outboxRepo.save(e);
  return new SubmissionResponse(s.getId(), "PENDING", nowMs());
}
```

Both writes are in one `@Transactional` block; CRDB commits them atomically. The contestant sees 200 the moment the txn commits.

A separate `@Scheduled(fixedRate=500ms)` thread is the outbox publisher:

```java
@Scheduled(fixedDelay = 500)
public void publishOutbox() {
  List<OutboxEvent> batch = outboxRepo.findUnpublished(BATCH_SIZE);
  for (OutboxEvent e : batch) {
    kafkaTemplate.send(pretestTopic, e.submissionId.toString(), e.payloadBytes).get(5, SECONDS);
    e.markPublished();
    outboxRepo.save(e);
    metrics.incOutboxPublished();
  }
}
```

The `.get(5, SECONDS)` is the key — it blocks until the broker has acknowledged the publish. Only then does the row get marked published. If anything fails (Kafka down, broker rejects, timeout), the exception propagates and the row stays unpublished. The next tick retries.

Survives:
- **API gateway crashes between `submissionRepo.save` and `outboxRepo.save`.** The transaction rolls back. The submission row is gone. The contestant gets 500; they retry. No half-state.
- **API gateway crashes between txn commit and Kafka send.** Both rows are in CRDB; the publisher on the next tick (or after gateway restart) finds the unpublished outbox row and sends.
- **Kafka down for minutes.** Publisher keeps failing, rows accumulate. When Kafka comes back, the next tick drains the backlog.

### 3.3 Reconciliation scanner

The reconciliation scanner catches a different failure class. The outbox publisher protects against "I wrote both rows but couldn't reach Kafka". The scanner protects against the much rarer "I committed the submission row but the outbox INSERT itself never made it" — which the single-txn shouldn't allow, but defence in depth.

The scanner runs every 60 s (`app.reconciliation.interval-seconds`):

```java
@Scheduled(fixedDelayString = "${app.reconciliation.interval-seconds:60}000")
@Transactional
public ScanResult sweep() {
  Instant staleBefore = Instant.now().minusSeconds(staleAfterSeconds);
  List<Submission> stuck = repo.findStuckSubmissions(staleBefore, maxAttempts, batchSize);
  int republished = 0, dlq = 0;
  for (Submission s : stuck) {
    if (s.getReconcileAttempts() >= maxAttempts) {
      repo.markFailed(s.getId());
      kafkaTemplate.send(dlqTopic, s.getId().toString(), buildDlqEnvelope(s));
      dlq++;
      continue;
    }
    SubmissionEvent ev = buildSubmissionEvent(s);
    kafkaTemplate.send(pretestTopic, s.getUserId().toString(), ev.toByteArray()).get(5, SECONDS);
    repo.bumpReconcileAttempts(s.getId());
    republished++;
  }
  metrics.recordSweep(stuck.size(), republished, dlq);
  return new ScanResult(stuck.size(), republished, dlq);
}
```

`findStuckSubmissions` uses the partial index `idx_submissions_stuck_pending ON submissions (status, created_at) WHERE status='PENDING'` so the scan is cheap even with millions of submissions in the table.

Defaults:

```yaml
app.reconciliation:
  enabled: true
  interval-seconds: 60
  stale-after-seconds: 900   # 15 min — submissions older than this with status=PENDING are stuck
  batch-size: 500            # cap per sweep; prevents thundering herd after a long outage
  max-attempts: 10           # before DLQ
```

The `reconcile_attempts` column on `submissions` (V8 migration) bounds the retry budget. Beyond 10 attempts the submission is `markFailed` (terminal) and a DLQ envelope is published.

The worker's idempotency layer ensures the re-published events don't double-execute. A re-published `SubmissionEvent` for a submission that already produced a verdict in the worker arrives at `claimSubmission()` which returns COMPLETED → ack.

### 3.4 Rate limiting

`RateLimitService` runs a Lua script against Redis implementing a leaky-bucket. Two limiters today, both pre-`/submissions`:

| Limiter | Bucket key | Capacity | Refill |
|---|---|---|---|
| Per-IP | `rate-limit:ip:{ip}` | `app.rate-limit.per-ip.capacity` (default 60) | `per-ip.refill-per-second` (default 1.0) |
| Per-user | `rate-limit:{userId}` | `app.rate-limit.per-user.capacity` (default 30) | `per-user.refill-per-second` (default 0.5) |

Both rejected requests return 429 with `Retry-After: <seconds>`.

Today's defaults are dev-grade; real production tuning is roadmap §3.2. The auth endpoints share the per-IP bucket with submissions — a brute-force login attempt eats the contestant's submission budget. Splitting them is tracked in [`../design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md).

### 3.5 Flyway migrations

api-gateway owns ALL schema. Every JVM service connects to `onlinejudge` post-§2.2 and `validate`s against it. Migration files at `api-gateway/src/main/resources/db/migration/`:

| Version | Description | What it adds |
|---|---|---|
| V1 | init | `submissions`, `outbox_events`, `idempotency_keys` |
| V2 | add region | `submissions.region`, `outbox_events.region` |
| V3 | unify schema | `users`, `problems`, `test_cases` — landed when api-gateway was repointed at `onlinejudge` (had been on `defaultdb` pre-§2.2). Uses `CREATE TABLE IF NOT EXISTS` so the partial schema from the retired `init.sql` path doesn't conflict. |
| V4 | idempotency attempts | `idempotency_keys.attempts INT NOT NULL DEFAULT 0` |
| V5 | auth | `users.password_hash`, `users.created_at`, `refresh_tokens`, `auth_events` |
| V6 | contests | `contests` table |
| V7 | contest_problems | join table for the contest-close replay path |
| V8 | reconcile_attempts | `submissions.reconcile_attempts`, partial index `idx_submissions_stuck_pending` |

Flyway runs on every container boot. Required compose env (from the deployment that landed in `ceb53ac`):

```yaml
SPRING_FLYWAY_BASELINE_ON_MIGRATE: "true"
SPRING_FLYWAY_BASELINE_VERSION:    "0"
```

`baseline-on-migrate=true` is required because the canonical `onlinejudge` database was previously populated by the retired `init.sql` path — Flyway has to accept the partial pre-existing schema. `baseline-version=0` forces V1 to actually run (V1 is `CREATE TABLE IF NOT EXISTS` so safe to re-apply). Removing either kicks the gateway into a crash-loop on first connect.

---

## 4. Data ownership

api-gateway owns the **public-facing entity model**:

| Table | Writes | Reads |
|---|---|---|
| `users` | this service (signup) | this service (login, JWT validation); contest-service (foreign-key validation) |
| `refresh_tokens` | this service (login, refresh, logout) | this service only |
| `auth_events` | this service | (operator queries; future audit endpoint) |
| `submissions` | this service (create); execution-worker indirectly via outbox; reconciliation scanner (status flip on cap exceed) | this service (get-by-id, scanner stuck-query); contest-service (system-test replay) |
| `outbox_events` | this service (create + mark-published) | this service only |
| `idempotency_keys` | execution-worker | this service does NOT touch |
| `problems`, `test_cases` | seeded manually (admin SQL); future problem-service admin endpoint | problem-service (read) |
| `contests`, `contest_problems` | contest-service | contest-service; api-gateway (ContestWindowFilter) |

Cross-cutting Redis ownership:

| Key | Writes | Reads |
|---|---|---|
| `rate-limit:ip:{ip}` | this service (Lua script) | this service |
| `rate-limit:{userId}` | this service (Lua script) | this service |
| `verdict-cache:{submissionId}` | leaderboard-service | this service (status query) |

Cross-cutting Kafka ownership:

| Topic | Produces | Consumes |
|---|---|---|
| `submissions.pretest` | this service (outbox publisher); reconciliation scanner | execution-worker |
| `submissions.dlq` | this service (reconciliation scanner cap-exceeded); execution-worker (idempotency cap-exceeded) | (manual replay only) |

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| Kafka broker down during outbox publish | `kafkaTemplate.send(...).get(5s)` times out | Row stays unpublished. Next outbox tick retries. Contestant already got 200; verdict will arrive when Kafka recovers. |
| CRDB unreachable mid-transaction | Hikari connection failure | Spring rolls back; client gets 500. Retry with idempotent client behaviour OK because submission UUID hasn't been generated yet. |
| User submits with stolen JWT | `JwtAuthenticationFilter` validates signature + expiry only; doesn't check for revocation | Access token has 15 min TTL → bounded window. To force revocation: rotate the `kid` (logging out everyone). |
| User submits with expired access token | Filter returns 401 | Client refreshes via `/refresh`, retries. |
| Refresh token reused after rotation | `RefreshTokenRepository.findByTokenHash` returns a revoked row | 401 + `REFRESH_FAIL` audit log. Best-effort detection — a determined attacker who got both tokens can race the refresh. |
| 50 MB code upload | Tomcat-level body cap fires before Spring | 413. Spring's `@Size(max=65536)` is the second layer. |
| Reconciliation scanner Kafka publish fails | `kafkaTemplate.send(...).get(5s)` throws | The row's `reconcile_attempts` is NOT bumped (since `bumpReconcileAttempts` is only called on send success). Next sweep retries from the same attempt count. |
| Outbox publisher falls behind | Lag metric `oj.gateway.outbox.unpublished` climbs | Indicates Kafka backpressure OR publisher throttling. Bump `app.outbox.batch-size` OR add a second gateway pod. |
| Two reconciliation scanners run concurrently (e.g. blue/green deploy) | Both sweep, both republish | Worker idempotency dedupes — both events trigger `claimSubmission`, only one wins. Not a correctness bug; wastes a small amount of work. |
| Argon2id native lib missing | Spring fails to load `Argon2PasswordEncoder` at startup | Container crash. Fix: BouncyCastle dep is declared in `build.gradle`; rebuild + push the image. |
| Schema validation fails on container boot | Flyway throws / Hibernate `ddl-auto=validate` throws | Crash-loop. See §3.5 for the baseline-on-migrate env vars. CRDB INT vs JPA `int` is the canonical class — widen the entity field to `long`. |

---

## 6. Configuration reference

`api-gateway/src/main/resources/application.yml`; env override via Spring relaxed binding.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8088` | Tomcat port. |
| `server.tomcat.max-swallow-size` | `64KB` | Hard cap on request body. |
| `server.tomcat.max-http-form-post-size` | `64KB` | Same. |
| `spring.datasource.url` | `jdbc:postgresql://localhost:26257/onlinejudge?sslmode=disable` | CRDB JDBC. Override in compose. |
| `spring.flyway.baseline-on-migrate` | `false` in yml; `true` in compose | Required when `onlinejudge` has pre-existing partial schema from `init.sql`. |
| `spring.flyway.baseline-version` | `0` in compose | Forces V1 to run. |
| `app.jwt.kid-current` | `v1` | Current signing key id. |
| `app.jwt.kid-previous` | `` | One-version rollback window. Empty = no previous accepted. |
| `app.jwt.keys.v1` | `${app.jwt.secret:<dev-default>}` | Actual key bytes. In compose: derived from `JWT_SECRET` env (terraform-generated). |
| `app.jwt.keys.v2` | `` | Used during rotation. |
| `app.jwt.access-ttl-seconds` | `900` | 15 min access token TTL. |
| `app.jwt.refresh-ttl-seconds` | `604800` | 7 day refresh token TTL. |
| `app.jwt.issuer` | `online-judge` | JWT `iss` claim. Validated on every request. |
| `app.rate-limit.per-ip.capacity` | `60` | Per-IP leaky bucket capacity. |
| `app.rate-limit.per-ip.refill-per-second` | `1.0` | |
| `app.rate-limit.per-user.capacity` | `30` | |
| `app.rate-limit.per-user.refill-per-second` | `0.5` | |
| `app.outbox.batch-size` | `100` | Max rows per outbox publisher tick. |
| `app.outbox.poll-interval-ms` | `500` | Outbox tick cadence. |
| `app.reconciliation.enabled` | `true` | Kill-switch. |
| `app.reconciliation.interval-seconds` | `60` | |
| `app.reconciliation.stale-after-seconds` | `900` | PENDING + this old = stuck. |
| `app.reconciliation.batch-size` | `500` | |
| `app.reconciliation.max-attempts` | `10` | Before DLQ. |
| `app.kafka.topic.pretest` | `submissions.pretest` | Outbox + reconciliation destination. |
| `app.kafka.topic.dlq` | `submissions.dlq` | DLQ destination. |
| `app.region` | `${REGION:-asia-south1}` | Stamped on every SubmissionEvent. |

---

## 7. Metrics emitted

`GatewayMetrics` bean wires these. Names prefixed `oj.gateway.*`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.gateway.submission.accepted_total` | counter | `region`, `language` | One per accepted submission. |
| `oj.gateway.submission.rejected_total` | counter | `reason` | Rejected pre-Kafka. Reason: `size_too_large`, `rate_limited`, `invalid_jwt`, `unknown_problem`. |
| `oj.gateway.auth.login_attempts_total` | counter | `outcome` | `success`/`failure`. |
| `oj.gateway.auth.signups_total` | counter | (none) | |
| `oj.gateway.outbox.unpublished` | gauge | (none) | Current count of `outbox_events WHERE published=FALSE`. Should be < 100 in steady state. |
| `oj.gateway.outbox.publish_latency_seconds` | histogram | (none) | Per-row publish latency. |
| `oj.gateway.reconciliation.swept_total` | counter | (none) | Total stuck rows observed across all sweeps. |
| `oj.gateway.reconciliation.republished_total` | counter | (none) | |
| `oj.gateway.reconciliation.dlq_total` | counter | (none) | |
| `oj.gateway.reconciliation.stuck_now` | gauge | (none) | Last sweep's `swept` count. |

The planned **submission funnel** dashboard uses `oj.gateway.submission.accepted_total` as the leftmost panel (the rate at which submissions enter the system) and the reconciliation counters as the right edge (the rate at which they fall off it).

---

## 8. Runbook

### 8.1 "Outbox publisher falling behind" (gauge `oj.gateway.outbox.unpublished` > 100)

**Diagnose.**
```sh
sudo docker exec -i oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute="
  SELECT count(*), max(created_at) FROM outbox_events WHERE published=FALSE;
"
sudo docker logs oj-api-gateway --tail 100 | grep -E "outbox|ERROR|TimeoutException"
```

**Likely cause & fix.**
- *Kafka is slow.* Check broker health on `oj-control-plane`. The 5 s `.get(5, SECONDS)` timeout will throw on the first batch row; the publisher logs at ERROR. Restart Kafka or wait it out.
- *Outbox table too large.* If the `published=FALSE` rows accumulate to thousands, the publisher's query `SELECT FROM outbox_events WHERE published=FALSE` becomes slow. Confirm `idx_outbox_region_unpublished` exists; if not, add. Long-term: a TTL on published rows (not yet implemented).

### 8.2 "Login endpoint timing out under load"

**Symptom.** `/api/v1/auth/login` p99 > 5 s; user complaints.

**Diagnose.** Argon2id verification is intentionally expensive (~50-150 ms per call). With high concurrency, the Tomcat thread pool can fill.

```sh
sudo docker exec oj-api-gateway sh -c 'curl -s localhost:8088/actuator/metrics/http.server.requests | jq .'
```

**Fix.**
- *Short-term.* Bump Tomcat thread count via `server.tomcat.threads.max` (default 200).
- *Long-term.* Per-IP rate limit on `/auth/login` separately from `/submissions` (roadmap §3.2 + design doc).

### 8.3 "All users seeing 401 on every request"

**Symptom.** JWT validation rejecting every token. New tokens still issued by `/login` but rejected on `/submissions`.

**Likely cause.** `app.jwt.kid-current` was rotated but `app.jwt.keys.{kid}` wasn't updated, so the gateway can't verify the tokens it just issued. Operator config drift.

**Fix.** Check `JWT_KID_CURRENT` and `JWT_KEY_*` env vars on the running container:
```sh
sudo docker exec oj-api-gateway printenv | grep -E "JWT_"
```

Reconcile. If you're mid-rotation, ensure `JWT_KID_PREVIOUS` is set + the previous key is still in the keys map.

### 8.4 "Submission status query returns PENDING forever for a submission whose verdict went out"

**Symptom.** `GET /submissions/{id}` says PENDING but `kafka-console-consumer` on `evaluated_results` shows the verdict was published.

**Likely cause.** leaderboard-service crashed before populating `verdict-cache:{submissionId}` in Redis. The gateway's status query falls through to CRDB, where the status flip from PENDING is currently DRIVEN BY leaderboard-service — not by the worker. (Yes, that's a coupling smell; documented in tech-spec §14.)

**Fix.** Restart leaderboard-service. Re-consume `evaluated_results` from earliest with `auto.offset.reset=earliest` on the leaderboard consumer group to backfill the cache. Long-term: have the worker write the verdict to CRDB directly post-publish.

### 8.5 "Flyway baseline error: 'Found non-empty schema(s) but no schema history table'"

**Symptom.** Gateway crash-loops at startup. Log shows the Flyway error.

**Cause.** Connecting to `onlinejudge` for the first time post-§2.2, where `init.sql` had already created tables. Flyway refuses to proceed without baseline configuration.

**Fix.** Confirm compose env carries:
```yaml
SPRING_FLYWAY_BASELINE_ON_MIGRATE: "true"
SPRING_FLYWAY_BASELINE_VERSION:    "0"
```

If you see this error AND those are set, the orphan tables from `init.sql` need to be DROP'd first. The operator runbook is `infra/scripts/db-migration-onlinejudge.sql`.

---

## 9. Tests & verification

### 9.1 Unit tests (`api-gateway/src/test/java/`)

| File | Coverage |
|---|---|
| `AuthServiceTest` | Signup happy / duplicate; login wrong / right password; refresh after revoke; refresh after expiry; logout idempotent; audit-event rows written |
| `JwtTokenProviderTest` | Issue + verify round-trip; expired token rejected; wrong issuer rejected; legacy 3-arg ctor preserved for back-compat |
| `JwtAuthenticationFilterTest` | Bearer-token extraction; SecurityContext populated; missing token → 401 |
| `ReconciliationScannerTest` | Empty / republished / cap-exceeded / Kafka-fail / proto round-trip / configured-window / partial-batch-failure (7 scenarios) |
| `SubmissionControllerTest` | Validation (size cap, missing fields); body-size 413; userId comes from SecurityContext not body |
| `RateLimitServiceTest` | Bucket exhaustion; Lua script atomicity (single-threaded simulation) |

Run via `./gradlew :api-gateway:test`.

### 9.2 Integration verification

Today there's no live integration test against a TestContainers CRDB + Kafka; the worker's smoke (`sum-of-two`) exercises the full pipeline post-publish, and the gateway endpoints are spot-checked via curl in `docs/ci-cd.md`'s smoke walkthrough.

A proper TestContainers integration is on the gap list — would catch Flyway-on-fresh-CRDB regressions before they hit production.

### 9.3 Manual smoke

```sh
# Signup
curl -sX POST http://localhost:8088/api/v1/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"hunter2!Hunter"}'

# Login
TOKEN=$(curl -sX POST http://localhost:8088/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"hunter2!Hunter"}' | jq -r .accessToken)

# Submit
curl -sX POST http://localhost:8088/api/v1/submissions \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"problemId":"00000000-0000-0000-0000-0000000cafee","language":"python","code":"print(2+2)"}'
```

---

## 10. Relevant design docs

- [`../design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md) — full design context for §3.1, including the rate-limit split that's still TODO.
- [`../design-docs/ci-cd-github-actions.md`](../design-docs/ci-cd-github-actions.md) — the deploy workflow that bumps this service's image. Defines required repo secrets.
- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — affects this service most because per-region gateway pods with geo-routed DNS is the entry point for the multi-region story.

The reconciliation scanner is implementation-only — no design doc beyond the roadmap entry; the section in this page is its definitive spec.

---

## 11. Code map

| Concern | File |
|---|---|
| Auth REST | `api-gateway/src/main/java/com/onlinejudge/gateway/controller/AuthController.java` |
| Submission REST | `.../controller/SubmissionController.java` |
| Auth service (Argon2 + token rotation) | `.../service/AuthService.java` |
| JWT issue + verify + rotation | `.../security/{JwtTokenProvider,JwtAuthenticationFilter,SecurityConfig}.java` |
| Outbox publisher | `.../publisher/OutboxPublisher.java` |
| Reconciliation scanner | `.../scanner/{ReconciliationScanner,StuckSubmissionRepository}.java` |
| Rate limit (Lua) | `.../service/RateLimitService.java` |
| User / RefreshToken / AuthEvent entities | `.../entity/{User,RefreshToken,AuthEvent}.java` |
| Repositories | `.../repository/` |
| Flyway migrations | `.../resources/db/migration/V1__init.sql` ... `V8__reconcile_attempts.sql` |
| Spring main class | `.../ApiGatewayApplication.java` |
