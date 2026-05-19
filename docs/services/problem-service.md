# problem-service

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the problem-service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the auth model, the system-wide reliability story) live in [`../tech-spec.md`](../tech-spec.md). The execution-worker that consumes this service's output is documented at [`./execution-worker.md`](./execution-worker.md).
>
> Read this page if you are: (a) on-call for the test-case fetch path, (b) onboarding into the team that owns problem authoring, (c) changing anything in `problem-service/` or the V4-signing layer.

---

## 1. Purpose

The narrow waist between the `problems` / `test_cases` CRDB rows and the GCS bytes the execution-worker actually consumes. Two things matter:

1. **It reads the canonical problem definition** from `onlinejudge.problems` + `onlinejudge.test_cases` and turns it into a worker-consumable response (`time_limit_ms`, `memory_limit_mib`, ordered list of per-ordinal `(input_url, expected_output_url)`).
2. **It signs short-lived V4 GCS download URLs** so the worker can fetch test-case bytes without GCS-side IAM. This is the ONLY component that holds the signer service account's private key in process.

Everything else is incidental. There's no Kafka, no Redis, no auth, no admin UI today. A single GET endpoint and a small amount of Spring + JPA + the Google Cloud Storage SDK.

The "why V4 signing instead of ADC tokens" rationale lives in [`../tech-spec.md#73-v4-signed-gcs-urls`](../tech-spec.md#73-v4-signed-gcs-urls). This page focuses on how the service implements its part.

---

## 2. External interfaces

### 2.1 REST API

`GET /api/v1/problems/{problemId}/test-cases?pretestOnly={true|false}`

Returns the test-case bundle for a problem. Called by the execution-worker once per submission.

```http
GET /api/v1/problems/00000000-0000-0000-0000-0000000cafee/test-cases?pretestOnly=true
```

Response 200:

```json
{
  "time_limit_ms": 1000,
  "memory_limit_mb": 64,
  "test_cases": [
    {
      "ordinal": 1,
      "input_url": "https://storage.googleapis.com/oj-test-cases-online-judge-hk/cafee/1/input.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=...&X-Goog-Signature=...",
      "expected_output_url": "https://storage.googleapis.com/.../expected.txt?X-Goog-..."
    },
    { "ordinal": 2, "input_url": "...", "expected_output_url": "..." },
    ...
  ]
}
```

URLs are V4-signed with a 5-minute TTL. When `pretestOnly=true`, only ordinals 1–10 are returned (the convention: ordinals 1–10 are pretests, 11+ are system tests). When `pretestOnly=false`, the full suite is returned.

Failures:
- **404** — problemId doesn't exist in `problems`.
- **500** — GCS signer key file unreadable, JCA RSA-SHA256 fails, JDBC down. Operator-visible immediately.

The signing happens in-process (no API roundtrip to GCS) so the URL response time is bounded by the JCA + JDBC + N test-case rows. Typical latency for a 10-ordinal pretest fetch is < 100 ms.

`GET /actuator/health`

Spring Boot actuator. Returns `{"status":"UP"}` once the signer SA JSON is parsed + a CRDB connection is acquired.

### 2.2 Outbound

- **CRDB.** Spring Data JPA against `onlinejudge`. Two entities: `Problem` (read), `TestCase` (read). No writes today (admin endpoint is a roadmap item).
- **GCS.** **No runtime API calls.** V4 signing is offline: take the canonical request string, RSA-SHA256-sign it with the SA's private key from disk, attach the signature to the URL. Done. The downstream consumer (the worker) actually fetches the bytes via the signed URL.
- **OpenTelemetry collector** when `OTEL_JAVAAGENT_ENABLED=true` — OTLP gRPC at `http://oj-otel-collector:4317`.

### 2.3 Listening surface

- `GET /api/v1/problems/{id}/test-cases` on `:8089` (Tomcat).
- `GET /actuator/health`, `/actuator/metrics`, etc. on `:8089`.
- Internal-only — the firewall rule `oj-allow-internal` allows the worker's compute VM to reach it on `:8089`.

---

## 3. Internal design

### 3.1 Test-case fetch flow

`ProblemController.getTestCases(problemId, pretestOnly)` does:

```java
@GetMapping("/api/v1/problems/{problemId}/test-cases")
public TestCaseBundleDto getTestCases(@PathVariable String problemId,
                                       @RequestParam(defaultValue = "false") boolean pretestOnly) {
  Problem p = problemRepo.findById(UUID.fromString(problemId))
    .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "problem not found"));
  List<TestCase> cases = testCaseRepo.findByProblemId(p.getId(), pretestOnly ? 10 : Integer.MAX_VALUE);
  return new TestCaseBundleDto(
      Math.toIntExact(p.getTimeLimitMs()),
      Math.toIntExact(p.getMemoryLimitMb()),
      cases.stream().map(tc -> new TestCaseUrlsDto(
          tc.getOrdinal(),
          gcsSigner.signGet(bucket, tc.getInputGcsKey(), URL_TTL),
          gcsSigner.signGet(bucket, tc.getExpectedOutputGcsKey(), URL_TTL)
      )).toList()
  );
}
```

`findByProblemId` uses `idx_test_cases_problem_ordinal` (composite index on `(problem_id, ordinal)`) and orders by ordinal ascending. `pretestOnly` translates to `ordinal <= 10` in the query.

`Math.toIntExact` is deliberate — `Problem.time_limit_ms` and `memory_limit_mb` are `long` (post-§2.2 entity widening to match CRDB BIGINT). The DTO and the wire shape carry `int` since contest problems don't have billion-millisecond time limits. An out-of-range value blows up loudly rather than silently truncating.

### 3.2 GCS V4 signing

`GcsSigner.signGet(bucket, objectKey, ttlSeconds)` is the heart of this service. Algorithm summarised:

1. Build the canonical query parameters: `X-Goog-Algorithm=GOOG4-RSA-SHA256`, `X-Goog-Credential=<sa-email>/<date>/auto/storage/goog4_request`, `X-Goog-Date=<UTC>`, `X-Goog-Expires=<ttl-seconds>`, `X-Goog-SignedHeaders=host`.
2. Build the canonical request string per GCS V4 spec: HTTP method + canonical URI + canonical query + canonical headers + signed headers + payload (UNSIGNED-PAYLOAD).
3. Build the string-to-sign: `GOOG4-RSA-SHA256` + datetime + credential scope + SHA-256(canonical request).
4. RSA-SHA256-sign the string-to-sign with the SA's private key (read from `GCS_SIGNER_KEY_PATH` at app startup, kept in JVM memory).
5. Hex-encode the signature; append to the URL as `X-Goog-Signature=<hex>`.
6. Return `https://storage.googleapis.com/<bucket>/<object-key>?<canonical-query>&X-Goog-Signature=<hex>`.

The Google Cloud Storage SDK does steps 1–5 internally when you call `storage.signUrl(BlobInfo, ttl, TimeUnit, SigningOptions.withV4Signature())`. We use the SDK; the manual derivation above is for reference when debugging signature mismatches.

URL TTL is configurable via `GCS_URL_TTL_SECONDS` env, default 300 s.

### 3.3 Signer key loading

`GcsConfig.gcsSigner()` is a `@Bean` factory:

```java
@Bean
public Storage storage(@Value("${gcs.signer-credentials-path:}") String keyPath,
                       @Value("${gcs.project-id}") String projectId) {
  if (!keyPath.isBlank()) {
    try (InputStream in = new FileInputStream(keyPath)) {
      ServiceAccountCredentials creds = ServiceAccountCredentials.fromStream(in);
      return StorageOptions.newBuilder()
          .setProjectId(projectId).setCredentials(creds).build().getService();
    }
  }
  // Local-dev fallback: no signing key, falls through to ADC. V4 signing won't
  // work in this mode — the runtime path emits an error if signUrl is invoked.
  return StorageOptions.newBuilder().setProjectId(projectId).build().getService();
}
```

The key file is loaded ONCE at startup. The credentials object lives in JVM memory for the process lifetime. The on-disk file is mounted read-only from `/var/secrets/gcs-signer.json` (compose volume from `/opt/oj/gcs-signer.json` on the host). The host's signer key is fetched from Secret Manager by `region.sh.tpl` on every VM boot.

**Rotation.** Monthly via Cloud Scheduler → Cloud Function `oj-rotate-signer-key`, which mints a fresh SA key and writes it to Secret Manager. On each VM the `oj-refresh-secrets.timer` systemd timer mirrors the secret to `/opt/oj/gcs-signer.json` every 60 s and runs `docker restart oj-problem-service` when the bytes change. The bounce is sub-second; the worker's `ack.nack(5s)` retry on test-case GETs absorbs the gap. Why a bounce rather than hot-reload: Google's `ServiceAccountCredentials` is intentionally immutable, and V4-signed URLs already carry a 5-minute TTL so the "overlap window" we need for outstanding URLs is free. See [`../design-docs/key-rotation.md`](../design-docs/key-rotation.md) for the full design.

### 3.4 Health check + readiness

`/actuator/health` succeeds iff:
- Hikari can acquire a CRDB connection (Spring Boot's `DataSourceHealthIndicator`).
- The Storage client was constructed (i.e. the signer key was parsed). There is NO check that signing actually works at health-check time; a misconfigured key still passes health but fails on the first `signUrl` call.

The startup-time check is implicit: if `GCS_SIGNER_KEY_PATH` is set but the file is missing / unreadable / malformed, `GcsConfig.storage()` throws and the container crashes with a clear log.

---

## 4. Data ownership

| Resource | Lifetime | Where |
|---|---|---|
| Signer SA private key | per-VM-boot AND per-rotation (`oj-refresh-secrets.timer` re-fetches every 60 s; bounces the container when bytes change) | `/opt/oj/gcs-signer.json` on host; mounted into container at `/var/secrets/gcs-signer.json` (ro, mode 0400). Rotation orchestration in [`../design-docs/key-rotation.md`](../design-docs/key-rotation.md) |
| Storage client + parsed credentials | process lifetime | JVM heap |
| `problems` rows | (read-only here; api-gateway Flyway owns the schema, operator SQL writes today) | `onlinejudge.problems` |
| `test_cases` rows | (read-only here) | `onlinejudge.test_cases` |
| GCS objects (input + expected bytes) | per-problem (uploaded by operator at problem-seed time) | `gs://oj-test-cases-<projectId>/<problem-slug>/<ordinal>/{input,expected}.txt` |

problem-service does NOT touch:
- Kafka.
- Redis.
- Any write to CRDB.
- Any other service's data.

If problem-service is restarted, no state is lost. The next request fetches from CRDB + signs fresh URLs.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| Signer key file missing at startup | `FileNotFoundException` in `GcsConfig.storage()` | Container fails to start. Log: "GCS_SIGNER_KEY_PATH does not exist". Fix: re-fetch from Secret Manager on the host (the control-plane startup script does this; manual override via `gcloud secrets versions access`). |
| Signer key file malformed | `ServiceAccountCredentials.fromStream` throws | Same as missing — container fails to start. |
| `gcs.signer-credentials-path` empty (ADC fallback) | Container starts but `signUrl` throws at request time | 500 on every test-case fetch. The local-dev path; production must have the key. |
| problemId not in CRDB | `problemRepo.findById` returns Optional.empty() | 404. Common cause: a contestant submitted a stale problem reference. Worker handles by failing the submission. |
| `test_cases` table empty for an existing problem | Empty list returned to worker | Worker sees zero ordinals, immediately publishes RUNTIME_ERROR (no test cases to compare against). Operator should detect via seed-step verification. |
| CRDB connection lost mid-request | Spring exception → 500 | Worker `ack.nack(5s)`. The connection pool recovers; next request succeeds. |
| Signing key revoked at GCP side | URLs still SIGN successfully but GCS returns 403 on use | Detected ONLY at worker GET time, not at problem-service sign time. The worker's `TestCaseFetcher` throws on the 403; submission goes to `INTERNAL_ERROR`. Operator must rotate the key (terraform `taint` + apply OR Secret Manager version bump). |
| Schema validation fails on container boot | Hibernate `ddl-auto=validate` throws | Container crash-loops. CRDB INT vs JPA `int` is the canonical class — entity fields widened to `long` in commit `dd7eb45`. |
| GCS_BUCKET env unset / wrong bucket | Signing succeeds but URLs point at a non-existent bucket → 403 at fetch | Same diagnostic flow as revoked key. |

---

## 6. Configuration reference

`problem-service/src/main/resources/application.yml`; env override via Spring relaxed binding. Defaults shown.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8089` | Tomcat port. |
| `spring.datasource.url` | `jdbc:postgresql://localhost:26257/onlinejudge?sslmode=disable` | CRDB JDBC. Override via `DB_URL` env. |
| `spring.datasource.username` | `root` | Override via `DB_USER`. |
| `spring.datasource.password` | `` | Override via `DB_PASSWORD`. |
| `spring.jpa.hibernate.ddl-auto` | `validate` | Schema validation against entity types. Must be `validate`; never `update` (api-gateway Flyway owns the schema). |
| `gcs.project-id` | `online-judge-hk` | GCP project hosting the test-cases bucket. Override via `GCS_PROJECT_ID`. |
| `gcs.bucket` | `oj-test-cases` | Bucket name. Override via `GCS_BUCKET`. On terraform-driven deploys this is `oj-test-cases-<project>` due to the suffix convention. |
| `gcs.signer-credentials-path` | `` (empty → ADC fallback, dev-only) | Path to the signer SA JSON. Override via `GCS_SIGNER_KEY_PATH`. Production: `/var/secrets/gcs-signer.json`. |
| `gcs.url-ttl-seconds` | `300` | Signed URL TTL. 5 minutes is the right balance: long enough that a slow microVM boot doesn't fail the GET, short enough that a leaked URL is a small window. |
| `app.region` | `${REGION:-asia-south1}` | Stamped on metrics; not behaviourally meaningful. |
| `JAVA_TOOL_OPTIONS` | `-Xmx384m -XX:+ExitOnOutOfMemoryError` | Set in compose. Tight heap because the control-plane VM is at ~4.2 GB committed and problem-service is a small JVM. |

---

## 7. Metrics emitted

problem-service is small; the metrics catalogue is correspondingly small. Names prefixed `oj.problem.*`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.problem.test_cases.fetch_total` | counter | `problem_id`, `pretest_only` | One per GET. Spike on a single problem_id = a contest is live for that problem. |
| `oj.problem.test_cases.fetch_latency_seconds` | histogram | (none) | End-to-end fetch handler latency. Should be < 100 ms p99 for a 10-ordinal pretest. |
| `oj.problem.sign.latency_seconds` | histogram | (none) | V4-sign call latency. Pure CPU (RSA-SHA256); should be sub-ms per call. |
| `oj.problem.cache_miss_total` | counter | (none) | The test-case list isn't cached today; this metric reserved for a future cache layer (per-problem caching on the assumption that a problem's test cases don't change once authored). |
| `oj.problem.not_found_total` | counter | (none) | 404 responses. Sustained > 0 = clients are referencing stale problem IDs OR an operator deleted a problem mid-contest. |

The submission-funnel dashboard touches this with a single line — "problem-service GET latency" — sitting between worker's gateway hop and the worker's GCS hop.

---

## 8. Runbook

### 8.1 "Worker can't reach problem-service / test-case GETs failing"

**Symptom.** Worker logs `[test-fetcher] HTTP 500` or `connection refused` to `oj-control-plane:8089`.

**Diagnose.**
```sh
# Container alive?
gcloud compute ssh oj-control-plane --zone=asia-south1-a --tunnel-through-iap --command='
  sudo docker ps --filter name=oj-problem-service --format "table {{.Names}}\t{{.Status}}"
  sudo docker logs oj-problem-service --tail 80 | tail -40
'

# Cross-VM reachability from compute side
gcloud compute ssh oj-compute --zone=asia-south1-a --tunnel-through-iap --command='
  curl -v --max-time 5 http://10.0.0.2:8089/actuator/health
'
```

**Likely cause & fix.**
- *Container crashed on Hibernate validate.* Pre-§2.2, `ddl-auto: validate` would fail on the CRDB INT vs `int` mismatch. Fix: confirm entity uses `long`; re-pull image post commit `dd7eb45`.
- *Signer key missing.* `gcloud secrets versions access latest --secret=oj-problem-signer-key > /opt/oj/gcs-signer.json` on the host; bounce the container.
- *Firewall rule changed.* `oj-allow-internal` should permit 10.0.0.3 → 10.0.0.2:8089. Verify the terraform `google_compute_firewall.allow_internal` rule still has source range 10.0.0.0/24.

### 8.2 "All signed URLs return 403 from GCS"

**Symptom.** Worker logs `[test-fetcher] test-case GET HTTP 403` for every URL.

**Diagnose.**
```sh
# Pull a fresh URL and curl it directly
gcloud compute ssh oj-control-plane --zone=asia-south1-a --tunnel-through-iap --command='
  curl -s "http://localhost:8089/api/v1/problems/00000000-0000-0000-0000-0000000cafee/test-cases?pretestOnly=true" | jq -r ".test_cases[0].input_url"
' | xargs curl -v --max-time 10
```

**Likely cause & fix.**
- *Signer SA lost `roles/storage.objectViewer` on the bucket.* Reapply via terraform. Verify via `gcloud storage buckets get-iam-policy gs://oj-test-cases-<project>`.
- *Signer key rotated but the new version isn't on disk.* Re-fetch from Secret Manager. If the secret has multiple versions, ensure `--secret-version=latest` selects the right one.
- *URL TTL expired between sign and use.* If the worker is slow to dispatch (cold pool), the 5-minute TTL might be tight. Verify the timestamp on the URL is within the last 5 minutes; bump `GCS_URL_TTL_SECONDS` if cold-starts genuinely exceed 5 min (they shouldn't).

### 8.3 "Some problems return 404; others work"

**Symptom.** `GET /test-cases?problem_id=X` 404s for one problem but works for others.

**Diagnose.**
```sh
sudo docker exec -i oj-cockroachdb cockroach sql --insecure --database=onlinejudge --execute="
  SELECT id::STRING, title, time_limit_ms FROM problems WHERE id = '<UUID>';
  SELECT count(*) FROM test_cases WHERE problem_id = '<UUID>';
"
```

If problems row exists but test_cases is empty: the seeder missed the test-case INSERT. Re-seed via `infra/firecracker/test/problems/<problem-slug>/seed.sql` (when it exists) OR manually:
```sql
INSERT INTO test_cases (id, problem_id, ordinal, input_gcs_key, expected_output_gcs_key) VALUES (...);
```

If neither row exists: the problem was never seeded. Today's seed flow is manual (admin endpoint is a roadmap item); refer to the `sum-of-two` problem in `infra/firecracker/test/problems/` for the canonical example.

### 8.4 "URL signing succeeds but the worker still says hash mismatch"

**Symptom.** `oj.problem.sign.latency_seconds` is healthy; worker fetches the signed URLs (no 403); but every verdict is WRONG_ANSWER on a known-correct submission.

**Likely cause.** The expected output object on GCS doesn't match what `canonicalHash` expects.

**Fix.** Pull the bytes via the signed URL:
```sh
# get a fresh URL
URL=$(curl -s "http://localhost:8089/api/v1/problems/<problem-id>/test-cases?pretestOnly=true" | jq -r '.test_cases[0].expected_output_url')
curl -s "$URL" | xxd | head
# Compute the expected canonical hash
curl -s "$URL" | python3 -c "import sys,hashlib;b=sys.stdin.buffer.read();print(hashlib.sha256(b.rstrip().decode('utf-8').encode('utf-8')).hexdigest())"
```

Compare against the hash the worker would compute. Mismatch → re-upload the GCS object with correct bytes (UTF-8, no BOM, content matches reference solution output).

### 8.5 "signer key rotated; problem-service didn't pick it up"

**Symptom.** A new Secret Manager version of `oj-problem-signer-key` was written (manually or by the Cloud Function) more than 90 s ago, but `/opt/oj/gcs-signer.json` on the VM still has the old bytes — or the file changed but the container is still serving signed URLs verified with the old key.

**Diagnose.**
```sh
gcloud compute ssh oj-region-a --zone=asia-south1-a --tunnel-through-iap --command='
  # Did the systemd timer run?
  systemctl status oj-refresh-secrets.timer
  journalctl -u oj-refresh-secrets.service --since "10 min ago" --no-pager | tail -40

  # When was the file last updated?
  stat /opt/oj/gcs-signer.json | grep Modify

  # Is the container still up?
  sudo docker ps --filter name=oj-problem-service --format "table {{.Status}}"
'
```

**Likely cause & fix.**
- *Timer disabled.* `systemctl enable --now oj-refresh-secrets.timer`. The startup script installs and enables it; a manual `systemctl disable` is the only way it gets turned off.
- *gcloud auth lapsed on the VM.* The region SA should have `roles/secretmanager.secretAccessor`. Verify the binding in `infra/gcp/terraform/key-rotation.tf`.
- *Bounce didn't happen.* The refresh script `docker restart`s the container only when the file bytes change. If the file was overwritten with identical content, no bounce. Force one: `sudo docker restart oj-problem-service`.
- *Bounce happened but the bean cached the OLD credentials.* The bean lifetime IS the container lifetime; a restart MUST give a fresh credentials object. If this is somehow not the case, that's a Spring DI bug — capture the JVM heap and open an incident.

See [`../design-docs/key-rotation.md`](../design-docs/key-rotation.md#rotation-flow-gcs-v4-signer-sa-key) for the full flow.

### 8.6 "problem-service heap pressure / OOM"

**Symptom.** Container restart with `OutOfMemoryError` in logs. JVM heap pegged.

**Likely cause.** The `-Xmx384m` is tight. A spike in concurrent requests + many test cases per request can balloon working memory.

**Fix.**
- *Short-term.* Bump JAVA_TOOL_OPTIONS in compose to `-Xmx512m`. Watch the control-plane VM memory headroom (already tight; see `tech-spec.md` deployment §10.1).
- *Long-term.* Bump the control-plane VM to e2-standard-2 (8 GB) — this is the canonical roadmap item from when contest-service + leaderboard-service were added.

---

## 9. Tests & verification

### 9.1 Unit tests (`problem-service/src/test/java/`)

| File | Coverage |
|---|---|
| `ProblemControllerTest` | Happy path 200; 404 on unknown problemId; `pretestOnly=true` limits ordinals; response shape matches DTO contract |
| `ProblemServiceFilteringTest` | `findByProblemId` filter by ordinal threshold; ordering by ordinal ascending |
| `GcsSignerTest` (if present) | Smoke-test the SDK call with a fake credentials object; signed URL shape sanity |

Run via `./gradlew :problem-service:test`.

### 9.2 Integration verification

Today there's no TestContainers-backed integration test (CRDB + fake-GCS). The production smoke is the canonical end-to-end check:

```sh
# Pull live signed URLs from a running problem-service
curl -s http://10.0.0.2:8089/api/v1/problems/00000000-0000-0000-0000-0000000cafee/test-cases?pretestOnly=true | jq .

# Sanity-check one of the URLs actually fetches
URL=$(curl -s "http://10.0.0.2:8089/api/v1/problems/.../test-cases?pretestOnly=true" | jq -r '.test_cases[0].input_url')
curl -s "$URL"
```

### 9.3 Manual smoke

The smaller `print(2+2)` problem (UUID `…cafee`) + the larger `sum-of-two` problem (UUID `11111111-2222-3333-4444-555555555555`) are the two known-good fixtures. Both have working GCS objects + DB rows; both have been verified end-to-end during the development session.

---

## 10. Relevant design docs

- [`../design-docs/auth-end-to-end.md`](../design-docs/auth-end-to-end.md) — currently problem-service has no auth. The auth design covers when (and if) problem-service should require a service-to-service JWT for the worker call.
- [`../design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md) — per-region problem-service deployment + signed URLs from a regional bucket.

The problem authoring + admin API is a roadmap item with no dedicated design doc yet; the rough plan lives in the prod-readiness roadmap §4.18.

---

## 11. Code map

| Concern | File |
|---|---|
| REST controller | `problem-service/src/main/java/com/onlinejudge/problem/controller/ProblemController.java` |
| Service layer (filter + assemble DTO) | `.../service/ProblemService.java` |
| GCS signer wrapper | `.../service/GcsSigner.java` |
| GCS storage client `@Bean` | `.../config/GcsConfig.java` |
| Entities | `.../entity/{Problem,TestCase}.java` |
| Repositories | `.../repository/{ProblemRepository,TestCaseRepository}.java` |
| Response DTOs | `.../dto/{TestCaseBundleDto,TestCaseUrlsDto,ProblemDto}.java` |
| Spring main class | `.../ProblemServiceApplication.java` |
| Dockerfile | `problem-service/Dockerfile` (multi-stage; gradle build → eclipse-temurin runtime) |
| Compose entry | `infra/gcp/compose/control-plane-compose.yml` (the `problem-service:` block) |
| GCS bucket + signer SA terraform | `infra/gcp/terraform/main.tf` (the `oj_test_cases` bucket + `problem_service_signer` SA + Secret Manager wiring) |
