# execution-worker

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the execution-worker service. Cross-cutting concerns (proto schema, Kafka topic catalogue, the auth model, the system-wide reliability story) live in [`../tech-spec.md`](../tech-spec.md). The sandbox lifecycle this service consumes is documented at [`./sandbox-manager.md`](./sandbox-manager.md).
>
> Read this page if you are: (a) on-call for the submission pipeline, (b) onboarding into the team that owns it, (c) changing anything in `execution-worker/` or the Kafka contract.

---

## 1. Purpose

The translator between the asynchronous Kafka world and the synchronous lease/exec API. One submission in, one verdict out, exactly once per `(submission, phase)` pair.

Concrete responsibilities:

1. **Kafka consumer.** Pulls `SubmissionEvent` from `submissions.pretest` (Phase 1) and `submissions.system` (Phase 2). Two separate consumer groups, two concurrency settings (4 for pretest, 2 for system).
2. **Source code resolution.** Resolves the `s3_code_url` from the event into contestant source bytes — `data:`, `gs://`, `s3://`, `r2://`, `http(s)://` (last three are TODO).
3. **Test-case fetch.** Calls `problem-service` for the per-ordinal signed URLs + per-problem time/memory limits. Downloads the inputs + expected outputs from GCS. Canonical-hashes the expected outputs.
4. **Sandbox lease.** Calls the sandbox-manager's `/lease` REST endpoint to acquire a warm microVM.
5. **Agent dispatch.** Sends the full per-test request (code + inputs + expected hashes + time limit) to the in-guest agent in a single JSON message over vsock via the `oj-vsock-client` Go bridge. Receives a single response with per-ordinal verdicts.
6. **Idempotency claim.** Holds a phase-scoped `idempotency_keys` row across the lifetime of the execution; releases on transient backpressure, marks completed on success, marks poison after `max-attempts` cycles.
7. **Verdict publish.** Emits `VerdictEvent` to `evaluated_results` (with the `per_test` breakdown) and a slimmer `AnalyticsEvent` to `analytics`.
8. **Phase 1 → Phase 2 promotion.** On Phase 1 ACCEPTED, re-publishes the original `SubmissionEvent` to `submissions.system` so the system-test suite runs.

The cross-cutting reliability story (outbox, idempotency, DLQ, pool-exhausted retry) is in [`../tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms). This page focuses on how the worker implements its part.

---

## 2. External interfaces

### 2.1 Kafka

| Direction | Topic | Group / role | Wire format |
|---|---|---|---|
| Consume | `submissions.pretest` | group `execution-worker-pretest`, concurrency 4 | proto `SubmissionEvent` (fallback: JSON envelope from CRDB changefeed; auto-detected) |
| Consume | `submissions.system` | group `execution-worker-system`, concurrency 2 | proto `SubmissionEvent` |
| Produce | `evaluated_results` | acks=all, keyed by user_id | proto `VerdictEvent` (with `per_test`) |
| Produce | `analytics` | fire-and-forget, keyed by submission_id | proto `AnalyticsEvent` |
| Produce | `submissions.system` | acks=all, keyed by user_id | proto `SubmissionEvent` (phase=system) — Phase 1→2 promotion |
| Produce | `submissions.dlq` | acks=all, keyed by submission_id | JSON envelope `{poisoned_at, last_error, submission_proto_b64}` — for attempts-cap exceeded |

Schema in [`../tech-spec.md#5-wire-formats-and-data-models`](../tech-spec.md#5-wire-formats-and-data-models).

`max.poll.records=1` per consumer thread — one submission in flight per thread. When a worker is busy executing, it stops polling — backpressure is automatic.

### 2.2 Outbound HTTP

**To problem-service.** `ProblemServiceClient.fetchTestCases(problemId, pretestOnly)` issues `GET /api/v1/problems/{id}/test-cases?pretestOnly={bool}` and parses the JSON response into a `TestCaseBundle` with `time_limit_ms`, `memory_limit_mb`, and a list of `(ordinal, input_url, expected_output_url)`. 10 s request timeout; 5 s connect timeout. URL configurable via `APP_PROBLEM_SERVICE_URL` env (default `http://oj-control-plane:8089` — DNS doesn't cross VMs in compose, so production overrides this to the IP).

**To sandbox-manager.** `SandboxManagerClient.lease(language, submissionId, timeLimitMs, memoryLimitMib)` POSTs to `/lease`. On 503 with `error: "pool_exhausted"`, parses `retry_after_ms` and throws `PoolExhaustedException` carrying that delay. `release(sandboxId)` POSTs to `/release`. URL via `APP_SANDBOX_MANAGER_URL` (default `http://oj-sandbox-manager:9100` — intra-compose-network DNS works).

**To GCS.** `TestCaseFetcher.httpGet(signedUrl)` uses JDK `HttpClient.send(...)` for each input + expected URL. 15 s timeout. No auth — the signed URLs carry their own.

**To the in-guest agent (via Go bridge).** `AgentClient.exec(vsockUdsPath, port, request)` shells out to `/usr/local/bin/oj-vsock-client` with the JSON request on stdin. Reads the response from stdout. Process wall-clock cap is `(time_limit_ms + 2_000) ms` to leave headroom for compile + cold-start.

### 2.3 Outbound database

CRDB on `onlinejudge` (post-§2.2 schema unification). The worker writes only `idempotency_keys`. Reads only `idempotency_keys`. No other tables — Spring Data JPA is configured but only this one entity is mapped.

### 2.4 No inbound listening surface

The worker is a pure Kafka consumer. No REST API except `/actuator/health` on `:8081` (for compose healthcheck — note port number deliberately not 8080 to keep the internal port distinct from JVM service convention).

---

## 3. Internal design

### 3.1 Consumer dispatch

Two `@KafkaListener` methods in `SubmissionConsumer`:

```java
@KafkaListener(topics="${app.kafka.topic.pretest}", groupId="execution-worker-pretest", concurrency="4")
public void consumePretest(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
  processSubmission(record, ack, PHASE_PRETEST);
}

@KafkaListener(topics="${app.kafka.topic.system}", groupId="execution-worker-system", concurrency="2")
public void consumeSystem(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
  processSubmission(record, ack, PHASE_SYSTEM);
}
```

Both delegate to `processSubmission(record, ack, phase)`. The single `processSubmission` method drives the entire pipeline; it's deliberately monolithic so the control flow is readable top-to-bottom rather than scattered across handler classes.

### 3.2 Submission processing pipeline

The full sequence diagram is in [`../tech-spec.md#a-submission-round-trip-sequence-diagram`](../tech-spec.md#a-submission-round-trip-sequence-diagram). Inside the worker, the steps are:

1. **Parse the SubmissionEvent.** `parseSubmissionEvent(record.value())` tries proto first, falls back to a JSON envelope detection for the CRDB-changefeed path. Both paths produce a `SubmissionEvent` object.
2. **Resolve source code.** `resolveSourceCode(s3_code_url)` — `data:` URLs decoded inline; `gs://` URLs fetched via `GcsClient` (Workstream B). `s3://`, `r2://`, `http(s)://` schemes throw `UnsupportedOperationException` — TODO.
3. **Fetch test cases.** `testCaseFetcher.fetch(problemId, phase)` — calls problem-service, downloads bytes, returns a `ProblemSpec` with `time_limit_ms`, `memory_limit_mib`, and a `List<TestCaseSpec>` where each spec carries `ordinal`, `input` (UTF-8 string), and `expectedHash` (canonical SHA-256 hex).
4. **Lease a sandbox.** `executionService.execute(submissionId, language, code, testCases, timeLimitMs, memoryLimitMib)` calls the SM `/lease`, dispatches to the agent, calls `/release`, returns an `ExecutionResult` with `status`, `output`, `executionTimeMs`, `memoryUsedMb`, and a `List<PerTestResult>`.
5. **Claim idempotency** (deferred until AFTER lease — see §3.3).
6. **Determine the overall verdict.** `determineVerdict(result, timeLimitMs)` maps the agent's `status` to the canonical verdict set; `OK + time_ms > limit → TIME_LIMIT_EXCEEDED`; `OK + time_ms ≤ limit → ACCEPTED`; any other status passes through.
7. **Publish VerdictEvent.** Build the proto with `per_test` populated from `result.perTest()`, send to `evaluated_results` keyed by `userId`, wait for the broker ack via `kafkaTemplate.send(...).get(10, SECONDS)`.
8. **Publish AnalyticsEvent.** Fire-and-forget to `analytics`.
9. **Phase 1 → Phase 2 promotion.** If `phase == PRETEST && verdict == ACCEPTED`, re-serialize the original `SubmissionEvent` with `phase=system` and send to `submissions.system`.
10. **Mark idempotency completed.** `idempotencyService.markCompleted(submissionId, phase, leaseStartedAt)`.
11. **Ack the Kafka offset.** `ack.acknowledge()`.

Each step can fail; the catch block in `processSubmission` handles the matrix below.

### 3.3 Idempotency: claim AFTER lease

The naive design claims `idempotency_keys` BEFORE leasing — that way duplicate Kafka deliveries trip on the IN_PROGRESS branch and don't try to lease twice. We tried that and ran into the §3.6 pool-exhausted bug: a `503 pool_exhausted` burned an idempotency attempt even though the contestant did nothing wrong. After 5 attempts the row went poison and the submission DLQ'd.

The current design claims AFTER successful lease:

```java
// 1. Lease (may throw PoolExhaustedException → caught, nack, no idempotency state mutated)
ExecutionResult result = executionService.execute(...);

// 2. Claim — at this point we KNOW we got a sandbox, so the attempt is "real"
IdempotencyService.ClaimDecision claim = idempotencyService.claimSubmission(submissionId, phase);
if (claim.status() == ClaimStatus.COMPLETED) { ack.acknowledge(); return; }
if (claim.status() == ClaimStatus.IN_PROGRESS) { ack.nack(Duration.ofSeconds(5)); return; }
if (claim.status() == ClaimStatus.POISON) {
  // attempts-cap exceeded; dead-letter and ack
  kafkaTemplate.send(dlqTopic, submissionId, dlqEnvelope);
  ack.acknowledge();
  return;
}
// claim.status() == CLAIMED: proceed
```

Caveat: if two workers race to lease for the same submissionId, BOTH acquire sandboxes and one ends up wasted. The pool absorbs this (release returns the slot). In practice the race window is small — Kafka redelivery has at least seconds of latency, so we very rarely see double-lease.

The full reclaim+attempts+DLQ semantics are in [`../tech-spec.md#83-idempotency-keys--dlq-worker`](../tech-spec.md#83-idempotency-keys--dlq-worker).

### 3.4 Pool-exhausted handling

`SandboxManagerClient` parses the `/lease` 503 body and throws:

```java
public class PoolExhaustedException extends RuntimeException {
  public final String language;
  public final long retryAfterMs;
  // ...
}
```

`processSubmission` catches this BEFORE the `idempotencyService.claimSubmission` call, calls `ack.nack(Duration.ofMillis(retryAfterMs))`, and returns. The idempotency state is untouched.

```java
} catch (PoolExhaustedException ex) {
  log.info("[worker:{}] Pool exhausted for submission={} language={}; nacking with retry_after={}ms",
           phase, submissionId, ex.language, ex.retryAfterMs);
  ack.nack(Duration.ofMillis(ex.retryAfterMs));
  return;
}
```

The result: a contestant during a burst never sees `RUNTIME_ERROR` for what is purely a server-side backpressure event. The verdict pipeline is correct under load; the only visible effect is latency.

### 3.5 ExecutionBackend abstraction

Two implementations:

| Class | When active | What it does |
|---|---|---|
| `FirecrackerExecutionService` | `app.sandbox.backend=firecracker` (production) | Calls SM lease, dispatches via Go vsock bridge to the in-guest agent, calls SM release |
| `DockerExecutionService` | `app.sandbox.backend=docker` (dev fallback) | `docker run --rm ...` with the contestant code mounted; one test case per invocation (no in-process pool). Used for the macOS dev loop where Firecracker isn't available. |

Both implement `ExecutionBackend`:

```java
public interface ExecutionBackend {
  ExecutionResult execute(String submissionId, String language, String code,
                          List<TestCaseSpec> testCases,
                          int timeLimitMs, int memoryLimitMb);

  record ExecutionResult(String status, String output,
                         int executionTimeMs, int memoryUsedMb,
                         List<PerTestResult> perTest) {
    /** Back-compat ctor for legacy tests pre-§2.4. */
    public ExecutionResult(String status, String output, int t, int m) {
      this(status, output, t, m, List.of());
    }
  }
}
```

Spring picks one based on the configured backend. The interface deliberately accepts `timeLimitMs` and `memoryLimitMib` — roadmap §2.3 propagation lives here. Pass 0 to mean "use the backend's application.yml default" (the smoke/bypass path).

### 3.6 AgentClient + the vsock bridge

The JVM has no native AF_VSOCK. We could write a JNI layer or pull in `mdlayher/vsock`, but both require a libc dep in the worker container. Instead the worker shells out to `oj-vsock-client`, a tiny Go binary (~250 lines, statically linked, no libc) baked into the worker image.

`AgentClient.exec(vsockUdsPath, port, request)`:

1. Build the JSON request object — `session_token`, `language`, `code`, `time_limit_ms`, `per_test[]`.
2. Marshal via Jackson.
3. `ProcessBuilder("oj-vsock-client", vsockUdsPath, String.valueOf(port))`. Write the JSON bytes to the child's stdin, close stdin.
4. Read all of the child's stdout. Wait for exit with a wall-clock cap of `(time_limit_ms * cases + 2_000) ms`.
5. Parse the JSON response into `PerTestResult`s.

The half-close discipline is critical and documented in [`./sandbox-manager.md#38-vsock-bridge-oj-vsock-client`](./sandbox-manager.md#38-vsock-bridge-oj-vsock-client). Do NOT add a half-close after writing the request; Firecracker's vsock layer doesn't preserve AF_UNIX semantics.

### 3.7 Reconciliation safety net

The reconciliation scanner lives in api-gateway, not here — see [`./api-gateway.md#33-reconciliation-scanner`](./api-gateway.md#33-reconciliation-scanner). The worker doesn't reconcile its own work. If a worker crashes mid-execution, the idempotency row stays `processing` for 300 s, then is reclaimed by the next consumer (with `attempts++`). After 10 reclaim cycles the row goes POISON and is DLQ'd. The worker never re-publishes its own work.

---

## 4. Data ownership

| Resource | Lifetime | Where |
|---|---|---|
| `idempotency_keys` rows | per-(submission, phase) | CRDB `onlinejudge.idempotency_keys` |
| Kafka consumer offsets | per consumer-group | Kafka `__consumer_offsets` |
| In-flight submission state | per-consumer-thread JVM scope | JVM heap (local vars in `processSubmission`) |
| Verdict, analytics events | once-published | Kafka topics `evaluated_results`, `analytics` |
| Promotion events | once-published | Kafka topic `submissions.system` |
| DLQ envelopes | once-published | Kafka topic `submissions.dlq` |

The worker is **otherwise stateless across restarts**. A redeployed worker just resumes consumption from its committed offsets; any in-flight submission whose worker died re-arrives via Kafka redelivery + the idempotency reclaim path.

The worker does NOT own:
- `submissions` rows (api-gateway owns them via the outbox)
- `users` rows (api-gateway, via auth)
- problem-service tables (problems / test_cases)
- contest tables (contest-service)
- Redis (leaderboard-service writes; the worker doesn't touch Redis)
- GCS (problem-service signs; the worker is a GET-only consumer)

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| Kafka redelivery, submission already in-flight elsewhere | `claimSubmission` returns IN_PROGRESS | `ack.nack(5s)`. Idempotency row untouched. Next redelivery in 5 s. |
| Kafka redelivery, submission already completed | `claimSubmission` returns COMPLETED | `ack.acknowledge()`. No re-execution. |
| Idempotency attempts-cap exceeded | `claimSubmission` returns POISON | DLQ envelope sent to `submissions.dlq`, ack the offset. Operator inspects manually. |
| problem-service unreachable | `testCaseFetcher.fetch()` throws `IOException` | `ack.nack(5s)`. The idempotency claim wasn't yet attempted (it happens AFTER lease, but lease happens AFTER fetch — actually, fetch comes BEFORE lease in the current pipeline). Wait — see §3.2 step order. Re-check the implementation: fetch is step 3, lease step 4. A fetch failure happens with no idempotency state, no sandbox, just a Kafka nack. |
| Pool exhausted (503 from SM) | `executionService.execute()` throws `PoolExhaustedException` | `ack.nack(retry_after_ms)`. Idempotency untouched. Sandbox released back automatically. |
| Sandbox lease succeeded but agent dies mid-execution | The Go bridge process exits non-zero; `AgentClient.exec` throws | The worker emits `INTERNAL_ERROR` (mapped to `RUNTIME_ERROR` in the verdict; debatable — see [tech-spec §14](../tech-spec.md#14-known-limitations-and-debt)). Idempotency `markCompleted` runs because we got SOME terminal verdict. Kafka offset committed. |
| Sandbox watchdog fires (TLE) | Agent's response carries `verdict=TIME_LIMIT_EXCEEDED` for the offending ordinal OR the vsock connection drops mid-response | Either way, mapped to `TIME_LIMIT_EXCEEDED` in the verdict. Normal completion. |
| Kafka broker down during VerdictEvent publish | `kafkaTemplate.send(...).get(10s)` times out | The try block throws to the catch; `ack.nack(5s)` so Kafka redelivers. Idempotency stays `processing`; next consumer either re-leases (if stale enough) or sees IN_PROGRESS. |
| `markCompleted` returns false (claim lost) | `markCompleted` returns false when `created_at` doesn't match the original lease | Throws `IllegalStateException`. The catch path nacks. Another consumer holds the claim; this redelivery is effectively a duplicate that should resolve as COMPLETED on the next pass. |
| Worker container OOM-killed mid-execution | Kubernetes-style; SIGKILL. Sandbox is left LEASED. | The SM watchdog fires after `app.lease.wall-seconds` (default 30 s), kills + cleans up the sandbox. The idempotency row stays `processing` for 300 s, then is reclaimed by the next consumer (attempts++). |
| Two consumers race to lease for same submissionId | Kafka delivers the same record to multiple consumers (e.g. rebalance after a stale heartbeat) | Both acquire sandboxes, both attempt the claim. The loser sees IN_PROGRESS, nacks, releases its sandbox. Wasted lease — small. |

---

## 6. Configuration reference

`execution-worker/src/main/resources/application.yml`; env override via Spring relaxed binding. Defaults shown.

| Property | Default | Purpose |
|---|---|---|
| `app.kafka.topic.pretest` | `submissions.pretest` | Phase 1 consume topic. |
| `app.kafka.topic.system` | `submissions.system` | Phase 2 consume topic + Phase 1→2 promotion produce topic. |
| `app.kafka.topic.evaluated-results` | `evaluated_results` | VerdictEvent destination. |
| `app.kafka.topic.analytics` | `analytics` | AnalyticsEvent destination. |
| `app.kafka.topic.dlq` | `submissions.dlq` | Poisoned-submission destination. |
| `app.problem-service.url` | `http://oj-control-plane:8089` | problem-service base URL. Override via `APP_PROBLEM_SERVICE_URL` env on GCP. |
| `app.problem-service.required` | `true` | When `false`, the worker bypasses problem-service entirely and uses a single empty TestCaseSpec — the "smoke-test before problem-service is live" path. |
| `app.sandbox.manager.url` | `http://oj-sandbox-manager:9100` | SM REST API URL. |
| `app.sandbox.backend` | `firecracker` | `firecracker` or `docker`. |
| `app.sandbox.docker.runtime` | `runc` | When backend=docker, the OCI runtime. `runsc` enables gVisor. |
| `app.sandbox.linux-hardening.enabled` | `false` | When backend=docker, applies Seccomp-BPF + capability drop + cgroupns. Linux-only. |
| `app.sandbox.seccomp-profile` | `/etc/seccomp/sandbox-seccomp.json` | Seccomp profile path for docker backend. |
| `app.sandbox.in-process-pool.enabled` | `false` | Gates the LEGACY in-process FC pool. Off in production (the worker delegates to the separate SM). |
| `app.execution.timeout-seconds` | `5` | Fallback wall-clock when `time_limit_ms` is 0 from problem-service. |
| `app.execution.memory-limit-mb` | `256` | Fallback memory cap when `memory_limit_mib` is 0. |
| `app.idempotency.processing-lease-seconds` | `300` | Stale-row reclaim threshold. |
| `app.idempotency.max-attempts` | `5` | Reclaim attempts before POISON. |
| `app.agent.vsock-client` | `/usr/local/bin/oj-vsock-client` | Path to the Go bridge binary. |
| `app.region` | `${REGION:-asia-south1}` | Stamped on every emitted event. |

---

## 7. Metrics emitted

The `WorkerMetrics` bean wires these via the OpenTelemetry API. Names prefixed `oj.worker.*`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.worker.verdicts.published_total` | counter | `verdict`, `language`, `phase` | One per verdict published. Verdict values: ACCEPTED / WRONG_ANSWER / TIME_LIMIT_EXCEEDED / RUNTIME_ERROR / COMPILE_ERROR / MEMORY_LIMIT_EXCEEDED / INTERNAL_ERROR. |
| `oj.worker.submission.duration_seconds` | histogram | `verdict`, `language` | End-to-end submission processing: parse → fetch → lease → exec → publish. Bucketed [0.1, 0.5, 1, 2, 5, 10, 30] s. |
| `oj.worker.gcs.fetch.latency_seconds` | histogram | `bucket_label` | Per-object GCS fetch latency (`tests` for problem-service signed URLs, `source` for `gs://` source code). |
| `oj.worker.lease.latency_seconds` | histogram | `language` | Wall-clock for the `/lease` round-trip. Includes a `pool_exhausted` retry budget if any. |
| `oj.worker.pool_exhausted_total` | counter | `language` | Each 503 from SM. Sustained > 0 = pool sizing problem. |
| `oj.worker.idempotency.attempts_max` | gauge | (none) | Max `attempts` value seen in the last sweep. Above 3 = poison territory. |
| `oj.worker.dlq.sent_total` | counter | `reason` | Each DLQ publish. Reason: `poison`, `unparseable_event`. |
| `oj.worker.phase_promotion_total` | counter | (none) | Each Phase 1 ACCEPTED → Phase 2 re-publish. Should equal the ACCEPTED count on `submissions.pretest`. |

The planned **submission funnel** dashboard uses `oj.worker.submission.duration_seconds` as its primary signal; see [`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md).

---

## 8. Runbook

### 8.1 "Worker has no committed offsets" (consumer-group lag CURRENT-OFFSET=-)

**Symptom.** `kafka-consumer-groups --describe --group execution-worker-pretest` shows `CURRENT-OFFSET = -` for every partition.

**Diagnose.**
```sh
sudo docker logs oj-execution-worker --tail 200 | grep -E "ERROR|Caused by"
```

**Likely cause & fix.** The worker is crash-looping on bean wiring or schema validation. Real production examples:
- Two `@Component` beans with multiple ctors and no `@Autowired` → "No default constructor found" at boot. Fix: annotate the production ctor with `@Autowired`. Pattern documented in `TestCaseFetcher`, `AgentClient`, `JwtTokenProvider`.
- CRDB column type vs JPA entity mismatch (CRDB INT = BIGINT). Fix: widen entity field to `long`. Pattern documented in `Problem`, `IdempotencyKey`.

### 8.2 "Submissions piling up on submissions.pretest" (lag > 100)

**Symptom.** `kafka-consumer-groups` shows growing `LAG`. Worker is alive but not keeping up.

**Diagnose.**
```sh
sudo docker logs oj-execution-worker --tail 100 | grep -E "Pool exhausted|Verdict submission" | tail -30
```

**Likely cause & fix.**
- *Pool exhausted persistently.* See [`./sandbox-manager.md#81-pool-empty-for--30-s`](./sandbox-manager.md#81-pool-empty-for--30-s) for SM-side diagnostics. Tactical fix: bump `app.pool.targets.<language>` on SM, recreate.
- *problem-service slow.* Look at `oj.worker.gcs.fetch.latency_seconds` — if `tests` p99 > 5 s, problem-service or GCS is throttling. Less common on a single-tenant workload.
- *Worker thread starvation.* If you've increased concurrency beyond 4 and the JVM's heap is tight, GC pauses dominate. Bump heap.

### 8.3 "Every submission gets WRONG_ANSWER, even trivial ones"

**Symptom.** Reference solutions for known problems return WRONG_ANSWER. Per-test inspection shows hash mismatch on every ordinal.

**Diagnose.**
```sh
# Spot-check the canonical hash for "4\n":
python3 -c "import hashlib; print(hashlib.sha256(b'4').hexdigest())"
# Expected: 4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a

# Compare against what problem-service signed + GCS returned:
gcloud storage cat gs://oj-test-cases-online-judge-hk/<problem>/<ordinal>/expected.txt | xxd | head -3
```

**Likely cause & fix.**
- *Expected output has trailing whitespace mismatched with stripping.* `canonicalHash` does `stripTrailing` on UTF-8. If the GCS object has `4\r\n` but the agent's stdout strips to `4`, hashes match — that's fine. If the agent stops stripping, regression. Check `infra/firecracker/agent/cmd/agent/main.go` for the `strings.TrimRight` call.
- *Expected output charset drift.* Problem authors uploaded a UTF-16 BOM file. Re-upload as UTF-8.

### 8.4 "All submissions stuck on `processing` in idempotency_keys"

**Symptom.** `SELECT count(*) FROM idempotency_keys WHERE status='processing'` is large and growing. No verdicts on `evaluated_results`.

**Diagnose.** The worker is consuming submissions but never finishing them. Most often: it's leasing but the agent isn't responding, or the verdict publish is failing.

```sh
sudo docker logs oj-execution-worker --tail 200 | grep -E "LEASED submission|Verdict submission|Error processing"
```

**Likely cause & fix.**
- *Agent's vsock-client failing to connect.* Validate the worker can talk to SM:
  ```sh
  sudo docker exec oj-execution-worker sh -c 'ls -la /tmp/fc-*-vsock.sock'
  ```
  Empty = SM didn't expose UDS files. Check the SM container's `volumes: - /tmp:/tmp` bind.
- *Kafka publish timing out.* `kafkaTemplate.send(...).get(10, SECONDS)` is the gate. If broker is overloaded, this fails. Lower the worker concurrency.
- *Stuck rows from past crashes.* One-time cleanup:
  ```sql
  DELETE FROM idempotency_keys WHERE status='processing' AND created_at < now() - INTERVAL '1 hour';
  ```
  Then bounce the worker. Real production examples on `main` git log.

### 8.5 "DLQ topic filling up"

**Symptom.** `sudo docker exec oj-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic submissions.dlq --from-beginning --max-messages 10` shows poisoned envelopes.

**Decode + diagnose.** Each envelope is `{poisoned_at, last_error, submission_proto_b64}`. The proto bytes decode via the Python decoder in [`../tech-spec.md#c-wire-protocol-decoder-example`](../tech-spec.md#c-wire-protocol-decoder-example).

**Fix.** Inspect `last_error` for the failure class. Re-publish to `submissions.pretest` once the underlying issue is fixed:
```sh
# Pseudocode — there's no admin endpoint yet.
sudo docker exec oj-execution-worker java -cp /app/app.jar com.onlinejudge.worker.tools.DlqReplay <submissionId>
```

### 8.6 "Worker emits RUNTIME_ERROR for valid Python code"

**Symptom.** `print(2+2)` returns RUNTIME_ERROR. Looks like the in-guest agent can't find `python3`.

**Diagnose + fix.** PID-1 inherits no environment from the kernel. The `/init` script in the rootfs must export `PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`. Check `infra/firecracker/rootfs/init.sh`. If missing, bump `OJ_HARNESS_VERSION` and rebuild the rootfs on the next compute-VM boot.

---

## 9. Tests & verification

### 9.1 Unit tests (`execution-worker/src/test/java/`)

| File | Coverage |
|---|---|
| `SubmissionConsumerTest` | Proto parse + JSON-envelope fallback; idempotency-claim branches (CLAIMED / IN_PROGRESS / COMPLETED / POISON); pool-exhausted nack path |
| `TestCaseFetcherTest` | Happy path with mocked HttpClient; canonical hash stability; trailing whitespace handling; problem-service error → IOException |
| `IdempotencyServiceTest` | INSERT happy path; reclaim under cap; reclaim at cap → POISON; race-loser sees IN_PROGRESS; concurrent reclaim |
| `FirecrackerExecutionServiceTest` | Lease + agent + release happy path; PoolExhaustedException surfaces; agent error mapped to ExecutionResult |
| `DockerExecutionServiceTest` | Argv shape (with + without gVisor + with + without seccomp); TLE detection; per-test execution result |
| `AgentClientTest` | Request JSON shape; response parsing including `per_test`; Process timeout handling |
| `ProblemServiceClientTest` | URL composition; bundle shape parsing; non-200 → IOException |

Run via `./gradlew :execution-worker:test`. Full repo test (`./gradlew test`) includes the worker matrix.

### 9.2 Integration verification

**The `sum-of-two` smoke** at `infra/firecracker/test/problems/sum-of-two/`. Five test cases; reference solutions in Python / Java / C++ + a deliberately-wrong Python. Submit each via `submit-sample.py --expect-verdict ACCEPTED` (or WRONG_ANSWER for the buggy one). The full run was demonstrated live on GCP during the development session — see commit `bd3c7ff`.

**The `print(2+2)` smoke** (`problemId 00000000-0000-0000-0000-0000000cafee`) is the smaller fixture used for "does the pipeline run at all" checks.

### 9.3 Manual debugging

End-to-end trace of a submission via worker logs:
```sh
SID=<submission-uuid>
sudo docker logs oj-execution-worker --since 5m | grep "$SID"
# Expected lines, in order:
#   [worker:pretest] Received submission=...
#   [firecracker] LEASED submission=... sandbox=sb-... vsock=/tmp/...
#   [firecracker] submission=... overall=OK perTest=5
#   [worker:pretest] Verdict submission=... result=ACCEPTED time=66ms
#   [worker:pretest] Submission ... accepted; enqueued to submissions.system for Phase 2
```

If any line is missing, the diagnostics in §8 cover the corresponding failure modes.

---

## 10. Relevant design docs

- [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md) — affects this service's durability assumptions when the 3-broker migration lands (consumer-group rebalance behaviour, `acks=all` semantics under ISR=2).
- [`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md) — defines the metrics in §7 and the submission-funnel dashboard.
- [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md) — the SM-side lockdown affects what the agent inside the microVM CAN do (no DNS, no internet); the worker doesn't need to know this directly but it's relevant for understanding why certain agent failure modes exist.

The reconciliation scanner that complements this worker lives in api-gateway; see [`./api-gateway.md#33-reconciliation-scanner`](./api-gateway.md#33-reconciliation-scanner).

---

## 11. Code map

| Concern | File |
|---|---|
| Kafka listener + dispatch + verdict publish | `execution-worker/src/main/java/com/onlinejudge/worker/consumer/SubmissionConsumer.java` |
| Idempotency claim / reclaim / poison / DLQ | `.../service/IdempotencyService.java` |
| Idempotency JPA entity | `.../model/IdempotencyKey.java` |
| Execution backend interface + result type | `.../service/ExecutionBackend.java` |
| Firecracker backend (production path) | `.../service/FirecrackerExecutionService.java` |
| Docker backend (dev fallback) | `.../service/DockerExecutionService.java` |
| SM client (HTTP) | `.../service/SandboxManagerClient.java` |
| Pool-exhausted exception | `.../service/PoolExhaustedException.java` |
| Agent client (shells out to vsock-client) | `.../service/AgentClient.java` |
| Test-case fetcher (problem-service + GCS) | `.../service/TestCaseFetcher.java` |
| Problem-service HTTP client | `.../service/ProblemServiceClient.java` |
| GCS client (`gs://` source code fetch) | `.../service/GcsClient.java` |
| Metrics registration | `.../observability/WorkerMetrics.java` |
| Legacy in-process pool (gated OFF) | `.../sandbox/SandboxManager.java` |
| Spring main class | `.../ExecutionWorkerApplication.java` |
