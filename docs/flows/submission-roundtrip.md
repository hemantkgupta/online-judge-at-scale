# Submission Roundtrip — Browser to Verdict

> Last reconciled with the repo on 2026-05-19.
>
> The canonical happy-path flow: a contestant clicks Submit in the SPA and watches an ACCEPTED verdict appear on the leaderboard within ~1 second. This document traces every hop along the way and names the invariants each step preserves.

## 1. Why this flow exists

The submission round-trip is the system's defining workload — every architectural decision in this repo (outbox, idempotency, Kafka spine, microVM isolation, score-sharded ZSET, STOMP fan-out) exists to make this one path fast, fair, and exactly-once. Most other flows in `docs/flows/` are degenerate or failure-path variants of this one. Read this first, then everything else makes more sense.

## 2. Sequence

```mermaid
sequenceDiagram
    autonumber
    participant SPA
    participant GW as api-gateway
    participant CRDB as CockroachDB
    participant K as Kafka
    participant W as execution-worker
    participant PS as problem-service
    participant GCS
    participant SM as sandbox-manager
    participant FC as Firecracker microVM
    participant LB as leaderboard-service
    participant R as Redis

    SPA->>GW: POST /api/v1/submissions {problemId, language, code}
    GW->>CRDB: BEGIN; INSERT submissions; INSERT outbox; COMMIT
    GW-->>SPA: 202 Accepted {submissionId}
    Note over CRDB,K: CRDB changefeed → kafka
    CRDB->>K: SubmissionEvent on submissions.pretest
    K->>W: poll (max.poll.records=1)
    W->>PS: GET /api/v1/problems/{id}/test-cases?pretestOnly=true
    PS-->>W: {time_limit_ms, memory_limit_mb, [(ordinal, signed_url_in, signed_url_out)]}
    W->>GCS: GET signed_url_in (× N test cases)
    W->>GCS: GET signed_url_out (× N test cases)
    W->>SM: POST /lease {language, timeLimitMs, memoryLimitMib}
    SM-->>W: {sandboxId, vsockUdsPath, port}
    W->>CRDB: INSERT idempotency_keys (CLAIMED)
    W->>FC: vsock JSON {code, perTest[]}
    FC-->>W: vsock JSON {status, output, executionTimeMs, perTest[]}
    W->>SM: POST /release {sandboxId}
    W->>CRDB: UPDATE idempotency_keys SET status=COMPLETED
    W->>K: VerdictEvent on evaluated_results (acks=all)
    W->>K: AnalyticsEvent on analytics (fire-and-forget)
    K->>LB: poll evaluated_results
    LB->>R: ZADD leaderboard:{contestId}:s{idx} score user
    LB->>SPA: STOMP frame on /topic/leaderboard/{contestId}
```

## 3. Step-by-step walkthrough

The numbered steps below correspond to the autonumbered arrows in the diagram. For each step: what happens, where in code, and the invariant the step preserves.

1. **SPA submits the request.** `frontend/src/services/api.ts` (`submitSolution`) POSTs JSON to `POST /api/v1/submissions`. Bearer JWT in the `Authorization` header. The SPA does not wait for a verdict here — the 202 response is the only synchronous handshake.

2. **api-gateway opens a single CRDB transaction containing two inserts.** `api-gateway/src/main/java/com/onlinejudge/gateway/controller/SubmissionController.java` validates the payload, then calls into `SubmissionService.create(...)`. Inside one transaction: `INSERT INTO submissions (...) VALUES (...)` and `INSERT INTO outbox (aggregate_type='Submission', aggregate_id=submissionId, event_type='SubmissionCreated', payload=proto bytes)`. The transaction commits atomically — either both rows land or neither.
   *Invariant:* a row in `submissions` always has a matching outbox row to publish. See [ADR-0002](../adr/0002-transactional-outbox-over-sync-publish.md).

3. **api-gateway responds 202 to the SPA.** The response body contains the `submissionId` so the SPA can subscribe to its eventual STOMP frame. The verdict is async — the SPA shows a "Pending" pill and waits for the STOMP push.

4. **CRDB changefeed emits the outbox row as a Kafka record.** CRDB's native changefeed (configured at table creation via `CREATE CHANGEFEED FOR TABLE outbox INTO 'kafka://...'`) reads the WAL and publishes one record per outbox insert to `submissions.pretest`. Key = `user_id` (preserves per-user ordering); value = the proto-bytes payload field of the outbox row.
   *Invariant:* a successful CRDB COMMIT eventually produces exactly one Kafka record per outbox insert. Changefeed retries until the broker acks.

5. **execution-worker consumes the SubmissionEvent.** `execution-worker/src/main/java/com/onlinejudge/worker/consumer/SubmissionConsumer.java#consumePretest`. `@KafkaListener(topics="submissions.pretest", groupId="execution-worker-pretest", concurrency="4")`. `max.poll.records=1` per consumer thread — natural backpressure: a busy worker stops polling.

6. **Worker resolves the source code.** `resolveSourceCode(s3_code_url)` — `data:` URLs decoded inline; `gs://` URLs fetched via `GcsClient`; `s3://` / `r2://` / `http(s)://` schemes throw `UnsupportedOperationException` (TODO).

7. **Worker fetches the test-case bundle.** `TestCaseFetcher.fetch(problemId, phase)` calls `ProblemServiceClient.fetchTestCases(problemId, pretestOnly=true)` → `GET /api/v1/problems/{id}/test-cases?pretestOnly=true`. problem-service signs V4 download URLs (5-min TTL) against the `oj-test-cases-*` GCS bucket. The worker downloads inputs + expected outputs and canonical-hashes the expected outputs with `stripTrailing` + SHA-256.
   *Invariant:* the canonical hash applied to expected output must match what the in-guest agent computes against the contestant's stdout. See [`services/execution-worker.md#83`](../services/execution-worker.md) for the canonical-hash rule.

8. **Worker leases a sandbox.** `SandboxManagerClient.lease(language, submissionId, timeLimitMs, memoryLimitMib)` POSTs to `http://oj-sandbox-manager:9100/lease`. SM returns `{sandboxId, vsockUdsPath, port}` on 200, or `503 {error: "pool_exhausted", retry_after_ms: N}` if the warm pool is empty (see [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md)).
   *Invariant:* a successful lease guarantees the worker exclusive use of one Firecracker microVM until matching `/release` (or watchdog kill after `lease.wall-seconds`).

9. **Worker claims the idempotency row — AFTER lease, not before.** `IdempotencyService.claimSubmission(submissionId, phase)` returns one of `CLAIMED` / `IN_PROGRESS` / `COMPLETED` / `POISON`. The order — *claim after lease* — is the result of a production bug: `pool_exhausted` retries used to burn idempotency attempts, eventually poisoning a contestant's submission for what was purely a server-side capacity event. See [ADR-0003](../adr/0003-idempotency-claim-after-lease.md).
   *Invariant:* a `CLAIMED` row corresponds to a worker that holds a live sandbox lease.

10. **Worker dispatches the per-test request to the in-guest agent.** `AgentClient.exec(vsockUdsPath, port, request)` shells out to `/usr/local/bin/oj-vsock-client` (a tiny Go binary; see [ADR-0006](../adr/0006-vsock-go-bridge-not-jni.md)) with the JSON request on stdin. The agent inside the microVM compiles (if needed), runs each test case under the watchdog, and returns per-ordinal results: status, stdout hash, time_ms, memory_kb. One vsock round-trip carries the full test suite.

11. **Worker releases the sandbox.** `SandboxManagerClient.release(sandboxId)` POSTs to `/release`. SM destroys the microVM (not recycled — see `services/sandbox-manager.md#1-purpose`). A fresh VM will be spawned by the background warmer to replace it.

12. **Worker computes the overall verdict.** `determineVerdict(result, timeLimitMs)` maps the agent's `status` to the canonical verdict set: `OK + time ≤ limit → ACCEPTED`; `OK + time > limit → TIME_LIMIT_EXCEEDED`; anything else passes through (`WRONG_ANSWER`, `COMPILE_ERROR`, `RUNTIME_ERROR`, `MEMORY_LIMIT_EXCEEDED`, `INTERNAL_ERROR`).

13. **Worker publishes the VerdictEvent.** `kafkaTemplate.send("evaluated_results", userId, verdictBytes).get(10, SECONDS)` — synchronous wait for broker ack with `acks=all`. The proto carries the overall verdict plus the `per_test` breakdown for the SPA's expandable view.
   *Invariant:* exactly one VerdictEvent per `(submissionId, phase)` reaches Kafka.

14. **Worker marks idempotency completed.** `idempotencyService.markCompleted(submissionId, phase, leaseStartedAt)`. The CAS check on `created_at` rejects stale claims — protection against a redelivery whose original worker died and whose claim was reclaimed by another consumer.

15. **Worker acks the Kafka offset.** `ack.acknowledge()`. Only after the verdict was on the broker.

16. **leaderboard-service consumes the VerdictEvent.** `leaderboard-service/src/main/java/com/onlinejudge/leaderboard/consumer/VerdictConsumer.java`. `@KafkaListener(topics="evaluated_results", groupId="leaderboard")`. Re-resolves the contest and the user's current contest state.

17. **leaderboard-service updates the score-range-sharded ZSET.** `redisTemplate.opsForZSet().add("leaderboard:" + contestId + ":s" + shardIdx, userId, newScore)`. The sharding by score range avoids the hot-key collapse a single global ZSET per contest would create. See [ADR-0008](../adr/0008-score-range-sharded-leaderboard-zset.md). It also caches `verdict:{submissionId}` in Redis with a 24 h TTL so a reconnecting SPA can fetch the verdict via HTTP if the STOMP push was missed.

18. **leaderboard-service publishes a STOMP frame.** `simpMessagingTemplate.convertAndSend("/topic/leaderboard/" + contestId, leaderboardUpdate)`. The SPA's STOMP-over-SockJS subscription (`frontend/src/contexts/WebSocketContext.tsx`) receives the frame and updates the leaderboard panel + flips the submission pill from "Pending" to "ACCEPTED".

## 4. Failure modes at each step

| Step | Failure | Detection | Behaviour |
|---|---|---|---|
| 2 | CRDB transaction commit fails | exception in `SubmissionService.create` | 500 to SPA; nothing was published; SPA retries on user action |
| 4 | CRDB → Kafka publish lags (broker down) | changefeed retry metric; outbox row stays unpublished | reconciliation scanner detects and re-publishes — see [`reconciliation-scanner.md`](./reconciliation-scanner.md) |
| 5 | Worker crashes between poll and processing | Kafka rebalance; offset uncommitted | another worker re-receives; idempotency absorbs the duplicate |
| 7 | problem-service unreachable / 5xx | `IOException` from `TestCaseFetcher.fetch` | `ack.nack(5s)`; no idempotency state was created |
| 8 | Sandbox pool exhausted | 503 with `retry_after_ms` | `PoolExhaustedException` → `ack.nack(retry_after_ms)`; no idempotency claim — see [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md) |
| 9 | Idempotency claim sees `IN_PROGRESS` (duplicate delivery) | enum value | `ack.nack(5s)`; release sandbox; let original consumer finish |
| 9 | Idempotency claim sees `POISON` (attempts cap exceeded) | enum value | DLQ envelope to `submissions.dlq`; ack offset; operator inspects |
| 10 | In-guest agent hangs | `AgentClient` wall-clock cap = `(time_limit_ms + 2_000) ms` | process killed; mapped to `INTERNAL_ERROR` |
| 10 | vsock connection drops mid-response | `AgentClient` reads partial JSON | parse error → `INTERNAL_ERROR`; SM watchdog kills the VM after `lease.wall-seconds` |
| 13 | Kafka broker down during VerdictEvent publish | `.get(10, SECONDS)` throws | catch → `ack.nack(5s)`; idempotency stays `processing`; redelivery resolves |
| 16 | leaderboard-service crashes | Kafka rebalance | another instance re-consumes; ZADD is idempotent (same score → no-op) |
| 18 | SPA WebSocket disconnected | no STOMP frame received | SPA fetches `verdict:{submissionId}` from Redis on reconnect |

## 5. Related material

- Per-service detail: [`services/api-gateway.md`](../services/api-gateway.md) (steps 1–3), [`services/execution-worker.md`](../services/execution-worker.md) (steps 5–15), [`services/sandbox-manager.md`](../services/sandbox-manager.md) (steps 8, 10, 11), [`services/problem-service.md`](../services/problem-service.md) (step 7), [`services/leaderboard-service.md`](../services/leaderboard-service.md) (steps 16–18).
- Cross-cutting reliability story: [`tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms).
- Wire formats: [`tech-spec.md#5-wire-formats-and-data-models`](../tech-spec.md#5-wire-formats-and-data-models).
- The five ADRs that this flow depends on: [0001](../adr/0001-multi-service-event-driven.md), [0002](../adr/0002-transactional-outbox-over-sync-publish.md), [0003](../adr/0003-idempotency-claim-after-lease.md), [0006](../adr/0006-vsock-go-bridge-not-jni.md), [0008](../adr/0008-score-range-sharded-leaderboard-zset.md).
