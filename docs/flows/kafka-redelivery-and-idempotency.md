# Kafka Redelivery and the Idempotency State Machine

> Last reconciled with the repo on 2026-05-19.
>
> Why a submission is processed exactly once even though Kafka guarantees only at-least-once delivery — and how the four state branches at `IdempotencyService.claimSubmission` close every gap.

## 1. Why this flow exists

Kafka's delivery guarantee is at-least-once. Causes of redelivery in this system:
- Worker process crash after lease but before VerdictEvent publish.
- Worker host OOM-killed mid-execution.
- Consumer-group rebalance after a stale heartbeat.
- Manual offset reset by operator.
- Pool-exhausted nack with `retry_after_ms` (the benign case).

Without idempotency, each of these would publish a duplicate verdict, double-score the contestant, or worse. The system has to detect "this submission is already in flight elsewhere" and "this submission has already terminally completed" — the discrimination lives in the `idempotency_keys` table.

## 2. The four branches

`IdempotencyService.claimSubmission(submissionId, phase)` runs after a successful `/lease`. It performs a CAS-style insert/update and returns one of four `ClaimDecision.status()` values:

| Status | Meaning | Worker behaviour |
|---|---|---|
| `CLAIMED` | New row inserted, or stale row reclaimed under the attempts cap | proceed with execution |
| `IN_PROGRESS` | A row exists with `status='processing'` and `created_at` within the lease window | `ack.nack(5s)`; release leased sandbox; another worker is handling this |
| `COMPLETED` | A row exists with `status='completed'` | `ack.acknowledge()`; release sandbox; verdict was already published — this redelivery is a duplicate of a known-completed submission |
| `POISON` | A row exists with `attempts >= max_attempts` (default 5) | publish DLQ envelope to `submissions.dlq`; ack offset; release sandbox; operator inspects |

## 3. Step-by-step walkthrough

1. **Worker receives a Kafka record on `submissions.pretest`.** `max.poll.records=1` ensures one record per consumer thread.

2. **Worker pre-lease steps run** (source resolve, test-case fetch). These do not touch `idempotency_keys`; a fetch failure here causes a clean nack with no idempotency state mutation.

3. **Worker calls `/lease`.** On success: `{sandboxId, vsockUdsPath, port}`. On `pool_exhausted`: nack and return — no idempotency state. See [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md).

4. **Worker calls `IdempotencyService.claimSubmission(submissionId, phase)`.** Three possible rows the SELECT might find:
   - **No row** → INSERT `(submission_id, phase, status='processing', attempts=1, created_at=now())`. Return `CLAIMED`. Proceed.
   - **Row exists with `status='processing'`** → check `(now() - created_at) > app.idempotency.processing-lease-seconds`?
     - If `false` (still within someone's lease): return `IN_PROGRESS`. Worker nacks 5 s, releases sandbox.
     - If `true` (lease expired, original worker is presumed dead): increment `attempts`; check `attempts > max_attempts`?
       - If yes: UPDATE `status='poison'`. Return `POISON`. Worker DLQs.
       - If no: UPDATE `created_at=now()`, leave `status='processing'`. Return `CLAIMED`. Proceed.
   - **Row exists with `status='completed'`** → return `COMPLETED`. Worker acks.

5. **If CLAIMED, worker proceeds.** Dispatch to in-guest agent; collect per-test results; compute overall verdict; release sandbox.

6. **Worker publishes VerdictEvent to `evaluated_results`.** `kafkaTemplate.send(...).get(10, SECONDS)` — synchronous wait for `acks=all`. On exception, throws to the catch block — worker nacks. Idempotency row stays `processing`; next consumer either re-leases or sees `IN_PROGRESS`.

7. **Worker marks idempotency `completed`.** `markCompleted(submissionId, phase, leaseStartedAt)` does CAS on `created_at` — protects against a redelivery whose claim was reclaimed under us.

8. **Worker acks the offset.** Only after the verdict was confirmed on the broker AND `markCompleted` succeeded.

## 4. Why "claim AFTER lease, not before"

The pre-`/lease` design treated every redelivery as a fresh attempt — under `pool_exhausted` storms, the attempts counter ticked once per nack even though the worker never had a sandbox. Five fast nacks → POISON → DLQ → contestant's perfectly fine submission lost. See [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md) and [ADR-0003](../adr/0003-idempotency-claim-after-lease.md).

The current rule: an attempt only counts when the worker actually held a sandbox lease. Capacity events are invisible to the attempts counter.

## 5. Failure modes

| Scenario | What happens | What protects correctness |
|---|---|---|
| Worker crashes between `/lease` and `claimSubmission` | Sandbox is leased but no idempotency row exists | SM watchdog kills the unowned VM after `lease.wall-seconds`; Kafka redelivers; second worker leases a fresh VM and claims |
| Worker crashes between `claimSubmission` and dispatch | Row is `processing`, sandbox is leased, work hasn't started | Watchdog kills sandbox; redelivery; next worker sees `IN_PROGRESS`; nacks; eventually `processing-lease-seconds` expires; next worker reclaims under `attempts++` |
| Worker crashes between VerdictEvent publish and `markCompleted` | Verdict is on Kafka; idempotency stays `processing` | Eventually reclaimed; second worker re-runs the same submission; second VerdictEvent published; leaderboard ZADD is idempotent so the duplicate has no effect — but it does double-count the analytics event. Acceptable. |
| Worker dies cleanly at offset commit but before ack | Kafka has uncommitted offset, next consumer re-receives | second consumer sees `COMPLETED`, acks |
| Two consumers race to lease the same record (post-rebalance) | Both acquire sandboxes | One claims first → `CLAIMED`; other sees `IN_PROGRESS` → nacks, releases sandbox. Wasted lease — small. |
| Attempt cap hit on a valid submission (server-side bug) | `POISON` → DLQ | Operator inspects DLQ; if the bug is fixed, re-publishes; row state is reset manually via SQL |
| Idempotency table corrupted | DB integrity violation | api-gateway alert; emergency: SQL TRUNCATE and accept double-verdicts for the next minute |

## 6. Operator queries

```sql
-- find rows stuck > 10 min
SELECT submission_id, phase, status, attempts, created_at
FROM idempotency_keys
WHERE status='processing' AND created_at < now() - INTERVAL '10 minutes';

-- reclaim manually (rare)
DELETE FROM idempotency_keys
WHERE status='processing' AND created_at < now() - INTERVAL '1 hour';
```

## 7. Related material

- The idempotency implementation: [`services/execution-worker.md`](../services/execution-worker.md) §3.3.
- The "claim after lease" ADR: [`adr/0003-idempotency-claim-after-lease.md`](../adr/0003-idempotency-claim-after-lease.md).
- The reconciliation scanner that complements this from the api-gateway side: [`reconciliation-scanner.md`](./reconciliation-scanner.md).
- Cross-cutting reliability: [`tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms).
