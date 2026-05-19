# Reconciliation Scanner — Catching Stuck Submissions

> Last reconciled with the repo on 2026-05-19.
>
> The safety net that catches submissions whose path through the system stalled, and how it interacts with the worker's idempotency state machine to avoid producing duplicate verdicts during recovery.

## 1. Why this flow exists

The submission-roundtrip happy path depends on multiple atomic-looking transitions that aren't actually one transaction:
- INSERT submissions + INSERT outbox (one CRDB transaction) → Kafka publish (separate, by changefeed).
- Worker poll → `/lease` → idempotency claim → dispatch → VerdictEvent → markCompleted → offset commit.

If a process dies between any of those, a submission can sit in a stuck state. The reconciliation scanner is the eventual-consistency mechanism that detects these and re-drives them, without producing duplicates.

## 2. Two scanners, two responsibilities

The system has two scanners (named so because they sweep state on a cadence):

| Scanner | Owner | Watches | Reclaim rule |
|---|---|---|---|
| **Outbox relay** | api-gateway | `outbox` rows where `published_at IS NULL` and `created_at > 30s ago` | re-publish to Kafka; bump `published_at` on broker ack |
| **Stuck-submission scanner** | api-gateway | `submissions` rows where `status='SUBMITTED'` and `created_at > 5 min ago` and no row in `idempotency_keys` with `status IN ('completed','poison')` | re-publish a fresh SubmissionEvent to `submissions.pretest`; mark `submissions.last_reconciled_at=now()` |

In practice the CRDB changefeed is reliable enough that the outbox relay almost never fires — it's the belt for the changefeed's suspenders. The stuck-submission scanner is the more frequently-active one.

## 3. Sequence (stuck submission)

```mermaid
sequenceDiagram
    autonumber
    participant SCAN as stuck-scanner
    participant CRDB
    participant K as Kafka
    participant W as execution-worker
    SCAN->>CRDB: SELECT submissions WHERE status='SUBMITTED' AND created_at < now()-5min
    Note over SCAN: Found submissionId=S
    SCAN->>CRDB: SELECT idempotency_keys WHERE submission_id=S AND status IN ('completed','poison')
    Note over SCAN: No terminal row found → suspect stuck
    SCAN->>K: re-publish SubmissionEvent on submissions.pretest (key=user_id)
    SCAN->>CRDB: UPDATE submissions SET last_reconciled_at=now()
    K->>W: poll
    W->>W: pre-lease, lease, claimSubmission
    Note over W: Idempotency lookup finds existing row?
    alt existing row in 'processing' but past lease window
        W->>CRDB: reclaim under attempts++; status stays 'processing'
        W->>W: proceed with execution
    else existing row in 'completed'
        W-->>K: ack — duplicate, no re-execution
    else no row exists (truly stuck before claim)
        W->>CRDB: INSERT idempotency_keys (status='processing')
        W->>W: proceed with execution
    end
```

## 4. Step-by-step walkthrough

1. **Scanner tick.** `api-gateway/src/main/java/com/onlinejudge/gateway/reconciliation/StuckSubmissionScanner.java#scan` runs every `app.reconciliation.scan-interval-seconds` (default 30 s). Bounded scan: `LIMIT 100` per tick to prevent a backlog cascade.

2. **Candidate selection.** SQL roughly:
   ```sql
   SELECT s.submission_id, s.user_id, s.problem_id, s.language, s.payload
   FROM submissions s
   LEFT JOIN idempotency_keys i
     ON i.submission_id = s.submission_id
    AND i.status IN ('completed','poison')
   WHERE s.status = 'SUBMITTED'
     AND s.created_at < now() - INTERVAL '5 minutes'
     AND i.submission_id IS NULL
     AND (s.last_reconciled_at IS NULL OR s.last_reconciled_at < now() - INTERVAL '5 minutes')
   ORDER BY s.created_at ASC
   LIMIT 100;
   ```
   The 5-min freshness window leaves time for normal redelivery to resolve and avoids spamming Kafka.

3. **Re-publish.** For each candidate, build a fresh `SubmissionEvent` proto from the row's stored payload and send to `submissions.pretest` keyed by `user_id` (same partition assignment as the original). Wait for broker ack.

4. **Mark last_reconciled_at.** UPDATE `submissions SET last_reconciled_at=now()`. Prevents the same row from being re-published every 30 s if the worker continues to fail to terminate it (e.g., a permanent server-side bug). The next reconciliation attempt won't fire for another 5 min.

5. **Worker receives the re-published record.** Two cases:
   - **No existing idempotency row** (truly stuck pre-claim, e.g. worker died between `/lease` and claim): worker proceeds normally; claim creates a new row.
   - **Existing row in `processing` past lease window**: worker reclaims, increments `attempts`. If still under cap → proceed. If at cap → POISON → DLQ.
   - **Existing row `completed`**: worker acks the duplicate, no re-execution. The contest leaderboard already saw the verdict.

6. **Verdict eventually produced.** Identical to the normal roundtrip.

## 5. Failure modes

| Scenario | What happens | Protection |
|---|---|---|
| Scanner crashes mid-batch | next tick re-runs the same SELECT; same candidates re-discovered | idempotent; no harm |
| Scanner re-publishes a row whose worker JUST started processing | second worker receives, sees `IN_PROGRESS` in claim → nacks | claim state-machine absorbs |
| Scanner SQL slows under heavy load | `LIMIT 100` keeps the per-tick cost bounded | + DB index on `(status, created_at)` ensures the LEFT JOIN is fast |
| Same submission stuck for hours (genuine bug) | each 5-min cycle re-publishes; attempts cap eventually hits → POISON | DLQ collects; operator inspects |
| `submissions` table indexes missing | scan slows, alerts fire | covered by `db-runbook` migration check on deploy |

## 6. Operator queries

```sql
-- how many stuck right now?
SELECT count(*) FROM submissions
WHERE status='SUBMITTED' AND created_at < now() - INTERVAL '5 minutes';

-- stuck without an idempotency row?
SELECT s.submission_id, s.created_at, s.last_reconciled_at
FROM submissions s
LEFT JOIN idempotency_keys i ON i.submission_id = s.submission_id
WHERE s.status='SUBMITTED' AND i.submission_id IS NULL
ORDER BY s.created_at ASC LIMIT 20;

-- reconciliation tick health
SELECT count(*) AS reconciled_last_hour
FROM submissions
WHERE last_reconciled_at > now() - INTERVAL '1 hour';
```

## 7. Related material

- The worker's idempotency state machine: [`kafka-redelivery-and-idempotency.md`](./kafka-redelivery-and-idempotency.md).
- api-gateway scanner implementation: [`services/api-gateway.md`](../services/api-gateway.md) §3.3 (Reconciliation scanner).
- Outbox pattern: [`adr/0002-transactional-outbox-over-sync-publish.md`](../adr/0002-transactional-outbox-over-sync-publish.md).
- Cross-cutting reliability: [`tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms).
