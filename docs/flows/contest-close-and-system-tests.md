# Contest Close and System-Test Replay

> Last reconciled with the repo on 2026-05-19.
>
> What happens at contest close: every ACCEPTED-pretest submission gets a second evaluation against the full (hidden) test suite, scores are recomputed, and the leaderboard transitions to its final mode.

## 1. Why this flow exists

Pretest verdicts use a public subset of test cases — fast feedback during the contest. The final ranking has to be computed against the full hidden suite (system tests), but running all hidden cases during the contest is wasteful (it would burn capacity on submissions that fail on the first hidden case). The two-phase model:

- **Phase 1 (pretest)** runs during the contest. Live feedback to contestants. ACCEPTED-pretest submissions are eligible for system tests.
- **Phase 2 (system test)** runs after CLOSED. Replays each ACCEPTED-pretest submission against the hidden test cases. The verdict here is final.

The full design is in [`design-docs/contest-close-system-tests-replay.md`](../design-docs/contest-close-system-tests-replay.md).

## 2. The contest state machine

```
            create                      registration_open
NOTHING ────────────► CREATED ──────────────────────────► REGISTRATION
                                                                │
                                                                │ contest_start
                                                                ▼
                                                              ACTIVE ─┐
                                                                │     │ pretest submissions in flight
                                                                │     │
                                                                │ contest_end (manual or scheduled)
                                                                ▼
                                                              CLOSED
                                                                │
                                                                │ system-test replay scheduled
                                                                ▼
                                                       SYSTEM_TESTING
                                                                │
                                                                │ all Phase 2 verdicts in
                                                                ▼
                                                            FINALIZED
```

Transitions are driven by contest-service. The two interesting ones:
- **ACTIVE → CLOSED**: stops accepting new submissions; pretest evaluations in flight finish.
- **CLOSED → SYSTEM_TESTING**: kicks off the replay flow described below.

## 3. Replay sequence

```mermaid
sequenceDiagram
    autonumber
    participant CS as contest-service
    participant CRDB
    participant K as Kafka
    participant W as execution-worker
    participant SP as scoring-pipeline (Flink)
    participant LB as leaderboard-service
    CS->>CRDB: UPDATE contests SET status='CLOSED' WHERE id=C
    CS->>CRDB: SELECT submissions WHERE contest_id=C AND verdict='ACCEPTED' AND phase='pretest'
    Note over CS: Returns N submissions (Phase-1 ACCEPTED)
    loop For each submission, in batches of B
        CS->>K: publish SubmissionEvent(phase='system') on submissions.system
    end
    CS->>CRDB: UPDATE contests SET status='SYSTEM_TESTING'
    K->>W: poll submissions.system (groupId=execution-worker-system, concurrency=2)
    W->>W: full pipeline — lease, fetch full test-cases, execute
    W->>K: VerdictEvent (phase=system) on evaluated_results
    K->>LB: VerdictEvent
    LB->>LB: ignore for live leaderboard; record verdict for final tabulation
    K->>SP: VerdictEvent
    SP->>SP: keyed by (contest_id, user_id); recompute final score from Phase-2 verdicts
    SP->>K: FinalScoreEvent on contest.final_scores
    LB->>LB: consume FinalScoreEvent; emit STOMP "leaderboard finalized" to SPA
    LB->>CRDB: write final ranking to contest_results
    CS->>CRDB: UPDATE contests SET status='FINALIZED'
```

## 4. Step-by-step walkthrough

1. **Operator closes the contest.** Via UI or API: `PATCH /api/v1/contests/{id}/close`. contest-service validates the transition, writes `status='CLOSED'`, publishes `ContestClosedEvent` to `contest.events`.

2. **Replay query.** contest-service's `SystemTestReplayJob` (a scheduled @Async task) wakes up on the `ContestClosedEvent`. SQL:
   ```sql
   SELECT submission_id, user_id, problem_id, language, payload
   FROM submissions
   WHERE contest_id = ?
     AND status = 'EVALUATED'
     AND verdict = 'ACCEPTED'
     AND phase = 'pretest'
   ORDER BY user_id, problem_id, submitted_at DESC;
   ```
   Per `(user_id, problem_id)`, only the LATEST ACCEPTED-pretest submission is replayed. (Multiple ACCEPTEDs from the same user/problem are expected — only the final one counts.)

3. **Batched re-publish.** Each result row's payload is re-marshalled as a `SubmissionEvent` proto with `phase=system` and sent to `submissions.system`. Batched at `app.contest-close.batch-size` (default 100) with `app.contest-close.batch-interval-ms` (default 200 ms) between batches to avoid overwhelming the worker.

4. **contest-service transitions status to SYSTEM_TESTING.** This is the signal to leaderboard-service to start treating incoming verdicts as final, not live.

5. **execution-worker consumes Phase 2 events.** Different consumer group (`execution-worker-system`), concurrency 2 (slower than Phase 1's 4 — system tests don't need fast turnaround, and the lower concurrency leaves room for live submissions still finishing pretest).

6. **Worker runs the FULL test-case bundle.** `TestCaseFetcher.fetch(problemId, phase=SYSTEM)` calls problem-service with `pretestOnly=false`. The fetched bundle includes ALL test cases, not just the public subset.

7. **Worker publishes Phase 2 VerdictEvent.** Keyed by `user_id`; payload includes `phase=system`. Per-test breakdown reflects the full suite.

8. **leaderboard-service splits behaviour by phase.** `VerdictConsumer` checks `phase`:
   - `phase=pretest` → ZADD into the live leaderboard (only valid before SYSTEM_TESTING).
   - `phase=system` → ignore the live leaderboard; pass through to the final tabulation aggregator.

9. **scoring-pipeline (Flink) recomputes final scores.** Keyed by `(contest_id, user_id)`. Each Phase-2 verdict either confirms (ACCEPTED) or invalidates (anything else) the contestant's Phase-1 ACCEPTED. Final score = sum of (problem_score × penalty_factor) for surviving ACCEPTEDs. Penalty time = sum of (submission_time − contest_start) − for each problem's first ACCEPTED.

10. **Flink emits FinalScoreEvent.** When all Phase 2 verdicts for a `(contest_id, user_id)` are in, the scorer emits one `FinalScoreEvent` to `contest.final_scores`. Watermark + side output handles late verdicts.

11. **leaderboard-service writes the final ranking.** Consumes `contest.final_scores`; orders by score (desc) + penalty (asc); persists to `contest_results` table; emits one final STOMP frame to `/topic/contest/{id}/finalized`.

12. **contest-service transitions to FINALIZED.** When `contest_results` is complete (cardinality matches expected user count), contest-service flips the final status. The SPA's contest view switches to "final results" mode and disables submission/replay UI.

## 5. Failure modes

| Step | Failure | Behaviour |
|---|---|---|
| 2 | Replay query times out (large contest) | scheduled job retries; idempotent (re-publishing the same payload triggers IN_PROGRESS in the worker) |
| 3 | Kafka publish fails mid-batch | next batch tick picks up where this left off; deduplication handled by idempotency_keys for `(submission_id, phase=system)` |
| 5 | Worker concurrency too low → system-test phase takes hours | acceptable for v1; bump `app.kafka.concurrency.system` to 4 if needed; lower limit is for contestant-experience purity during overlapping contests |
| 6 | problem-service test-case bucket inaccessible | fetch fails; worker nacks; reconciliation scanner re-publishes |
| 9 | Flink job restarts mid-replay | ABS checkpoint to S3; restore + rewind Kafka; no double-counting |
| 10 | Late Phase-2 verdict arrives after FinalScoreEvent | side output → reconciliation job updates `contest_results` if material |
| 11 | leaderboard-service crashes mid-write | partial `contest_results`; on restart, idempotent re-write from `contest.final_scores` |

## 6. Operator queries

```sql
-- how many Phase 2 submissions are queued / in flight?
SELECT count(*) FROM idempotency_keys
WHERE phase = 'system' AND status = 'processing';

-- per-contest replay progress
SELECT
  count(*) FILTER (WHERE status='completed') AS done,
  count(*) AS total
FROM idempotency_keys
WHERE phase = 'system'
  AND submission_id IN (
    SELECT submission_id FROM submissions WHERE contest_id = ?
  );

-- final scores written?
SELECT count(*) FROM contest_results WHERE contest_id = ?;
```

## 7. Related material

- Full design doc: [`design-docs/contest-close-system-tests-replay.md`](../design-docs/contest-close-system-tests-replay.md).
- contest-service owner page: [`services/contest-service.md`](../services/contest-service.md).
- scoring-pipeline owner page: [`services/scoring-pipeline.md`](../services/scoring-pipeline.md).
- leaderboard-service owner page: [`services/leaderboard-service.md`](../services/leaderboard-service.md).
