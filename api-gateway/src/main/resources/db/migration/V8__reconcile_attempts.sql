-- Roadmap §3.9 — Stuck-submission reconciliation scanner.
--
-- Failure mode this closes: api-gateway crashes BETWEEN the `submissions`
-- row INSERT and the `outbox_events` INSERT. Because the outbox publisher
-- only walks the outbox table, the submission has no driver to ever leave
-- PENDING — it is "stuck forever". The reconciliation scanner is the safety
-- net: every interval-seconds it scans for PENDING rows older than
-- stale-after-seconds and re-publishes a SubmissionEvent for each one.
--
--   * `reconcile_attempts` — counter (not history) so we can cap retries at
--     `app.reconciliation.max-attempts` and route the persistently-stuck
--     rows to the DLQ. A column is cheaper than a side table because we
--     never query the *history* of attempts, just the current count.
--   * Partial index — CRDB supports partial indexes; the sweep query is
--     `WHERE status='PENDING' AND created_at < :staleBefore AND
--     reconcile_attempts < :maxAttempts` and only PENDING rows are
--     interesting, so filtering on the index keeps it tight.
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS reconcile_attempts INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_submissions_stuck_pending
    ON submissions (status, created_at)
    WHERE status = 'PENDING';
