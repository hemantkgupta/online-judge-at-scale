-- Roadmap §2.5 — bound the idempotency reclaim loop.
--
-- Before this migration, IdempotencyService.claimExisting reset created_at
-- on every reclaim. A row whose worker crashed mid-flight (or whose
-- downstream call deterministically failed) could cycle through
-- processing -> stale -> reclaim -> processing forever, eating Kafka
-- redelivery budget but never producing a verdict.
--
-- We add an attempts counter the service increments on each reclaim.
-- When attempts > app.idempotency.max-attempts (default 5), the worker
-- treats the next claim as POISON, publishes the original submission
-- bytes to submissions.dlq, and acks the Kafka record.
--
-- Backfill: existing rows get attempts=1 (their initial INSERT counted).
-- DEFAULT 0 on the column keeps any out-of-band INSERTs safe, but the
-- INSERT path in the service explicitly stamps attempts=1 going forward.

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;

UPDATE idempotency_keys SET attempts = 1 WHERE attempts = 0;
