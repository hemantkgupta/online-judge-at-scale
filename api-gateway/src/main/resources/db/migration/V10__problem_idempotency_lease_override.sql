-- =============================================================================
-- V10 — per-problem idempotency lease override (tech-spec §14 L4)
-- =============================================================================
-- Adds a nullable `lease_seconds_override` column to the `problems` table so
-- operators can tune the idempotency processing-lease duration on a per-problem
-- basis. Until this migration the worker's `app.idempotency.processing-lease-
-- seconds` (default 300 s, tech-spec §8.3) was a single global; that's fine
-- for typical problems but pinches when a contest includes a heavyweight
-- problem whose Phase-2 runs legitimately exceed 5 minutes (e.g. a graph
-- problem with 100+ system tests at ~3 s each).
--
-- Semantics:
--   * NULL  (default)  → use the global lease — backwards-compatible.
--   * not NULL         → worker uses this value as the staleness window
--                        when reclaiming an in-flight idempotency_keys row
--                        for THIS problem's submissions.
--
-- Why on `problems` (GLOBAL locality) and not `idempotency_keys`
-- (REGIONAL BY ROW): the override is a property of the problem definition,
-- not of any one submission attempt. Reads stay on the same row the worker
-- already fetches for time_limit_ms / memory_limit_mb so the read path
-- doesn't grow an extra round-trip.
--
-- Bounds enforcement (CHECK constraint): keep the override inside a sane
-- band so a typo doesn't accidentally pin a poisoned row for hours. Range
-- is 30 s ≤ override ≤ 3600 s; outside that the column rejects the write.
-- =============================================================================

ALTER TABLE problems
    ADD COLUMN IF NOT EXISTS lease_seconds_override BIGINT NULL;

-- Range check — see the file header. CRDB supports `CHECK` constraints in
-- ALTER TABLE; nullable means the constraint only fires for non-null writes.
-- The constraint name is explicit so we can drop/rename it cleanly in a
-- follow-up migration without depending on auto-generated names.
ALTER TABLE problems
    ADD CONSTRAINT check_problems_lease_override_range
    CHECK (lease_seconds_override IS NULL
           OR (lease_seconds_override BETWEEN 30 AND 3600));
