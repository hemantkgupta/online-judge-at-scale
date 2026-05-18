-- =============================================================================
-- V9 — multi-region locality
-- =============================================================================
-- Adds `region` columns where they were missing (most write-path tables
-- already had it from V2), then sets per-table LOCALITY so the schema honours
-- the cluster's multi-region layout.
--
-- PRE-FLIGHT: this migration assumes the CRDB cluster has already had its
-- PRIMARY REGION declared and the secondary region added — i.e. the operator
-- ran `infra/scripts/crdb-multiregion-init.sh <primary> <secondary>` BEFORE
-- the first api-gateway boot. Without that, the `SET LOCALITY` DDL below
-- fails (CRDB rejects locality declarations on a single-region database).
-- Flyway will mark V9 failed; `flyway repair` + retry after running the
-- init script is the recovery path.
--
-- LOCALITY MODEL:
--   * GLOBAL — replicated to every region. Reads are local (follower-reads);
--     writes pay cross-region latency. Right for tables with low write rate
--     and read-everywhere access patterns.
--       problems, test_cases, users, contests, contest_problems
--   * REGIONAL BY ROW — each row is pinned to the region indicated by CRDB's
--     hidden `crdb_region` column. CRDB adds that column automatically when
--     SET LOCALITY REGIONAL BY ROW runs; its default is the inserting node's
--     region. Reads + writes both stay in-region.
--       submissions, outbox_events, idempotency_keys, refresh_tokens,
--       auth_events
--
-- WHY KEEP THE APP-SIDE `region` STRING COLUMN ALONGSIDE CRDB's `crdb_region`:
-- Option A from the design discussion. The `region STRING` column is what the
-- app reads/writes (it's also what V2's idx_outbox_region_unpublished partial
-- index already depends on). CRDB's hidden `crdb_region` column drives row
-- placement and defaults to the inserting node's region — usually aligned
-- with what the gateway stamps on `region`. Drift is possible if rows are
-- inserted with an explicit `region` value that doesn't match the locality
-- of the inserting node; the hidden column wins for placement, the STRING
-- column wins for app-side filtering.
--
-- DEFERRED:
--   * `SURVIVE REGION FAILURE` requires 3+ regions; the 2-region cluster
--     stays at the default SURVIVE ZONE FAILURE. Operator follow-up when
--     scaling to a third region.
--   * `users` LOCALITY is GLOBAL today. The multi-region design doc proposes
--     REGIONAL BY ROW keyed on a `home_region` column — that conflates "add
--     column" with "change locality semantics" so it's deferred to a follow-
--     up migration.
-- =============================================================================

-- ── 1. Add region columns where missing ───────────────────────────────────

ALTER TABLE users           ADD COLUMN IF NOT EXISTS region STRING NOT NULL DEFAULT 'asia-south1';
ALTER TABLE contests        ADD COLUMN IF NOT EXISTS region STRING NOT NULL DEFAULT 'asia-south1';
ALTER TABLE refresh_tokens  ADD COLUMN IF NOT EXISTS region STRING NOT NULL DEFAULT 'asia-south1';
ALTER TABLE auth_events     ADD COLUMN IF NOT EXISTS region STRING NOT NULL DEFAULT 'asia-south1';
ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS region STRING NOT NULL DEFAULT 'asia-south1';

-- submissions.region and outbox_events.region already exist (V2).

-- ── 2. Set LOCALITY per table ─────────────────────────────────────────────

ALTER TABLE problems        SET LOCALITY GLOBAL;
ALTER TABLE test_cases      SET LOCALITY GLOBAL;
ALTER TABLE users           SET LOCALITY GLOBAL;
ALTER TABLE contests        SET LOCALITY GLOBAL;
ALTER TABLE contest_problems SET LOCALITY GLOBAL;

ALTER TABLE submissions     SET LOCALITY REGIONAL BY ROW;
ALTER TABLE outbox_events   SET LOCALITY REGIONAL BY ROW;
ALTER TABLE idempotency_keys SET LOCALITY REGIONAL BY ROW;
ALTER TABLE refresh_tokens   SET LOCALITY REGIONAL BY ROW;
ALTER TABLE auth_events     SET LOCALITY REGIONAL BY ROW;
