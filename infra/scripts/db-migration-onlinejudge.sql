-- =============================================================================
-- One-off operator runbook: move live rows from `defaultdb` to `onlinejudge`.
--
-- The big idea: V3 collapses the two-database split. This script handles the
-- ONE-WAY data move + the orphan-table cleanup that V3 itself can't do safely
-- (destructive ops in a Flyway migration are too dangerous to re-run).
--
-- Choreography (control-plane VM):
--
--   STEP 1. Pause api-gateway writes (worker is read-only on the DB):
--             gcloud compute ssh oj-control-plane -- "docker stop oj-api-gateway"
--
--   STEP 2. Drop the orphan write-path tables that init.sql created on
--           `onlinejudge` (they are empty — api-gateway never wrote there)
--           AND copy live rows from defaultdb into onlinejudge:
--             gcloud compute scp infra/scripts/db-migration-onlinejudge.sql \
--               oj-control-plane:/tmp/db-migration-onlinejudge.sql
--             gcloud compute ssh oj-control-plane -- "docker exec -i \
--               oj-cockroachdb cockroach sql --insecure \
--               < /tmp/db-migration-onlinejudge.sql"
--           NOTE: the first half of THIS file (the DROP TABLE block) does
--                 not actually depend on V3 having applied — V3 has not
--                 applied yet at this point because api-gateway is stopped.
--                 The INSERT … SELECT half will fail with "relation
--                 onlinejudge.submissions does not exist" — that's expected.
--                 Continue to step 3, then re-run THIS script.
--
--   STEP 3. Push the new compose YAML (V3'd, with onlinejudge URLs) and
--           bring api-gateway back up. Flyway runs V1+V2+V3 against
--           `onlinejudge` and creates the unified schema:
--             gcloud compute scp infra/gcp/compose/control-plane-compose.yml \
--               oj-control-plane:/opt/oj/control-plane-compose.yml
--             gcloud compute ssh oj-control-plane -- "cd /opt/oj && \
--               docker compose -f control-plane-compose.yml up -d"
--
--   STEP 4. Re-run this script — the DROPs are no-ops, and the INSERT halves
--           now succeed (tables exist post-V3):
--             gcloud compute ssh oj-control-plane -- "docker exec -i \
--               oj-cockroachdb cockroach sql --insecure \
--               < /tmp/db-migration-onlinejudge.sql"
--
--   STEP 5. Bounce the worker so it picks up the new JDBC URL:
--             gcloud compute scp infra/gcp/compose/compute-compose.yml \
--               oj-compute:/opt/oj/compute-compose.yml
--             gcloud compute ssh oj-compute -- "cd /opt/oj && \
--               docker compose -f compute-compose.yml up -d"
--
--   STEP 6. Verify the smoke problems still serve:
--             cockroach sql --insecure \
--               -e "SELECT count(*) FROM onlinejudge.problems;"   -- expect >= 2
--             cockroach sql --insecure \
--               -e "SELECT count(*) FROM onlinejudge.submissions;"-- expect = defaultdb.submissions
--
-- Idempotency: every INSERT uses ON CONFLICT DO NOTHING, so re-running is safe.
-- =============================================================================

-- ---- PRECONDITION: drop the orphan init.sql write-path tables on onlinejudge
-- The pre-V3 init.sql created submissions/outbox_events/idempotency_keys
-- inside `onlinejudge` with a SLIGHTLY DIFFERENT shape from the api-gateway's
-- Flyway-managed ones in `defaultdb` (init.sql had outbox_events.payload as
-- JSONB, no `published BOOLEAN`, no `gateway_ts_ms` on submissions). They
-- were never written to (the api-gateway connected to defaultdb), so they
-- are empty. We DROP them here so V3's CREATE TABLE IF NOT EXISTS actually
-- creates the canonical shape on the next api-gateway boot.
--
-- Run this BLOCK BEFORE booting the api-gateway against onlinejudge for the
-- first time. The DROPs are idempotent (CASCADE handles the FKs).
DROP TABLE IF EXISTS onlinejudge.idempotency_keys CASCADE;
DROP TABLE IF EXISTS onlinejudge.outbox_events    CASCADE;
DROP TABLE IF EXISTS onlinejudge.submissions      CASCADE;

-- ---- Sanity: target DB and tables exist (V3 already ran) -------------------
-- If this errors with "relation does not exist", V3 has not yet applied —
-- boot api-gateway against onlinejudge once so Flyway runs V3, then re-run
-- this script.
SELECT 'pre-check: onlinejudge.submissions exists' AS step,
       count(*) AS row_count_before
FROM onlinejudge.submissions;

-- ---- 1. submissions --------------------------------------------------------
-- Copy rows that the api-gateway wrote to defaultdb. The `region` column was
-- added by V2; rows pre-dating V2 default to 'us-east-1' per the migration.
INSERT INTO onlinejudge.submissions (
    id, user_id, problem_id, contest_id, language, s3_code_url,
    status, gateway_ts_ms, region, created_at
)
SELECT
    id, user_id, problem_id, contest_id, language, s3_code_url,
    status, gateway_ts_ms, region, created_at
FROM defaultdb.submissions
ON CONFLICT (id) DO NOTHING;

-- ---- 2. outbox_events ------------------------------------------------------
INSERT INTO onlinejudge.outbox_events (
    id, submission_id, event_type, payload, published, region, created_at
)
SELECT
    id, submission_id, event_type, payload, published, region, created_at
FROM defaultdb.outbox_events
ON CONFLICT (id) DO NOTHING;

-- ---- 3. idempotency_keys ---------------------------------------------------
-- Keys are short-lived (callers re-send within minutes) so dropping them is
-- usually fine, but we copy to preserve cross-cutover idempotency for any
-- in-flight retry. PK is `key` (string), so ON CONFLICT (key) DO NOTHING.
INSERT INTO onlinejudge.idempotency_keys (
    key, submission_id, status, created_at
)
SELECT
    key, submission_id, status, created_at
FROM defaultdb.idempotency_keys
ON CONFLICT (key) DO NOTHING;

-- ---- Post-check counts -----------------------------------------------------
SELECT 'post-move: onlinejudge.submissions'      AS step, count(*) FROM onlinejudge.submissions
UNION ALL
SELECT 'post-move: onlinejudge.outbox_events'    AS step, count(*) FROM onlinejudge.outbox_events
UNION ALL
SELECT 'post-move: onlinejudge.idempotency_keys' AS step, count(*) FROM onlinejudge.idempotency_keys
UNION ALL
SELECT 'reference: defaultdb.submissions'        AS step, count(*) FROM defaultdb.submissions
UNION ALL
SELECT 'reference: defaultdb.outbox_events'      AS step, count(*) FROM defaultdb.outbox_events
UNION ALL
SELECT 'reference: defaultdb.idempotency_keys'   AS step, count(*) FROM defaultdb.idempotency_keys;

-- NOTE: defaultdb.* tables are intentionally LEFT IN PLACE after the move so
-- you can roll back the env-var flip if anything goes wrong. Once the new
-- pipeline has soaked for a release, you can drop them with:
--
--   DROP TABLE defaultdb.idempotency_keys;
--   DROP TABLE defaultdb.outbox_events;
--   DROP TABLE defaultdb.submissions;
--   -- (defaultdb itself is CRDB's built-in DB; leave it alone.)
