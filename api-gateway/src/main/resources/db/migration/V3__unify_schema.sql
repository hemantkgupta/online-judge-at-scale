-- =============================================================================
-- V3: Single-database unification.
--
-- Background
-- ----------
-- Pre-V3 we ran a two-database split on the same CRDB cluster:
--   * `defaultdb`    — api-gateway Flyway (submissions, outbox_events, idempotency_keys)
--   * `onlinejudge`  — database/init.sql (users, problems, test_cases, plus a parallel
--                      copy of submissions/outbox_events/idempotency_keys that nobody
--                      actually wrote to)
--
-- The split was an accident of history (api-gateway picked the CRDB superuser's
-- default DB; problem-service picked the name baked into init.sql) and it bit
-- us every time a service's view of the schema drifted. From V3 onward there
-- is one logical database, `onlinejudge`, and Flyway in api-gateway is the
-- canonical migration tool. `database/init.sql` is retired.
--
-- This migration is the FIRST one to run against the `onlinejudge` database
-- when api-gateway is flipped over (compose `SPRING_DATASOURCE_URL` changes
-- from `…/defaultdb` to `…/onlinejudge`). It therefore has two jobs:
--
--   1. On the existing `onlinejudge` database (which already has
--      users / problems / test_cases from init.sql, but is MISSING
--      submissions / outbox_events / idempotency_keys because nobody
--      wrote them there) — create the three write-path tables and their
--      indexes, matching what Flyway V1 + V2 left on `defaultdb`.
--
--   2. On a fresh `onlinejudge` database (greenfield install, after
--      init.sql is gone) — create users / problems / test_cases so
--      problem-service still has a schema to validate against.
--
-- Every statement is `IF NOT EXISTS` so V3 is a no-op on a database that
-- already has the unified schema — re-running migrations is safe.
--
-- Data move is NOT in this file. The operator moves rows out of
-- defaultdb.{submissions,outbox_events,idempotency_keys} as a separate step
-- (see infra/scripts/db-migration-onlinejudge.sql) before flipping the env vars.
-- =============================================================================

-- ---- problem-service tables (from database/init.sql) ----------------------

CREATE TABLE IF NOT EXISTS users (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    username    STRING      UNIQUE NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT now()
);

-- problem-service entities use Java `long` for time_limit_ms, memory_limit_mb,
-- points (matching CRDB's `INT` = BIGINT). Hibernate's ddl-auto=validate
-- relies on the column being BIGINT — DO NOT downcast these to INT4.
CREATE TABLE IF NOT EXISTS problems (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    title           STRING      NOT NULL,
    time_limit_ms   INT         NOT NULL,
    memory_limit_mb INT         NOT NULL,
    points          INT         NOT NULL,
    created_at      TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS test_cases (
    id                       UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    problem_id               UUID    NOT NULL REFERENCES problems(id),
    ordinal                  INT     NOT NULL,
    input_gcs_key            STRING  NOT NULL,
    expected_output_gcs_key  STRING  NOT NULL,
    UNIQUE (problem_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_test_cases_problem_ordinal
    ON test_cases (problem_id, ordinal);

-- ---- api-gateway write-path tables (mirroring V1 + V2 on defaultdb) -------
-- These are what live on defaultdb today; recreate them on onlinejudge so
-- when api-gateway flips DBs it finds the schema it expects. Column types
-- must match the JPA entities exactly (gateway runs ddl-auto=validate).
--   * submissions.gateway_ts_ms — BIGINT (Java `long`)
--   * outbox_events.payload     — TEXT  (entity uses columnDefinition="TEXT";
--                                        do NOT switch to JSONB here, init.sql's
--                                        old `payload JSONB` would fail Hibernate
--                                        validation)
--   * outbox_events.published   — BOOLEAN (entity field)

CREATE TABLE IF NOT EXISTS submissions (
    id             UUID        PRIMARY KEY,
    user_id        UUID        NOT NULL,
    problem_id     UUID        NOT NULL,
    contest_id     UUID,
    language       VARCHAR(32) NOT NULL,
    s3_code_url    TEXT        NOT NULL,
    status         VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    gateway_ts_ms  BIGINT      NOT NULL,
    region         VARCHAR(32) NOT NULL DEFAULT 'us-east-1',
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id  UUID        NOT NULL REFERENCES submissions(id),
    event_type     VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    published      BOOLEAN     NOT NULL DEFAULT FALSE,
    region         VARCHAR(32) NOT NULL DEFAULT 'us-east-1',
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (published, created_at)
    WHERE published = FALSE;

CREATE INDEX IF NOT EXISTS idx_outbox_region_unpublished
    ON outbox_events (region, published, created_at)
    WHERE published = FALSE;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            VARCHAR(64) PRIMARY KEY,
    submission_id  UUID        NOT NULL,
    status         VARCHAR(32) NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);
