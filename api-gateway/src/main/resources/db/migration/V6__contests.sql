-- Roadmap §4.19: contest-service entity backing table.
--
-- contest-service ships with `ddl-auto: update` today which auto-creates
-- this table on first boot — that violates the §2.2 "Flyway is the single
-- source of truth" stance. This migration lands the canonical schema
-- BEFORE contest-service goes live so we can flip its ddl-auto to
-- `validate` on the same deploy.
--
-- Field shape matches com.onlinejudge.contest.model.Contest exactly:
--   - id (UUID PK)
--   - title (STRING)
--   - state (STRING enum stored as STRING, values from ContestState)
--   - start_time / end_time (TIMESTAMP)
--   - duration_minutes (INT → BIGINT on CRDB; entity carries Java `int`,
--     small enough that the widening read-back via Long.intValue() is safe)
--   - encryption_key / encrypted_bundle_url (STRING, nullable)
--   - version (BIGINT, JPA @Version optimistic-lock counter)
--   - created_at / updated_at (TIMESTAMP)

CREATE TABLE IF NOT EXISTS contests (
    id                   UUID        PRIMARY KEY,
    title                STRING      NOT NULL,
    state                STRING      NOT NULL DEFAULT 'CREATED',
    start_time           TIMESTAMP   NOT NULL,
    end_time             TIMESTAMP   NOT NULL,
    duration_minutes     INT         NOT NULL,
    encryption_key       STRING,
    encrypted_bundle_url STRING,
    version              INT         NOT NULL DEFAULT 0,
    created_at           TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT now()
);

-- Hot index: api-gateway's ContestWindowFilter queries by state to know
-- which contests are currently RUNNING and need their freeze window enforced.
CREATE INDEX IF NOT EXISTS idx_contests_state_time
    ON contests (state, start_time, end_time);
