CREATE DATABASE IF NOT EXISTS onlinejudge;
USE onlinejudge;

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username STRING UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE problems (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title STRING NOT NULL,
    time_limit_ms INT NOT NULL,
    memory_limit_mb INT NOT NULL,
    points INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Per-problem ordered catalogue of test cases. Owned by the Problem Service
-- (Workstream A). Input/expected-output bytes live in GCS — this table stores
-- only the keys. The Execution Service reads via
-- `GET /problems/{id}/test-cases?pretestOnly={bool}` and receives V4-signed
-- GCS download URLs (5-min TTL). Workers never read this table directly.
CREATE TABLE test_cases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    problem_id UUID NOT NULL REFERENCES problems(id),
    ordinal INT NOT NULL,
    input_gcs_key STRING NOT NULL,
    expected_output_gcs_key STRING NOT NULL,
    UNIQUE (problem_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_test_cases_problem_ordinal
    ON test_cases (problem_id, ordinal);

-- `region` is added to the write-path tables so a multi-region cluster can
-- enable LOCALITY REGIONAL BY ROW on them (see database/multi-region-setup.sql).
-- On this single-node demo it is just a STRING column carrying the originating
-- region; the application sets it via the X-Region header (or app.region default).
CREATE TABLE submissions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    problem_id UUID NOT NULL REFERENCES problems(id),
    contest_id UUID,
    language STRING NOT NULL,
    s3_code_url STRING NOT NULL,
    status STRING NOT NULL,
    region STRING NOT NULL DEFAULT 'us-east-1',
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID NOT NULL REFERENCES submissions(id),
    event_type STRING NOT NULL,
    payload JSONB NOT NULL,
    region STRING NOT NULL DEFAULT 'us-east-1',
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_region_unpublished
    ON outbox_events (region, created_at);

CREATE TABLE idempotency_keys (
    key STRING PRIMARY KEY,
    submission_id UUID NOT NULL,
    status STRING NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
