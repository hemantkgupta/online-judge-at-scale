CREATE TABLE IF NOT EXISTS submissions (
    id             UUID        PRIMARY KEY,
    user_id        UUID        NOT NULL,
    problem_id     UUID        NOT NULL,
    contest_id     UUID,
    language       VARCHAR(32) NOT NULL,
    s3_code_url    TEXT        NOT NULL,
    status         VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    gateway_ts_ms  BIGINT      NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id  UUID        NOT NULL REFERENCES submissions(id),
    event_type     VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    published      BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (published, created_at)
    WHERE published = FALSE;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            VARCHAR(64) PRIMARY KEY,
    submission_id  UUID        NOT NULL,
    status         VARCHAR(32) NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);
