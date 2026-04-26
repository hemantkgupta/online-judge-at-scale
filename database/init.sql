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
    points INT NOT NULL
);

CREATE TABLE submissions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    problem_id UUID NOT NULL REFERENCES problems(id),
    contest_id UUID,
    language STRING NOT NULL,
    s3_code_url STRING NOT NULL,
    status STRING NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID NOT NULL REFERENCES submissions(id),
    event_type STRING NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE idempotency_keys (
    key STRING PRIMARY KEY,
    submission_id UUID NOT NULL,
    status STRING NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
