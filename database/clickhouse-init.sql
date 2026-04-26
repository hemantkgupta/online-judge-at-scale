-- ClickHouse analytics schema
-- Engine: MergeTree for local dev
-- Production: AggregatingMergeTree + Materialized View from Kafka Engine

CREATE DATABASE IF NOT EXISTS onlinejudge;

CREATE TABLE IF NOT EXISTS onlinejudge.submission_analytics (
    submission_id    UUID,
    user_id          UUID,
    problem_id       UUID,
    contest_id       String,
    language         String,
    verdict          String,
    execution_time_ms Int32,
    memory_used_mb   Int32,
    event_time       DateTime
) ENGINE = MergeTree()
  ORDER BY (contest_id, problem_id, event_time)
  PARTITION BY toYYYYMM(event_time);

-- Useful analytics queries:
--
-- Verdict distribution per problem:
-- SELECT problem_id, verdict, count() AS cnt
-- FROM onlinejudge.submission_analytics
-- WHERE contest_id = 'your-contest-id'
-- GROUP BY problem_id, verdict ORDER BY problem_id, cnt DESC;
--
-- Language popularity:
-- SELECT language, count() AS cnt
-- FROM onlinejudge.submission_analytics
-- GROUP BY language ORDER BY cnt DESC;
--
-- Submission velocity per hour:
-- SELECT toStartOfHour(event_time) AS hour, count() AS submissions
-- FROM onlinejudge.submission_analytics
-- GROUP BY hour ORDER BY hour;
