-- =============================================================================
-- ClickHouse DDL for the Kafka Engine + Materialized View analytics path.
--
-- Background: docs/design-docs/clickhouse-kafka-engine-rollout.md.
--
-- Wiring:
--   Kafka topic (analytics_events)
--     -> Kafka Engine table  onlinejudge.analytics_kafka  (proto-parsed)
--     -> Materialized View   onlinejudge.analytics_mv     (push trigger)
--     -> Destination table   onlinejudge.submission_analytics (ReplacingMergeTree)
--
-- Token substitutions performed by scripts/apply-ddl.sh before the file is
-- piped into clickhouse-client:
--   __KAFKA_BROKER_LIST__   -> e.g. "oj-kafka:29092"
--   __KAFKA_TOPIC__         -> e.g. "analytics_events" or "analytics_events.asia-south1"
--   __KAFKA_GROUP_NAME__    -> e.g. "analytics-clickhouse"
--   __KAFKA_CONSUMER_OFFSET__ -> "latest" or "earliest" (only used on first
--                              deploy; see §3.4 of the design doc).
--
-- Every object is created with IF NOT EXISTS so re-applying is a no-op (T2).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS onlinejudge;

-- ---------------------------------------------------------------------------
-- Destination table. Long-lived rows; dedupe via ReplacingMergeTree.
-- ORDER BY is the canonical reporting key (verdict mix per problem,
-- ACCEPTED rate per (user, problem)).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onlinejudge.submission_analytics (
    submission_id      String,
    user_id            String,
    problem_id         String,
    contest_id         String,
    language           LowCardinality(String),
    verdict            LowCardinality(String),
    execution_time_ms  UInt32,
    memory_used_mb     UInt32,
    event_ts_ms        Int64,
    region             LowCardinality(String),
    phase              LowCardinality(String),
    event_time         DateTime64(3) MATERIALIZED toDateTime64(event_ts_ms / 1000, 3),
    ingest_time        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(event_ts_ms)
ORDER BY (contest_id, problem_id, user_id, submission_id)
PARTITION BY toYYYYMM(event_time)
-- TTL must be evaluated on a Date/DateTime, not DateTime64.
TTL toDateTime(event_time) + INTERVAL 18 MONTH;

-- ---------------------------------------------------------------------------
-- Kafka source. ClickHouse owns the consumer group and offsets.
-- kafka_handle_error_mode = 'stream' exposes _error / _raw_message virtual
-- columns so parse failures can be siphoned off (analytics_kafka_errors below)
-- without blocking the main consumer.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onlinejudge.analytics_kafka (
    submission_id      String,
    user_id            String,
    problem_id         String,
    contest_id         String,
    language           String,
    verdict            String,
    execution_time_ms  UInt32,
    memory_used_mb     UInt32,
    event_ts_ms        Int64,
    region             String,
    phase              String
)
ENGINE = Kafka SETTINGS
    kafka_broker_list       = '__KAFKA_BROKER_LIST__',
    kafka_topic_list        = '__KAFKA_TOPIC__',
    kafka_group_name        = '__KAFKA_GROUP_NAME__',
    kafka_format            = 'Protobuf',
    kafka_schema            = 'proto/events.proto:AnalyticsEvent',
    kafka_num_consumers     = 2,
    kafka_max_block_size    = 8192,
    kafka_handle_error_mode = 'stream';

-- ---------------------------------------------------------------------------
-- Push trigger. Runs whenever the Kafka Engine table produces a block and
-- inserts into submission_analytics.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS onlinejudge.analytics_mv
TO onlinejudge.submission_analytics AS
SELECT
    submission_id,
    user_id,
    problem_id,
    contest_id,
    language,
    verdict,
    execution_time_ms,
    memory_used_mb,
    event_ts_ms,
    region,
    phase
FROM onlinejudge.analytics_kafka;

-- ---------------------------------------------------------------------------
-- Error stream. Anything the Protobuf parser rejects lands here.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onlinejudge.analytics_kafka_errors (
    raw_message     String,
    error_message   String,
    error_time      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY error_time
TTL error_time + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS onlinejudge.analytics_errors_mv
TO onlinejudge.analytics_kafka_errors AS
SELECT _raw_message AS raw_message, _error AS error_message
FROM onlinejudge.analytics_kafka
WHERE length(_error) > 0;
