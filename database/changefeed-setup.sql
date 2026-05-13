-- ============================================================================
-- CockroachDB Changefeed setup for the transactional outbox
-- ============================================================================
--
-- This script replaces the `OutboxPublisherJob` polling loop with CockroachDB's
-- native CDC mechanism. The blog (Part 6) describes this as the production
-- approach: the database itself tails its own commit log (Raft log) and emits
-- row-level change events to Kafka with near-zero latency and no application
-- code in the critical path.
--
-- Prerequisites:
--   * CockroachDB v24.1+ (free-tier CDC for Kafka sinks; bumped in docker-compose).
--   * Kafka broker reachable from inside the CockroachDB container.
--     - From cockroachdb container the broker is at kafka:29092 (Docker
--       Compose internal hostname), NOT localhost:9092.
--
-- How to use:
--
--   1. Disable the polling publisher in api-gateway:
--          app.outbox.publisher.enabled=false
--      (or set the env var APP_OUTBOX_PUBLISHER_ENABLED=false before bootRun).
--
--   2. Run this script from the host machine:
--          docker exec -i $(docker compose ps -q cockroachdb) \
--              cockroach sql --insecure --database=onlinejudge \
--              < database/changefeed-setup.sql
--
--   3. Verify the changefeed is running:
--          docker exec $(docker compose ps -q cockroachdb) \
--              cockroach sql --insecure -e "SHOW CHANGEFEED JOBS;"
--
--   4. Watch the events flow:
--          kcat -C -b localhost:9092 -t submissions.pretest -o end -q
--
-- The execution-worker's `SubmissionConsumer` auto-detects changefeed-envelope
-- messages (`{"after": {...}}`) versus the raw-payload format the polling
-- publisher produces, so the rest of the system needs no changes to switch.
--
-- To stop the changefeed:
--          CANCEL JOB <job_id>;   -- get the id from SHOW CHANGEFEED JOBS
--
-- ============================================================================

CREATE CHANGEFEED FOR TABLE outbox_events
    INTO 'kafka://kafka:29092?topic_name=submissions.pretest'
    WITH
        -- envelope=row → strips the `{"after": ..., "updated": ...}` wrapper,
        -- but you still get all row columns. The consumer extracts `payload`
        -- (the JSON event the api-gateway produced) and parses that.
        envelope = 'wrapped',

        -- Skip backfill on existing rows that are already published=true.
        cursor = 'now',

        -- Only emit changes (resolved timestamps + initial row).
        resolved = '10s',

        -- Tighten the checkpoint frequency so failure recovery replays at most
        -- ~30s of events. Production defaults are higher.
        min_checkpoint_frequency = '30s';
