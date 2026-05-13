-- ============================================================================
-- CockroachDB REGIONAL BY ROW setup — production reference, not run locally.
--
-- The single-node CockroachDB in docker-compose cannot host multi-region
-- replicas, but the schema is shaped so that, on a multi-region cluster, this
-- script flips it to REGIONAL BY ROW with no application changes:
--
--   * `submissions` and `outbox_events` already carry a `region` column.
--   * The application stamps that column on every insert via RegionResolver
--     (api-gateway), reading the X-Region request header or the
--     `app.region` config default.
--
-- To enable on a real multi-region cluster:
--   1. Provision the cluster with `--locality region=...` on each node.
--   2. Mark the database as multi-region and declare regions.
--   3. Convert the column type and apply the REGIONAL BY ROW locality.
--
-- This file is the canonical reference for those three steps.
-- ============================================================================

-- 1. Declare regions on the database. The PRIMARY REGION is where the
--    leaseholder for global tables lives by default; secondary regions are
--    where REGIONAL BY ROW data can be pinned.
ALTER DATABASE onlinejudge PRIMARY REGION 'us-east-1';
ALTER DATABASE onlinejudge ADD REGION 'eu-west-1';
ALTER DATABASE onlinejudge ADD REGION 'ap-south-1';

-- 2. Re-type the region column to `crdb_internal_region` so it binds to the
--    enum of declared database regions. Existing STRING values must be one of
--    the declared regions or this ALTER fails.
ALTER TABLE submissions
    ALTER COLUMN region TYPE crdb_internal_region
    USING region::crdb_internal_region;
ALTER TABLE outbox_events
    ALTER COLUMN region TYPE crdb_internal_region
    USING region::crdb_internal_region;

-- 3. Apply REGIONAL BY ROW locality. CRDB will now pin each row's replicas
--    to the region named in its `region` column, so:
--      * A gateway pod in `us-east-1` writing a submission with region='us-east-1'
--        hits the local replica — no cross-region round-trip.
--      * Read-your-own-write from the same region is single-region quorum
--        (fast); cross-region reads pay the WAN.
ALTER TABLE submissions SET LOCALITY REGIONAL BY ROW AS region;
ALTER TABLE outbox_events SET LOCALITY REGIONAL BY ROW AS region;

-- Reference tables (rarely-written, mostly-read) belong as GLOBAL so every
-- region reads them from a local follower. The blog's Part 6 calls this out
-- for `problems`.
-- ALTER TABLE problems SET LOCALITY GLOBAL;
