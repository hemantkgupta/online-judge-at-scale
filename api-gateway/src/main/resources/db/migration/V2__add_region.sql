-- Add a `region` column to the write-path tables so REGIONAL BY ROW can be
-- enabled on a multi-region CockroachDB cluster (see database/multi-region-setup.sql).
-- On a single-node cluster the column is a plain STRING; in production it is the
-- bound `crdb_internal_region` enum and CRDB pins replicas to the region named
-- in this column.

ALTER TABLE submissions ADD COLUMN IF NOT EXISTS region VARCHAR(32) NOT NULL DEFAULT 'us-east-1';
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS region VARCHAR(32) NOT NULL DEFAULT 'us-east-1';

-- Index outbox by (region, published, created_at) so a regional changefeed /
-- polling publisher can read its own region's events without scanning others.
CREATE INDEX IF NOT EXISTS idx_outbox_region_unpublished
    ON outbox_events (region, published, created_at)
    WHERE published = FALSE;
