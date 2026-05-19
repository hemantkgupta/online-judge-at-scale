# ADR-0005: CockroachDB over PostgreSQL

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The OJ has two structurally different tables with opposite locality needs:

- **submissions**: write-heavy, read-rare (written once per submit, read once by the same-region worker). Cross-region writes would add WAN latency to the submit path.
- **contests / problems**: read-heavy, write-rare (checked thousands of times per second per region during a contest; updated twice per contest's lifetime). Cross-region reads would add WAN latency to the contestant's front-end checks.

A single-leader RDBMS (PostgreSQL or AWS Aurora) optimises for one but not the other. The OJ also needs native CDC into Kafka for the outbox pattern (see [ADR-0002](./0002-transactional-outbox-over-sync-publish.md)) without standing up a separate Debezium cluster.

## Decision

Use CockroachDB. Use `REGIONAL BY ROW` locality on `submissions` (writes pinned to the contestant's region) and `GLOBAL` locality on `contests` and `problems` (replicated to all regions for sub-10ms reads). Use CRDB's native changefeed for the outbox → Kafka publish.

## Alternatives considered

**PostgreSQL primary + read replicas.** Locality is single-region; cross-region writes pay WAN replication lag. There's no per-table locality knob — you can't have the submissions table live in three regions and the contests table replicated everywhere. Rejected because the OJ's workload literally demands both at once.

**AWS Aurora.** Vendor lock-in, no GLOBAL/RBR equivalent at the row level, no native CDC into Kafka without Debezium.

**Spanner.** Strong locality controls + global consistency + native changefeeds. Cost is significantly higher than CRDB at the OJ's expected query volume. Also GCP-only — would be acceptable but the team's CRDB familiarity won.

**Per-region MySQL with bidirectional replication.** Operational nightmare for active-active. Rejected.

## Consequences

**Positive:**
- Two tables, two locality policies, one cluster. Matches the OJ workload exactly.
- Native changefeed → Kafka eliminates Debezium as a separately-operated component.
- Serializable isolation by default (no `READ COMMITTED` foot-guns).
- Raft per range: one node failure tolerated transparently.

**Negative:**
- CRDB changefeed and locality are CRDB-specific. Migration to another DB would require a Debezium deployment + a partitioning strategy.
- CRDB is operationally heavier than PostgreSQL for a single-region deployment. The local-dev compose runs a single-node CRDB which papers over this.
- Some SQL patterns common in Postgres-land (e.g. `SERIAL` keys) don't translate cleanly — CRDB encourages UUIDs.
- CRDB's `INT` is `BIGINT`. JPA entity field types must match — a real production bug class.

## Implementation pointers

- Schema: `database/init.sql`.
- Locality policies: applied at table creation time in `database/init.sql`.
- Changefeed config: `database/init.sql` after table creation.
- JPA entity fields: `Problem.java`, `IdempotencyKey.java` use `Long`/`long` for CRDB INT columns.
- Multi-region cluster topology: [`design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md).
- Cluster bring-up runbook: [`runbooks/multi-region.md`](../runbooks/multi-region.md).

## Related

- [`tech-spec.md#5-wire-formats-and-data-models`](../tech-spec.md#5-wire-formats-and-data-models)
- [`tech-spec.md#12-multi-region`](../tech-spec.md#12-multi-region)
- [`adr/0001-multi-service-event-driven.md`](./0001-multi-service-event-driven.md) — original cross-cutting rationale
- [`adr/0002-transactional-outbox-over-sync-publish.md`](./0002-transactional-outbox-over-sync-publish.md)
- [`adr/0007-per-region-kafka-no-cross-region-mirror.md`](./0007-per-region-kafka-no-cross-region-mirror.md)
