# ADR-0007: Per-Region Kafka, No Cross-Region Mirror

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The multi-region rollout places api-gateway, execution-worker, sandbox-manager, leaderboard-service, and CRDB in each of three regions: `asia-south1`, `us-east1`, `europe-west1`. CRDB handles cross-region data via `REGIONAL BY ROW` (submissions) and `GLOBAL` (contests/problems) — see [ADR-0005](./0005-cockroachdb-over-postgres.md).

The question for Kafka: do verdicts and submissions cross regions? The temptation is to mirror topics (MirrorMaker 2 or Confluent Cluster Linking) so a contest's leaderboard reflects all regions' verdicts in one feed. The problem is that a submission's natural locality is the contestant's region — the verdict is computed there and consumed by that region's leaderboard-service. Mirroring adds latency, complexity, and cross-region bandwidth cost for an aggregation the system doesn't need on the hot path.

## Decision

Each region runs its own Kafka cluster. Topics are not mirrored. Submissions, verdicts, and analytics events are region-scoped on Kafka. The cross-region aggregation happens at the CRDB layer (RBR submissions queried by a region-aware reader) and at the scoring-pipeline (Flink reads region-local Kafka, writes contest-final-scores to a GLOBAL-locality CRDB table that all regions read).

## Alternatives considered

**MirrorMaker 2.** Apache's Kafka-to-Kafka replicator. Sub-second mirror lag is achievable but real. Operationally non-trivial: another consumer group per topic, offset translation, conflict handling at consumer-group resume. We don't need the global feed enough to pay for it.

**Confluent Cluster Linking.** Commercial. Easier ops than MM2. Sub-second lag is still > submission round-trip; not a win on the hot path. Cost is also material at the OJ's expected throughput. Rejected.

**Single global Kafka cluster.** SPOF + cross-region write latency on every submission. Untenable for the multi-region rollout's whole point.

**Per-contest dedicated mirror.** Mirror only the topics for an active contest, only between the regions registered for that contest. Complex schedule + topic ACL management. Not worth it.

## Consequences

**Positive:**
- Each region's submission path is fully self-contained. Tokyo's surge cannot back up Frankfurt's broker.
- Failure isolation: a regional Kafka outage degrades only that region's submissions; other regions keep operating.
- No cross-region bandwidth cost for the data plane.
- Simpler operator story: one Kafka cluster per region, no replication topology.

**Negative:**
- Final-score aggregation must happen elsewhere. The scoring pipeline reads region-local Kafka and writes to a GLOBAL CRDB table; the leaderboard for a global contest queries CRDB, not Kafka.
- Analytics queries that span regions must federate at the ClickHouse layer.
- A contest with contestants in multiple regions has a per-region leaderboard during the contest; the cross-region merge is at contest close (when the system-test phase completes).

## Implementation pointers

- Per-region Kafka topology: `infra/terraform/multi-region/kafka/`.
- Region-aware producer/consumer config: `common/src/main/java/com/onlinejudge/common/RegionResolver.java`.
- The full design: [`design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md).
- Operations: [`runbooks/multi-region.md`](../runbooks/multi-region.md).

## Related

- [`tech-spec.md#12-multi-region`](../tech-spec.md#12-multi-region)
- [`design-docs/multi-region-rollout.md`](../design-docs/multi-region-rollout.md)
- [`adr/0005-cockroachdb-over-postgres.md`](./0005-cockroachdb-over-postgres.md)
