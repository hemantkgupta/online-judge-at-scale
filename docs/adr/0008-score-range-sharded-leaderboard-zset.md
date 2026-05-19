# ADR-0008: Score-Range Sharded Leaderboard ZSET

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The leaderboard for an active contest is the highest-read object in the system: every contestant's SPA polls (and STOMP-subscribes) for the current ranking, often multiple times per minute. The natural Redis representation is a sorted set keyed by `leaderboard:{contestId}`, with `ZADD` per verdict and `ZRANGE` / `ZRANGEBYSCORE` for queries.

The problem at scale: a single Redis ZSET is one key. One key lives on one Redis node. At a major contest (1M concurrent contestants, ~100 ZRANGE/s per contestant), that key becomes the cluster's hot key. ZADD per verdict also concentrates writes on it. Redis Cluster's hash-slot routing doesn't help — every operation on `leaderboard:{contestId}` hashes to the same slot. The throughput ceiling is the throughput of one Redis node, ~1M ops/sec on good hardware — too thin a margin for a major contest's read storm.

## Decision

Shard the contest's ZSET by score range. Keys are `leaderboard:{contestId}:s{idx}` where `s0` holds the top-scoring shard, `s1` the next band down, etc. The shard count is configured per contest (default 16); the score range is calculated from the contest's `max_problem_score` * `num_problems` / `shard_count`. Each shard is independently `ZADD`'d and queried.

For a global top-N query (most common SPA call), the leaderboard-service makes parallel `ZRANGE 0 -1 WITHSCORES` against `s0`, merges in memory if N exceeds s0's cardinality, and bails when N is reached. The hot shard is just s0 — but s0 is now ONE shard of the cluster, not the whole leaderboard.

For "where am I?" (around-me) queries, the leaderboard-service tracks each user's current shard in a small per-user index, queries that shard for the local rank, and offsets by the cumulative count of higher shards.

## Alternatives considered

**Single ZSET per contest.** Hot-key collapse at scale; one Redis node bottleneck.

**Hash by user_id.** Distributes keys uniformly but loses score ordering. Every "top N" query has to read every shard — N parallel reads, merge sorted, take top N. At 1M users this is unacceptable.

**CRDB-backed ranking.** SQL `SELECT user_id, score FROM contest_scores WHERE contest_id=? ORDER BY score DESC LIMIT 100`. Index-backed; fast for small N; degrades for "around me" queries; ~30-100 ms read latency. SLA target is sub-10 ms for the leaderboard read — fails.

**Pre-computed top-100 cached in api-gateway.** Works for the top-N case; fails for "around me"; staleness window. Implemented as a complement, not a replacement.

## Consequences

**Positive:**
- The hot key is `:s0`, which is one of 16 shards — load is bounded by the volume of users with top-band scores (typically small).
- Score-ordered queries within a shard are O(log N) ZRANGE.
- Sharding is transparent to the consumer (the leaderboard-service hides the shard math behind its public API).
- New contests can be re-sharded by adjusting `shard_count` at contest creation.

**Negative:**
- "Top N" across the contest requires up to N/shard_size parallel reads. Typical N=100, shard_size=10k+ → one read. For pathological cases (small top of a small shard), more reads.
- Around-me queries need a per-user shard index that must be updated on every score change. Adds one extra Redis op per verdict.
- Score-range boundaries are static; if score distribution skews dramatically, one shard can become hot (e.g. mid-range mass clustering). Mitigated by an adaptive re-shard at contest mid-point — currently TODO.

## Implementation pointers

- Sharded ZADD: `leaderboard-service/.../service/LeaderboardService.java#updateScore`.
- Sharded read API: `leaderboard-service/.../api/LeaderboardController.java`.
- Shard math: `leaderboard-service/.../sharding/ShardCalculator.java`.
- Per-user shard index (Redis hash): `user-shard:{contestId}` → user_id → shardIdx.
- Owner page detail: [`services/leaderboard-service.md`](../services/leaderboard-service.md).

## Related

- [`tech-spec.md#5-wire-formats-and-data-models`](../tech-spec.md#5-wire-formats-and-data-models)
- [`services/leaderboard-service.md`](../services/leaderboard-service.md)
- [`flows/submission-roundtrip.md`](../flows/submission-roundtrip.md) §3 step 17
