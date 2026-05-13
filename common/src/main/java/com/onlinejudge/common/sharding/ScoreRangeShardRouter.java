package com.onlinejudge.common.sharding;

import java.util.*;

/**
 * Score-range partitioning router for the Redis ZSET leaderboard.
 *
 * At 1M users, a single ZSET on a single Redis node becomes a hot shard.
 * Score-range sharding distributes the ZSET across multiple Redis nodes:
 *
 *   Shard 0 (node A): scores 0 – 29,999,999          (0–2 problems solved)
 *   Shard 1 (node B): scores 30,000,000 – 59,999,999  (3–5 problems solved)
 *   Shard 2 (node C): scores 60,000,000+              (6+ problems solved)
 *
 * Write routing is deterministic: the application computes the composite score
 * and writes to the correct shard based on the score's range.
 *
 * Why not user_id hash sharding? Because hash sharding makes global rank queries
 * expensive. To find user U's rank under hash sharding, you'd need to query every
 * shard for all users with a higher score, merge-sort the results, and find U's
 * position — O(S × N) worst case. Score-range sharding avoids this: shards are
 * naturally ordered by score, so you know which shards contain higher-scoring
 * users without querying them.
 *
 * Global rank computation:
 *   globalRank = sum(ZCARD(shard) for shards with all scores > user's score)
 *              + ZREVRANK(user's shard, userId) + 1
 *
 * ZCARD is O(1). Total computation is O(S + log N) where S = shard count.
 *
 * <p>This class lives in {@code common} so both the scoring-pipeline (Flink sink
 * write side) and the leaderboard-service (read side) can share the same shard
 * topology without duplication.
 */
public class ScoreRangeShardRouter {

    /** Score boundary for each shard. Shard i covers [boundaries[i], boundaries[i+1]) */
    private final long[] boundaries;

    /** Number of shards */
    private final int shardCount;

    /**
     * Creates a router with the given score boundaries.
     * boundaries must be sorted in ascending order.
     * The last shard covers [boundaries[last], +infinity).
     *
     * @param boundaries shard boundary values, e.g., [0, 30_000_000, 60_000_000]
     */
    public ScoreRangeShardRouter(long[] boundaries) {
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException("At least one boundary is required");
        }
        for (int i = 1; i < boundaries.length; i++) {
            if (boundaries[i] <= boundaries[i - 1]) {
                throw new IllegalArgumentException("Boundaries must be strictly ascending");
            }
        }
        this.boundaries = boundaries.clone();
        this.shardCount = boundaries.length;
    }

    /**
     * Default 3-shard router for ICPC scoring:
     *   Shard 0: 0-2 problems solved (scores 0 – 29,999,999)
     *   Shard 1: 3-5 problems solved (scores 30,000,000 – 59,999,999)
     *   Shard 2: 6+ problems solved  (scores 60,000,000+)
     */
    public static ScoreRangeShardRouter defaultIcpcRouter() {
        return new ScoreRangeShardRouter(new long[]{
                0,
                30_000_000,
                60_000_000
        });
    }

    /**
     * Returns the shard index for a given composite ZSET score.
     * The score is routed to the highest shard whose boundary is <= the score.
     */
    public int shardForScore(double zsetScore) {
        long score = Math.round(zsetScore);
        int shard = 0;
        for (int i = 1; i < shardCount; i++) {
            if (score >= boundaries[i]) {
                shard = i;
            }
        }
        return shard;
    }

    /**
     * Returns the Redis key for a specific shard.
     * Key pattern: leaderboard:{contestId}:shard:{shardIndex}
     */
    public String shardKey(String contestId, int shardIndex) {
        return "leaderboard:" + contestId + ":shard:" + shardIndex;
    }

    /**
     * Returns all shard keys for a contest, ordered from highest score shard
     * to lowest. Used for top-N queries that start from the highest shard.
     */
    public List<String> allShardKeysDescending(String contestId) {
        List<String> keys = new ArrayList<>(shardCount);
        for (int i = shardCount - 1; i >= 0; i--) {
            keys.add(shardKey(contestId, i));
        }
        return keys;
    }

    /**
     * Returns the indices of all shards that contain scores strictly higher
     * than the given score. Used for global rank computation.
     *
     * For a user in shard S:
     *   higherShards = all shards with index > S
     *
     * The sum of ZCARD across these shards gives the count of users who
     * definitely rank above the user. The user's local ZREVRANK within
     * their own shard completes the rank calculation.
     */
    public List<Integer> higherShardIndices(double zsetScore) {
        int userShard = shardForScore(zsetScore);
        List<Integer> higher = new ArrayList<>();
        for (int i = userShard + 1; i < shardCount; i++) {
            higher.add(i);
        }
        return higher;
    }

    /**
     * Computes the global rank for a user given:
     * - The user's ZREVRANK within their own shard (0-indexed)
     * - The ZCARD of each higher shard
     *
     * globalRank = sum(higherShardCards) + localRank + 1
     *
     * The +1 converts from 0-indexed to 1-indexed (rank 1 = best).
     */
    public long computeGlobalRank(long localReverseRank, List<Long> higherShardCards) {
        long higherCount = 0;
        for (long card : higherShardCards) {
            higherCount += card;
        }
        return higherCount + localReverseRank + 1;
    }

    /**
     * Returns a page of leaderboard entries across shards.
     *
     * For a top-100 query:
     * 1. Start from the highest shard
     * 2. Read entries until the page is full
     * 3. If the highest shard has fewer than pageSize entries,
     *    continue to the next-lower shard
     *
     * Returns the shard queries needed: [(shardIndex, start, count), ...]
     */
    public List<ShardQuery> planPageQuery(int page, int pageSize) {
        List<ShardQuery> queries = new ArrayList<>();
        long remaining = pageSize;

        // We need to figure out which shard contains the globalStart position.
        // This requires knowing the ZCARD of each shard (passed at query time).
        // For the planning phase, we return a query for every shard from highest to lowest.
        // The caller filters based on actual ZCARDs.
        for (int i = shardCount - 1; i >= 0 && remaining > 0; i--) {
            queries.add(new ShardQuery(i, remaining));
        }

        return queries;
    }

    public int shardCount() { return shardCount; }

    public long[] boundaries() { return boundaries.clone(); }

    /**
     * A query to execute against a single shard.
     */
    public record ShardQuery(int shardIndex, long maxEntries) {}
}
