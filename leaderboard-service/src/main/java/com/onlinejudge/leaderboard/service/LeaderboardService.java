package com.onlinejudge.leaderboard.service;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import com.onlinejudge.leaderboard.dto.LeaderboardEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Read path: Redis ZSET operations for leaderboard queries, routed across
 * score-range shards via {@link ScoreRangeShardRouter}.
 *
 * All reads are served from Redis — no relational DB involved.
 *
 * <p>For each query, the service walks shards from highest score to lowest:
 * <ul>
 *   <li>{@link #getLeaderboardPage(String, int, int)} fetches {@code ZCARD} of
 *       each shard once, then issues at most one {@code ZREVRANGE} per shard
 *       crossed by the requested page window.</li>
 *   <li>{@link #getUserRank(String, String)} locates the user via {@code ZSCORE}
 *       per shard (O(S) calls, S = shard count, ≤ 5 in practice), then sums
 *       {@code ZCARD} of higher shards + the user's local {@code ZREVRANK}.</li>
 *   <li>{@link #getParticipantCount(String)} sums {@code ZCARD} across all
 *       shards.</li>
 * </ul>
 *
 * <p>Per the Part 8 / Part 9 design: total complexity is O(S + log N) where
 * S is the shard count (3–5) and N is the per-shard cardinality.
 */
@Slf4j
@Service
public class LeaderboardService {

    /**
     * Read-only template. In single-node mode this is the primary; with a replica
     * configured ({@code app.redis.replica.host}) it points at the replica.
     * Either way the leaderboard reads — ZREVRANGE, ZREVRANK, ZSCORE, ZCARD — are
     * served from this template and never block writes from the scoring pipeline.
     */
    private final StringRedisTemplate redisTemplate;
    private final ScoreRangeShardRouter shardRouter;

    public LeaderboardService(@Qualifier("readRedisTemplate") StringRedisTemplate redisTemplate,
                              ScoreRangeShardRouter shardRouter) {
        this.redisTemplate = redisTemplate;
        this.shardRouter = shardRouter;
    }

    @Value("${app.leaderboard.default-page-size:100}")
    private int defaultPageSize;

    /**
     * Returns a leaderboard page across all shards. Page 0 = top of the
     * highest-score shard. Ranks are 1-indexed (rank 1 = best).
     */
    public List<Map<String, Object>> getLeaderboardPage(String contestId, int page, int size) {
        int pageSize = size > 0 ? Math.min(size, 500) : defaultPageSize;
        long globalStart = (long) page * pageSize;
        long remaining = pageSize;

        // 1. Fetch ZCARD of every shard (highest-score shard first).
        int shardCount = shardRouter.shardCount();
        long[] cardsDesc = new long[shardCount];     // cardsDesc[0] = highest shard
        for (int i = 0; i < shardCount; i++) {
            int shardIdx = shardCount - 1 - i;
            Long card = redisTemplate.opsForZSet().zCard(shardRouter.shardKey(contestId, shardIdx));
            cardsDesc[i] = card != null ? card : 0L;
        }

        // 2. Walk shards from highest to lowest, accumulating until the page is full.
        List<Map<String, Object>> result = new ArrayList<>();
        long cumulative = 0;          // count of entries in shards higher than the current one
        long rank = globalStart + 1;  // 1-indexed running rank counter

        for (int i = 0; i < shardCount && remaining > 0; i++) {
            int shardIdx = shardCount - 1 - i;
            long shardCard = cardsDesc[i];

            if (shardCard == 0) {
                continue;
            }

            if (globalStart >= cumulative + shardCard) {
                cumulative += shardCard;
                continue;
            }

            long localStart = Math.max(0L, globalStart - cumulative);
            long localEnd   = Math.min(shardCard - 1L, localStart + remaining - 1L);

            Set<ZSetOperations.TypedTuple<String>> entries =
                    redisTemplate.opsForZSet().reverseRangeWithScores(
                            shardRouter.shardKey(contestId, shardIdx), localStart, localEnd);

            if (entries != null) {
                for (ZSetOperations.TypedTuple<String> entry : entries) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("rank", rank++);
                    row.put("userId", entry.getValue());
                    row.put("zsetScore", entry.getScore());
                    row.put("points",   decodePoints(entry.getScore()));
                    row.put("penaltyMinutes", decodePenalty(entry.getScore()));
                    result.add(row);
                }
            }

            remaining -= (localEnd - localStart + 1L);
            cumulative += shardCard;
        }

        return result;
    }

    /**
     * Returns a user's 1-indexed global rank across all shards, or -1 if the
     * user is not on the leaderboard.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Probe each shard with {@code ZSCORE} to find which one holds the user.</li>
     *   <li>Sum {@code ZCARD} of every shard above the user's shard.</li>
     *   <li>Read the user's {@code ZREVRANK} within their own shard.</li>
     *   <li>Apply {@link ScoreRangeShardRouter#computeGlobalRank}.</li>
     * </ol>
     */
    public long getUserRank(String contestId, String userId) {
        int shardCount = shardRouter.shardCount();

        int userShard = -1;
        for (int i = shardCount - 1; i >= 0; i--) {
            Double s = redisTemplate.opsForZSet().score(shardRouter.shardKey(contestId, i), userId);
            if (s != null) {
                userShard = i;
                break;
            }
        }
        if (userShard < 0) {
            return -1;
        }

        List<Long> higherCards = new ArrayList<>();
        for (int i = userShard + 1; i < shardCount; i++) {
            Long c = redisTemplate.opsForZSet().zCard(shardRouter.shardKey(contestId, i));
            higherCards.add(c != null ? c : 0L);
        }

        Long localRank = redisTemplate.opsForZSet()
                .reverseRank(shardRouter.shardKey(contestId, userShard), userId);
        if (localRank == null) {
            return -1;
        }

        return shardRouter.computeGlobalRank(localRank, higherCards);
    }

    /**
     * Returns the user's current score entry: rank + decoded points + penalty.
     */
    public Map<String, Object> getUserScore(String contestId, String userId) {
        int shardCount = shardRouter.shardCount();

        Double zsetScore = null;
        for (int i = shardCount - 1; i >= 0; i--) {
            Double s = redisTemplate.opsForZSet().score(shardRouter.shardKey(contestId, i), userId);
            if (s != null) {
                zsetScore = s;
                break;
            }
        }

        long rank = getUserRank(contestId, userId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("rank", rank);
        result.put("points", zsetScore != null ? decodePoints(zsetScore) : 0);
        result.put("penaltyMinutes", zsetScore != null ? decodePenalty(zsetScore) : 0);
        return result;
    }

    /**
     * Returns a leaderboard page as typed {@link LeaderboardEntry} rows (no region tag).
     * Thin adapter over {@link #getLeaderboardPage(String, int, int)} for the
     * cross-region merge path in the controller. The {@code region} field is
     * left null here; the caller stamps it.
     */
    public List<LeaderboardEntry> getTop(String contestId, int page, int size) {
        List<Map<String, Object>> raw = getLeaderboardPage(contestId, page, size);
        List<LeaderboardEntry> out = new ArrayList<>(raw.size());
        for (Map<String, Object> row : raw) {
            long rank = ((Number) row.get("rank")).longValue();
            String userId = (String) row.get("userId");
            Double zsetScore = (Double) row.get("zsetScore");
            long points = ((Number) row.get("points")).longValue();
            long penalty = ((Number) row.get("penaltyMinutes")).longValue();
            out.add(new LeaderboardEntry(rank, userId, zsetScore, points, penalty, null));
        }
        return out;
    }

    /** Total participants on the leaderboard: sum of ZCARD across all shards. */
    public long getParticipantCount(String contestId) {
        long total = 0;
        for (int i = 0; i < shardRouter.shardCount(); i++) {
            Long card = redisTemplate.opsForZSet().zCard(shardRouter.shardKey(contestId, i));
            if (card != null) total += card;
        }
        return total;
    }

    // Decode points from composite ZSET score.
    // Note: with the encoding `points * 10M - penalty`, integer division loses
    // the points-boundary because penalty subtracts into the next-lower bucket.
    // Round on the floating-point divide instead so e.g. (4_999_999_880 / 10M)
    // decodes back to 500, not 499.
    private long decodePoints(double zsetScore) {
        return Math.round(zsetScore / 10_000_000.0);
    }

    // Decode penalty from composite ZSET score
    private long decodePenalty(double zsetScore) {
        long points = decodePoints(zsetScore);
        return Math.round(points * 10_000_000L - zsetScore);
    }
}
