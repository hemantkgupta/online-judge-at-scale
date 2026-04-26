package com.onlinejudge.leaderboard.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Read path: Redis ZSET operations for leaderboard queries.
 *
 * All reads are served from Redis - no relational DB involved.
 * ZREVRANGE: O(log N + M) where M = page size. At 100K users, log N ~ 17.
 * ZREVRANK:  O(log N).
 * ZCARD:     O(1).
 *
 * Production: score-range sharding across 3 nodes. Global rank =
 *   ZCARD(higher buckets) + ZREVRANK(user's bucket, userId).
 * Local: single node, all in one ZSET key "leaderboard:{contestId}".
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LeaderboardService {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.leaderboard.default-page-size:100}")
    private int defaultPageSize;

    /**
     * Returns a leaderboard page. Higher score = lower rank index (rank 1 = best).
     */
    public List<Map<String, Object>> getLeaderboardPage(String contestId, int page, int size) {
        String key = "leaderboard:" + contestId;
        int pageSize = size > 0 ? Math.min(size, 500) : defaultPageSize;
        long start = (long) page * pageSize;
        long end   = start + pageSize - 1;

        Set<ZSetOperations.TypedTuple<String>> entries =
                redisTemplate.opsForZSet().reverseRangeWithScores(key, start, end);

        if (entries == null || entries.isEmpty()) return List.of();

        List<Map<String, Object>> rows = new ArrayList<>();
        long rank = start + 1;
        for (ZSetOperations.TypedTuple<String> entry : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("rank", rank++);
            row.put("userId", entry.getValue());
            row.put("zsetScore", entry.getScore());
            row.put("points",   decodePoints(entry.getScore()));
            row.put("penaltyMinutes", decodePenalty(entry.getScore()));
            rows.add(row);
        }
        return rows;
    }

    /**
     * Returns a user's 1-indexed rank (1 = best). -1 if not on leaderboard.
     */
    public long getUserRank(String contestId, String userId) {
        String key = "leaderboard:" + contestId;
        Long rank = redisTemplate.opsForZSet().reverseRank(key, userId);
        return rank != null ? rank + 1 : -1;
    }

    /**
     * Returns the user's current score entry.
     */
    public Map<String, Object> getUserScore(String contestId, String userId) {
        String key = "leaderboard:" + contestId;
        Double zsetScore = redisTemplate.opsForZSet().score(key, userId);
        long rank = getUserRank(contestId, userId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("rank", rank);
        result.put("points", zsetScore != null ? decodePoints(zsetScore) : 0);
        result.put("penaltyMinutes", zsetScore != null ? decodePenalty(zsetScore) : 0);
        return result;
    }

    public long getParticipantCount(String contestId) {
        Long count = redisTemplate.opsForZSet().zCard("leaderboard:" + contestId);
        return count != null ? count : 0L;
    }

    // Decode points from composite ZSET score
    private long decodePoints(double zsetScore) {
        return Math.round(zsetScore) / 10_000_000L;
    }

    // Decode penalty from composite ZSET score
    private long decodePenalty(double zsetScore) {
        long points = decodePoints(zsetScore);
        return Math.round(points * 10_000_000L - zsetScore);
    }
}
