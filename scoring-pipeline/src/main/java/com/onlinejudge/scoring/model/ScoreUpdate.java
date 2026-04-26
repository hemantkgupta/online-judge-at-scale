package com.onlinejudge.scoring.model;

import java.io.Serializable;

/**
 * Output of the ScoringFunction: emitted after every score change.
 * Consumed by:
 *   - RedisLeaderboardSink (ZADD to ZSET)
 *   - KafkaAnalyticsSink (fire-and-forget to analytics_events)
 */
public record ScoreUpdate(
        String userId,
        String contestId,
        int totalScore,
        int penaltyMinutes,
        double zsetScore,      // composite: (totalScore * 10_000_000) - penaltyMinutes
        long eventTsMs
) implements Serializable {}
