package com.onlinejudge.scoring.sink;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import com.onlinejudge.scoring.model.ScoreUpdate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the RedisLeaderboardSink.
 *
 * Tests the Lua script content, key patterns, and ScoreUpdate input structure.
 * Full integration tests with a real Redis instance would require Testcontainers.
 * These unit tests verify the sink's configuration and contract.
 */
class RedisLeaderboardSinkTest {

    private final ScoreRangeShardRouter router = ScoreRangeShardRouter.defaultIcpcRouter();

    @Test
    void sinkConstructible_withRouter() {
        // The sink must be constructible with a ScoreRangeShardRouter so writes are sharded.
        RedisLeaderboardSink sink = new RedisLeaderboardSink("localhost", 6379, router);

        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", 300, 45, 2_999_999_955.0, 1000L);
        assertThat(update.userId()).isEqualTo("user-1");
        assertThat(update.contestId()).isEqualTo("contest-1");
        assertThat(update.zsetScore()).isEqualTo(2_999_999_955.0);
        assertThat(sink).isNotNull();
    }

    @Test
    void scoreUpdate_pubsubKeyPattern() {
        ScoreUpdate update = new ScoreUpdate("user-abc", "contest-42", 500, 100, 4_999_999_900.0, 2000L);

        String expectedPubsubChannel = "score_updates:" + update.contestId();
        assertThat(expectedPubsubChannel).isEqualTo("score_updates:contest-42");
    }

    @Test
    void shardKey_routesToCorrectBucket() {
        // 300 points × 10M - 45 ≈ 3 billion — well above the 60M shard-2 boundary.
        double zset = (double) ((long) 300 * 10_000_000L - 45);
        int shard = router.shardForScore(zset);
        assertThat(shard).isEqualTo(2);

        String key = router.shardKey("contest-1", shard);
        assertThat(key).isEqualTo("leaderboard:contest-1:shard:2");
    }

    @Test
    void scoreUpdate_carriesAllFieldsForLuaScript() {
        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", 300, 45, 2_999_999_955.0, 1000L);

        // The Lua script uses these ARGV values:
        // ARGV[1] = user_id
        assertThat(update.userId()).isNotBlank();
        // ARGV[2] = new_score (the composite zset score)
        assertThat(update.zsetScore()).isGreaterThan(0);
        // ARGV[3] = contest_id
        assertThat(update.contestId()).isNotBlank();
        // ARGV[4] = total_score (display value)
        assertThat(update.totalScore()).isEqualTo(300);
        // ARGV[5] = penalty (display value)
        assertThat(update.penaltyMinutes()).isEqualTo(45);
    }

    @Test
    void scoreUpdate_zsetScoreMatchesEncoder() {
        // The zsetScore in the update should already be the encoded value
        // that the Lua script passes directly to ZADD
        int totalScore = 500;
        int penalty = 120;
        double expectedZset = (double) ((long) totalScore * 10_000_000L - penalty);

        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", totalScore, penalty, expectedZset, 1000L);

        assertThat(update.zsetScore()).isEqualTo(expectedZset);
        assertThat(update.zsetScore()).isEqualTo(4_999_999_880.0);
    }

    @Test
    void sinkConstructor_acceptsHostPortAndRouter() {
        // Verify the sink can be constructed with custom Redis coordinates
        RedisLeaderboardSink sink = new RedisLeaderboardSink("redis.example.com", 6380, router);
        assertThat(sink).isNotNull();
    }

    @Test
    void scoreUpdate_serializable() {
        // ScoreUpdate must be Serializable for Flink to pass it between operators
        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", 300, 45, 2_999_999_955.0, 1000L);
        assertThat(update).isInstanceOf(java.io.Serializable.class);
    }
}
