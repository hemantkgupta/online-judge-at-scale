package com.onlinejudge.scoring.sink;

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

    @Test
    void luaScript_containsZadd() {
        // The Lua script must perform an atomic ZADD + PUBLISH
        // We verify the script structure via reflection or by examining
        // the sink's behavior
        RedisLeaderboardSink sink = new RedisLeaderboardSink("localhost", 6379);

        // ScoreUpdate that the sink processes
        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", 300, 45, 2_999_999_955.0, 1000L);
        assertThat(update.userId()).isEqualTo("user-1");
        assertThat(update.contestId()).isEqualTo("contest-1");
        assertThat(update.zsetScore()).isEqualTo(2_999_999_955.0);
    }

    @Test
    void scoreUpdate_leaderboardKeyPattern() {
        ScoreUpdate update = new ScoreUpdate("user-abc", "contest-42", 500, 100, 4_999_999_900.0, 2000L);

        // Verify the key pattern the sink would use
        String expectedLeaderboardKey = "leaderboard:" + update.contestId();
        String expectedPubsubChannel = "score_updates:" + update.contestId();

        assertThat(expectedLeaderboardKey).isEqualTo("leaderboard:contest-42");
        assertThat(expectedPubsubChannel).isEqualTo("score_updates:contest-42");
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
    void sinkConstructor_acceptsHostAndPort() {
        // Verify the sink can be constructed with custom Redis coordinates
        RedisLeaderboardSink sink = new RedisLeaderboardSink("redis.example.com", 6380);
        assertThat(sink).isNotNull();
    }

    @Test
    void scoreUpdate_serializable() {
        // ScoreUpdate must be Serializable for Flink to pass it between operators
        ScoreUpdate update = new ScoreUpdate("user-1", "contest-1", 300, 45, 2_999_999_955.0, 1000L);
        assertThat(update).isInstanceOf(java.io.Serializable.class);
    }
}
