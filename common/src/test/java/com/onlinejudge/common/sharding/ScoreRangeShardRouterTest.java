package com.onlinejudge.common.sharding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the score-range shard router.
 *
 * Verifies:
 * - Score-to-shard routing is correct for all ranges
 * - Global rank computation across shards
 * - Key patterns for Redis ZSET per shard
 * - Higher shard identification
 * - Edge cases: boundary scores, negative scores
 * - Constructor validation
 */
class ScoreRangeShardRouterTest {

    private ScoreRangeShardRouter router;

    @BeforeEach
    void setUp() {
        // 3 shards: [0, 30M), [30M, 60M), [60M, +∞)
        router = ScoreRangeShardRouter.defaultIcpcRouter();
    }

    // --- Score routing ---

    @Test
    void shardForScore_zeroSolvedGoesToShard0() {
        assertThat(router.shardForScore(0)).isEqualTo(0);
        assertThat(router.shardForScore(10_000_000)).isEqualTo(0); // 1 solved
        assertThat(router.shardForScore(29_999_999)).isEqualTo(0); // 2 solved, max penalty
    }

    @Test
    void shardForScore_threeSolvedGoesToShard1() {
        assertThat(router.shardForScore(30_000_000)).isEqualTo(1); // 3 solved, 0 penalty
        assertThat(router.shardForScore(49_999_955)).isEqualTo(1); // 4 solved, 45 penalty
        assertThat(router.shardForScore(59_999_999)).isEqualTo(1); // 5 solved, max penalty
    }

    @Test
    void shardForScore_sixPlusSolvedGoesToShard2() {
        assertThat(router.shardForScore(60_000_000)).isEqualTo(2); // 6 solved, 0 penalty
        assertThat(router.shardForScore(99_999_999)).isEqualTo(2); // 9 solved, high penalty
    }

    @Test
    void shardForScore_negativeScore_goesToShard0() {
        // Negative score (0 solved, penalty) goes to shard 0
        assertThat(router.shardForScore(-40)).isEqualTo(0);
    }

    @Test
    void shardForScore_exactBoundary() {
        // Exactly at boundary = belongs to the higher shard
        assertThat(router.shardForScore(30_000_000)).isEqualTo(1);
        assertThat(router.shardForScore(60_000_000)).isEqualTo(2);
    }

    // --- Key patterns ---

    @Test
    void shardKey_generatesCorrectPattern() {
        assertThat(router.shardKey("contest-1", 0)).isEqualTo("leaderboard:contest-1:shard:0");
        assertThat(router.shardKey("contest-1", 1)).isEqualTo("leaderboard:contest-1:shard:1");
        assertThat(router.shardKey("contest-1", 2)).isEqualTo("leaderboard:contest-1:shard:2");
    }

    @Test
    void allShardKeysDescending_highestFirst() {
        List<String> keys = router.allShardKeysDescending("contest-1");

        assertThat(keys).hasSize(3);
        assertThat(keys.get(0)).isEqualTo("leaderboard:contest-1:shard:2"); // Highest first
        assertThat(keys.get(1)).isEqualTo("leaderboard:contest-1:shard:1");
        assertThat(keys.get(2)).isEqualTo("leaderboard:contest-1:shard:0");
    }

    // --- Higher shard identification ---

    @Test
    void higherShardIndices_fromShard0() {
        List<Integer> higher = router.higherShardIndices(10_000_000); // shard 0
        assertThat(higher).containsExactly(1, 2);
    }

    @Test
    void higherShardIndices_fromShard1() {
        List<Integer> higher = router.higherShardIndices(40_000_000); // shard 1
        assertThat(higher).containsExactly(2);
    }

    @Test
    void higherShardIndices_fromShard2_noHigherShards() {
        List<Integer> higher = router.higherShardIndices(70_000_000); // shard 2
        assertThat(higher).isEmpty();
    }

    // --- Global rank computation ---

    @Test
    void computeGlobalRank_topUserInHighestShard() {
        long globalRank = router.computeGlobalRank(0, List.of());
        assertThat(globalRank).isEqualTo(1);
    }

    @Test
    void computeGlobalRank_userInShard1_withHigherShard() {
        long globalRank = router.computeGlobalRank(3, List.of(50L));
        assertThat(globalRank).isEqualTo(54);
    }

    @Test
    void computeGlobalRank_userInShard0_withBothHigherShards() {
        long globalRank = router.computeGlobalRank(10, List.of(50L, 200L));
        assertThat(globalRank).isEqualTo(261);
    }

    @Test
    void computeGlobalRank_realIcpcScenario() {
        // User solved 4 problems (shard 1), local rank = 150, shard 2 has 5K users.
        long globalRank = router.computeGlobalRank(150, List.of(5_000L));
        assertThat(globalRank).isEqualTo(5_151);
    }

    // --- Constructor validation ---

    @Test
    void constructor_nullBoundaries_throws() {
        assertThatThrownBy(() -> new ScoreRangeShardRouter(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_emptyBoundaries_throws() {
        assertThatThrownBy(() -> new ScoreRangeShardRouter(new long[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_nonAscendingBoundaries_throws() {
        assertThatThrownBy(() -> new ScoreRangeShardRouter(new long[]{50, 30, 60}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly ascending");
    }

    @Test
    void customShardConfig_fiveShards() {
        ScoreRangeShardRouter custom = new ScoreRangeShardRouter(
                new long[]{0, 10_000_000, 20_000_000, 40_000_000, 70_000_000});

        assertThat(custom.shardCount()).isEqualTo(5);
        assertThat(custom.shardForScore(5_000_000)).isEqualTo(0);
        assertThat(custom.shardForScore(15_000_000)).isEqualTo(1);
        assertThat(custom.shardForScore(25_000_000)).isEqualTo(2);
        assertThat(custom.shardForScore(50_000_000)).isEqualTo(3);
        assertThat(custom.shardForScore(99_000_000)).isEqualTo(4);
    }
}
