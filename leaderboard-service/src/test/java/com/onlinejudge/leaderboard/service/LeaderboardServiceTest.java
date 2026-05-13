package com.onlinejudge.leaderboard.service;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Tests for the Leaderboard Service read path.
 *
 * Verifies sharded ZSET access via {@link ScoreRangeShardRouter}:
 *   - top-N page reads start from the highest-score shard
 *   - global rank = sum(ZCARD higher shards) + ZREVRANK in own shard + 1
 *   - participant count is a sum of ZCARD across all shards
 */
@ExtendWith(MockitoExtension.class)
class LeaderboardServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    private final ScoreRangeShardRouter router = ScoreRangeShardRouter.defaultIcpcRouter();

    private LeaderboardService leaderboardService;

    private static final String CONTEST = "contest-1";
    private static final String SHARD_0 = "leaderboard:contest-1:shard:0";
    private static final String SHARD_1 = "leaderboard:contest-1:shard:1";
    private static final String SHARD_2 = "leaderboard:contest-1:shard:2";

    @BeforeEach
    void setUp() {
        leaderboardService = new LeaderboardService(redisTemplate, router);
        ReflectionTestUtils.setField(leaderboardService, "defaultPageSize", 100);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        // Default: empty shards unless overridden by the test
        lenient().when(zSetOperations.zCard(anyString())).thenReturn(0L);
    }

    // --- Pagination ---

    @Test
    void getLeaderboardPage_topUsersAreInHighestShard() {
        // Three users, all in shard 2 (300pts, 200pts → both > 60M boundary, so shard 2)
        Set<ZSetOperations.TypedTuple<String>> shard2Entries = new LinkedHashSet<>();
        shard2Entries.add(new DefaultTypedTuple<>("user-a", 2_999_999_955.0)); // 300pts, 45min
        shard2Entries.add(new DefaultTypedTuple<>("user-b", 2_999_999_940.0)); // 300pts, 60min
        shard2Entries.add(new DefaultTypedTuple<>("user-c", 2_000_000_000.0)); // 200pts

        when(zSetOperations.zCard(SHARD_2)).thenReturn(3L);
        when(zSetOperations.reverseRangeWithScores(SHARD_2, 0L, 2L)).thenReturn(shard2Entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage(CONTEST, 0, 100);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).get("rank")).isEqualTo(1L);
        assertThat(result.get(0).get("userId")).isEqualTo("user-a");
        assertThat(result.get(1).get("rank")).isEqualTo(2L);
        assertThat(result.get(2).get("rank")).isEqualTo(3L);
    }

    @Test
    void getLeaderboardPage_spansMultipleShards() {
        // 2 users in shard 2 + 2 users in shard 1 + 1 user in shard 0
        Set<ZSetOperations.TypedTuple<String>> shard2 = new LinkedHashSet<>();
        shard2.add(new DefaultTypedTuple<>("top-1", 70_000_000.0));
        shard2.add(new DefaultTypedTuple<>("top-2", 60_500_000.0));

        Set<ZSetOperations.TypedTuple<String>> shard1 = new LinkedHashSet<>();
        shard1.add(new DefaultTypedTuple<>("mid-1", 45_000_000.0));
        shard1.add(new DefaultTypedTuple<>("mid-2", 30_500_000.0));

        Set<ZSetOperations.TypedTuple<String>> shard0 = new LinkedHashSet<>();
        shard0.add(new DefaultTypedTuple<>("low-1", 15_000_000.0));

        when(zSetOperations.zCard(SHARD_2)).thenReturn(2L);
        when(zSetOperations.zCard(SHARD_1)).thenReturn(2L);
        when(zSetOperations.zCard(SHARD_0)).thenReturn(1L);
        when(zSetOperations.reverseRangeWithScores(SHARD_2, 0L, 1L)).thenReturn(shard2);
        when(zSetOperations.reverseRangeWithScores(SHARD_1, 0L, 1L)).thenReturn(shard1);
        when(zSetOperations.reverseRangeWithScores(SHARD_0, 0L, 0L)).thenReturn(shard0);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage(CONTEST, 0, 100);

        assertThat(result).extracting(r -> r.get("userId"))
                .containsExactly("top-1", "top-2", "mid-1", "mid-2", "low-1");
        assertThat(result).extracting(r -> r.get("rank"))
                .containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void getLeaderboardPage_secondPageSkipsHigherShards() {
        // shard 2 has 5 users, request page 1 of size 3 → globalStart=3 falls in shard 2
        Set<ZSetOperations.TypedTuple<String>> shard2Tail = new LinkedHashSet<>();
        shard2Tail.add(new DefaultTypedTuple<>("user-d", 60_500_000.0));
        shard2Tail.add(new DefaultTypedTuple<>("user-e", 60_400_000.0));

        when(zSetOperations.zCard(SHARD_2)).thenReturn(5L);
        when(zSetOperations.reverseRangeWithScores(SHARD_2, 3L, 4L)).thenReturn(shard2Tail);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage(CONTEST, 1, 3);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).get("rank")).isEqualTo(4L);
        assertThat(result.get(1).get("rank")).isEqualTo(5L);
    }

    @Test
    void getLeaderboardPage_emptyContestReturnsEmpty() {
        // All shards empty
        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage(CONTEST, 0, 100);
        assertThat(result).isEmpty();
    }

    // --- User rank ---

    @Test
    void getUserRank_userInHighestShard() {
        when(zSetOperations.score(SHARD_2, "user-top")).thenReturn(70_000_000.0);
        when(zSetOperations.reverseRank(SHARD_2, "user-top")).thenReturn(0L);

        long rank = leaderboardService.getUserRank(CONTEST, "user-top");
        assertThat(rank).isEqualTo(1L);
    }

    @Test
    void getUserRank_userInMidShard_countsHigherShard() {
        // user is in shard 1, shard 2 has 50 users above
        when(zSetOperations.score(SHARD_2, "user-mid")).thenReturn(null);
        when(zSetOperations.score(SHARD_1, "user-mid")).thenReturn(45_000_000.0);
        when(zSetOperations.zCard(SHARD_2)).thenReturn(50L);
        when(zSetOperations.reverseRank(SHARD_1, "user-mid")).thenReturn(3L);

        long rank = leaderboardService.getUserRank(CONTEST, "user-mid");
        assertThat(rank).isEqualTo(54L); // 50 (higher shard) + 3 (local 0-idx) + 1
    }

    @Test
    void getUserRank_userInLowestShard_countsAllHigherShards() {
        when(zSetOperations.score(SHARD_2, "user-low")).thenReturn(null);
        when(zSetOperations.score(SHARD_1, "user-low")).thenReturn(null);
        when(zSetOperations.score(SHARD_0, "user-low")).thenReturn(15_000_000.0);
        when(zSetOperations.zCard(SHARD_2)).thenReturn(50L);
        when(zSetOperations.zCard(SHARD_1)).thenReturn(200L);
        when(zSetOperations.reverseRank(SHARD_0, "user-low")).thenReturn(10L);

        long rank = leaderboardService.getUserRank(CONTEST, "user-low");
        assertThat(rank).isEqualTo(261L); // 50 + 200 + 10 + 1
    }

    @Test
    void getUserRank_missingUserReturnsMinusOne() {
        when(zSetOperations.score(SHARD_2, "user-unknown")).thenReturn(null);
        when(zSetOperations.score(SHARD_1, "user-unknown")).thenReturn(null);
        when(zSetOperations.score(SHARD_0, "user-unknown")).thenReturn(null);

        long rank = leaderboardService.getUserRank(CONTEST, "user-unknown");
        assertThat(rank).isEqualTo(-1L);
    }

    // --- User score ---

    @Test
    void getUserScore_returnsCompleteEntry() {
        double zsetScore = 3_000_000_000.0 - 45; // 300pts, 45min — shard 2
        when(zSetOperations.score(SHARD_2, "user-a")).thenReturn(zsetScore);
        when(zSetOperations.reverseRank(SHARD_2, "user-a")).thenReturn(2L);

        Map<String, Object> result = leaderboardService.getUserScore(CONTEST, "user-a");

        assertThat(result.get("userId")).isEqualTo("user-a");
        assertThat(result.get("rank")).isEqualTo(3L);
        assertThat(result.get("points")).isEqualTo(300L);
        assertThat(result.get("penaltyMinutes")).isEqualTo(45L);
    }

    // --- Participant count ---

    @Test
    void getParticipantCount_sumsAcrossShards() {
        when(zSetOperations.zCard(SHARD_0)).thenReturn(795_000L);
        when(zSetOperations.zCard(SHARD_1)).thenReturn(200_000L);
        when(zSetOperations.zCard(SHARD_2)).thenReturn(5_000L);

        long count = leaderboardService.getParticipantCount(CONTEST);
        assertThat(count).isEqualTo(1_000_000L);
    }

    @Test
    void getParticipantCount_returnsZeroForEmpty() {
        long count = leaderboardService.getParticipantCount(CONTEST);
        assertThat(count).isEqualTo(0L);
    }

    // --- Score decoding ---

    @Test
    void scoreDecoding_roundTrip() {
        int points = 500;
        int penalty = 120;
        double zsetScore = (double) ((long) points * 10_000_000L - penalty);

        Set<ZSetOperations.TypedTuple<String>> entries = new LinkedHashSet<>();
        entries.add(new DefaultTypedTuple<>("user-x", zsetScore));

        when(zSetOperations.zCard(SHARD_2)).thenReturn(1L);
        when(zSetOperations.reverseRangeWithScores(SHARD_2, 0L, 0L)).thenReturn(entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage(CONTEST, 0, 100);

        assertThat(result.get(0).get("points")).isEqualTo((long) points);
        assertThat(result.get(0).get("penaltyMinutes")).isEqualTo((long) penalty);
    }
}
