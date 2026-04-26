package com.onlinejudge.leaderboard.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the Leaderboard Service read path.
 *
 * Verifies: ZREVRANGE for paginated leaderboard, ZREVRANK for user rank,
 * ZCARD for participant count, score decoding from composite ZSET format.
 */
@ExtendWith(MockitoExtension.class)
class LeaderboardServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @InjectMocks
    private LeaderboardService leaderboardService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(leaderboardService, "defaultPageSize", 100);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
    }

    @Test
    void getLeaderboardPage_returnsRankedEntries() {
        // 3 users: 300pts/45min, 300pts/60min, 200pts/0min
        Set<ZSetOperations.TypedTuple<String>> entries = new LinkedHashSet<>();
        entries.add(new DefaultTypedTuple<>("user-a", 2_999_999_955.0)); // 300pts, 45min
        entries.add(new DefaultTypedTuple<>("user-b", 2_999_999_940.0)); // 300pts, 60min
        entries.add(new DefaultTypedTuple<>("user-c", 2_000_000_000.0)); // 200pts, 0min

        when(zSetOperations.reverseRangeWithScores("leaderboard:contest-1", 0, 99))
                .thenReturn(entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage("contest-1", 0, 100);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).get("rank")).isEqualTo(1L);
        assertThat(result.get(0).get("userId")).isEqualTo("user-a");
        assertThat(result.get(1).get("rank")).isEqualTo(2L);
        assertThat(result.get(1).get("userId")).isEqualTo("user-b");
        assertThat(result.get(2).get("rank")).isEqualTo(3L);
        assertThat(result.get(2).get("userId")).isEqualTo("user-c");
    }

    @Test
    void getLeaderboardPage_decodesPointsCorrectly() {
        Set<ZSetOperations.TypedTuple<String>> entries = new LinkedHashSet<>();
        entries.add(new DefaultTypedTuple<>("user-a", 5_000_000_000.0 - 120)); // 500pts, 120min

        when(zSetOperations.reverseRangeWithScores("leaderboard:contest-1", 0, 99))
                .thenReturn(entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage("contest-1", 0, 100);

        assertThat(result.get(0).get("points")).isEqualTo(500L);
        assertThat(result.get(0).get("penaltyMinutes")).isEqualTo(120L);
    }

    @Test
    void getLeaderboardPage_returnsEmptyForNoEntries() {
        when(zSetOperations.reverseRangeWithScores("leaderboard:contest-1", 0, 99))
                .thenReturn(null);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage("contest-1", 0, 100);

        assertThat(result).isEmpty();
    }

    @Test
    void getLeaderboardPage_respectsPagination() {
        Set<ZSetOperations.TypedTuple<String>> entries = new LinkedHashSet<>();
        entries.add(new DefaultTypedTuple<>("user-c", 1_000_000_000.0));

        // Page 2 with size 50 → start=100, end=149
        when(zSetOperations.reverseRangeWithScores("leaderboard:contest-1", 100, 149))
                .thenReturn(entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage("contest-1", 2, 50);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).get("rank")).isEqualTo(101L); // rank = start + 1
    }

    @Test
    void getLeaderboardPage_capsPageSizeAt500() {
        when(zSetOperations.reverseRangeWithScores(eq("leaderboard:contest-1"), eq(0L), eq(499L)))
                .thenReturn(Collections.emptySet());

        leaderboardService.getLeaderboardPage("contest-1", 0, 1000);

        // Should cap at 500, not 1000
        verify(zSetOperations).reverseRangeWithScores("leaderboard:contest-1", 0, 499);
    }

    @Test
    void getUserRank_returnsOneIndexedRank() {
        // Redis ZREVRANK returns 0-indexed rank; service converts to 1-indexed
        when(zSetOperations.reverseRank("leaderboard:contest-1", "user-a"))
                .thenReturn(0L); // 0-indexed = rank 1

        long rank = leaderboardService.getUserRank("contest-1", "user-a");

        assertThat(rank).isEqualTo(1);
    }

    @Test
    void getUserRank_returnsMinusOneForMissingUser() {
        when(zSetOperations.reverseRank("leaderboard:contest-1", "user-unknown"))
                .thenReturn(null);

        long rank = leaderboardService.getUserRank("contest-1", "user-unknown");

        assertThat(rank).isEqualTo(-1);
    }

    @Test
    void getUserScore_returnsCompleteScoreEntry() {
        when(zSetOperations.score("leaderboard:contest-1", "user-a"))
                .thenReturn(3_000_000_000.0 - 45); // 300pts, 45min
        when(zSetOperations.reverseRank("leaderboard:contest-1", "user-a"))
                .thenReturn(2L); // 0-indexed = rank 3

        Map<String, Object> result = leaderboardService.getUserScore("contest-1", "user-a");

        assertThat(result.get("userId")).isEqualTo("user-a");
        assertThat(result.get("rank")).isEqualTo(3L);
        assertThat(result.get("points")).isEqualTo(300L);
        assertThat(result.get("penaltyMinutes")).isEqualTo(45L);
    }

    @Test
    void getParticipantCount_returnsZcardResult() {
        when(zSetOperations.zCard("leaderboard:contest-1")).thenReturn(42_000L);

        long count = leaderboardService.getParticipantCount("contest-1");

        assertThat(count).isEqualTo(42_000L);
    }

    @Test
    void getParticipantCount_returnsZeroForNull() {
        when(zSetOperations.zCard("leaderboard:contest-1")).thenReturn(null);

        long count = leaderboardService.getParticipantCount("contest-1");

        assertThat(count).isEqualTo(0L);
    }

    @Test
    void scoreDecoding_roundTrip() {
        // Encode with ScoreEncoder formula, decode with LeaderboardService
        int points = 500;
        int penalty = 120;
        double zsetScore = (double) ((long) points * 10_000_000L - penalty);

        Set<ZSetOperations.TypedTuple<String>> entries = new LinkedHashSet<>();
        entries.add(new DefaultTypedTuple<>("user-x", zsetScore));

        when(zSetOperations.reverseRangeWithScores("leaderboard:contest-1", 0, 99))
                .thenReturn(entries);

        List<Map<String, Object>> result = leaderboardService.getLeaderboardPage("contest-1", 0, 100);

        assertThat(result.get(0).get("points")).isEqualTo((long) points);
        assertThat(result.get(0).get("penaltyMinutes")).isEqualTo((long) penalty);
    }
}
