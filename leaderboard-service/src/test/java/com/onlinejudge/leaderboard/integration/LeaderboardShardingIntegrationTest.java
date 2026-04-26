package com.onlinejudge.leaderboard.integration;

import com.onlinejudge.leaderboard.sharding.ScoreRangeShardRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: score-range sharding with simulated leaderboard operations.
 *
 * Simulates the full write-and-read flow described in Parts 7 and 8:
 * 1. Flink emits ScoreUpdates → sharded across Redis nodes by score range
 * 2. Leaderboard Service reads from the correct shard(s) for queries
 * 3. Global rank is computed across shards
 *
 * Uses in-memory data structures to simulate Redis ZSETs (no actual Redis).
 * This tests the sharding ALGORITHM, not Redis connectivity.
 *
 * This is the test that proves score-range sharding produces correct global
 * rankings — the alternative (user_id hash sharding) would make this test
 * impossible without scanning every shard.
 */
class LeaderboardShardingIntegrationTest {

    private ScoreRangeShardRouter router;

    /**
     * Simulated Redis ZSET per shard.
     * Key: shard index → Map of userId → zsetScore (sorted by score descending)
     */
    private final Map<Integer, TreeMap<Double, List<String>>> shards = new HashMap<>();

    @BeforeEach
    void setUp() {
        router = ScoreRangeShardRouter.defaultIcpcRouter();
        for (int i = 0; i < router.shardCount(); i++) {
            shards.put(i, new TreeMap<>(Comparator.reverseOrder()));
        }
    }

    private void addScore(String userId, double zsetScore) {
        int shard = router.shardForScore(zsetScore);
        shards.get(shard).computeIfAbsent(zsetScore, k -> new ArrayList<>()).add(userId);
    }

    private long getShardSize(int shardIndex) {
        return shards.get(shardIndex).values().stream().mapToInt(List::size).sum();
    }

    /** Gets the 0-indexed reverse rank within a shard. */
    private long getLocalReverseRank(int shardIndex, String userId, double zsetScore) {
        long rank = 0;
        for (Map.Entry<Double, List<String>> entry : shards.get(shardIndex).entrySet()) {
            if (entry.getKey() > zsetScore) {
                rank += entry.getValue().size();
            } else if (entry.getKey() == zsetScore) {
                int idx = entry.getValue().indexOf(userId);
                return rank + idx;
            }
        }
        return rank;
    }

    @Test
    void fullContest_1000users_correctGlobalRanking() {
        // Simulate 1000 users with various scores
        Random rng = new Random(42);
        List<UserScore> allUsers = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            String userId = "user-" + i;
            int solved = rng.nextInt(8) * 100; // 0-700 points
            int penalty = rng.nextInt(300);
            double zset = (double) ((long) solved * 10_000_000L - penalty);

            addScore(userId, zset);
            allUsers.add(new UserScore(userId, solved, penalty, zset));
        }

        // Sort all users by ICPC rules (brute force, for verification)
        allUsers.sort((a, b) -> {
            if (a.zsetScore != b.zsetScore) return Double.compare(b.zsetScore, a.zsetScore);
            return a.userId.compareTo(b.userId);
        });

        // Verify shard distribution
        assertThat(getShardSize(0)).isGreaterThan(0); // 0-2 solved
        assertThat(getShardSize(1)).isGreaterThan(0); // 3-5 solved
        assertThat(getShardSize(2)).isGreaterThan(0); // 6+ solved

        // Verify global rank computation for 10 random users
        for (int sample = 0; sample < 10; sample++) {
            int idx = rng.nextInt(1000);
            UserScore user = allUsers.get(idx);
            int expectedGlobalRank = idx + 1; // 1-indexed

            // Compute global rank using the sharding algorithm
            int userShard = router.shardForScore(user.zsetScore);
            long localRank = getLocalReverseRank(userShard, user.userId, user.zsetScore);

            List<Integer> higherShards = router.higherShardIndices(user.zsetScore);
            List<Long> higherCards = higherShards.stream()
                    .map(this::getShardSize)
                    .toList();

            long computedRank = router.computeGlobalRank(localRank, higherCards);

            // The computed rank should match the brute-force rank
            // (within tolerance of ties — users with identical scores have arbitrary order)
            assertThat(computedRank)
                    .as("Global rank for %s (solved=%d, penalty=%d)",
                            user.userId, user.solved, user.penalty)
                    .isBetween((long) expectedGlobalRank - 5, (long) expectedGlobalRank + 5);
        }
    }

    @Test
    void topN_query_acrossShards() {
        // Shard 2 (6+ solved): 3 users
        addScore("alice",  70_000_000 - 45);  // 7 solved, 45 penalty
        addScore("bob",    60_000_000 - 20);  // 6 solved, 20 penalty
        addScore("carol",  60_000_000 - 100); // 6 solved, 100 penalty

        // Shard 1 (3-5 solved): 2 users
        addScore("dave",   50_000_000 - 30);  // 5 solved, 30 penalty
        addScore("eve",    30_000_000 - 0);   // 3 solved, 0 penalty

        // Shard 0 (0-2 solved): 1 user
        addScore("frank",  10_000_000 - 60);  // 1 solved, 60 penalty

        // Top 4 query: should span shard 2 (3 entries) + shard 1 (1 entry)
        List<String> top4 = getTopN(4);
        assertThat(top4).containsExactly("alice", "bob", "carol", "dave");
    }

    @Test
    void globalRank_userInLowestShard() {
        addScore("top-user", 80_000_000 - 10);   // shard 2
        addScore("mid-user", 40_000_000 - 50);   // shard 1

        // 50 users in shard 0
        for (int i = 0; i < 50; i++) {
            addScore("low-user-" + i, 10_000_000 - i);
        }

        // low-user-25's global rank should account for shard 2 + shard 1
        double lowUserScore = 10_000_000 - 25;
        int userShard = router.shardForScore(lowUserScore);
        assertThat(userShard).isEqualTo(0);

        long localRank = getLocalReverseRank(0, "low-user-25", lowUserScore);
        List<Long> higherCards = List.of(getShardSize(2), getShardSize(1));
        long globalRank = router.computeGlobalRank(localRank, higherCards);

        // 1 (shard 2) + 1 (shard 1) + 25 (shard 0 local rank) + 1 = 28
        assertThat(globalRank).isEqualTo(28);
    }

    @Test
    void emptyShards_globalRankStillCorrect() {
        // All users in shard 0 (0-2 solved)
        addScore("user-a", 20_000_000 - 10);
        addScore("user-b", 10_000_000 - 20);
        addScore("user-c", 5_000_000 - 0);

        // Shards 1 and 2 are empty
        assertThat(getShardSize(1)).isEqualTo(0);
        assertThat(getShardSize(2)).isEqualTo(0);

        // user-a should be rank 1
        long localRank = getLocalReverseRank(0, "user-a", 20_000_000 - 10);
        List<Long> higherCards = List.of(getShardSize(2), getShardSize(1));
        long globalRank = router.computeGlobalRank(localRank, higherCards);
        assertThat(globalRank).isEqualTo(1);
    }

    @Test
    void writeRouting_deterministic() {
        // Same score always routes to same shard
        double score = 45_000_000 - 60;
        int shard1 = router.shardForScore(score);
        int shard2 = router.shardForScore(score);
        int shard3 = router.shardForScore(score);

        assertThat(shard1).isEqualTo(shard2).isEqualTo(shard3).isEqualTo(1);
    }

    // --- Helpers ---

    private List<String> getTopN(int n) {
        List<String> result = new ArrayList<>();
        // Read from highest shard to lowest
        for (int shard = router.shardCount() - 1; shard >= 0 && result.size() < n; shard--) {
            for (List<String> users : shards.get(shard).values()) {
                for (String user : users) {
                    if (result.size() >= n) break;
                    result.add(user);
                }
            }
        }
        return result;
    }

    record UserScore(String userId, int solved, int penalty, double zsetScore) {}
}
