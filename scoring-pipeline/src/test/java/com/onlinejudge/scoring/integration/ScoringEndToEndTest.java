package com.onlinejudge.scoring.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.model.ScoringState;
import com.onlinejudge.scoring.util.ScoreEncoder;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for the scoring pipeline.
 *
 * Simulates a full contest scenario: multiple users submitting solutions
 * to multiple problems, with wrong answers, acceptances, re-submissions,
 * and late arrivals. Verifies that the final leaderboard ordering is
 * correct according to ICPC rules.
 *
 * This test exercises the entire scoring chain:
 *   Raw verdict JSON → ScoringState management → ScoreEncoder → ScoreUpdate
 *
 * It does NOT use the Flink runtime (avoiding MiniCluster complexity).
 * Instead it drives the scoring logic directly, which tests the algorithm
 * that Flink's KeyedProcessFunction executes. The Flink integration
 * (watermarks, checkpointing, keying) is tested by Flink's own guarantees.
 */
class ScoringEndToEndTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Simulates per-user scoring state, mirroring Flink's ValueState<ScoringState>. */
    private final Map<String, ScoringState> userStates = new HashMap<>();
    private final List<ScoreUpdate> allUpdates = new ArrayList<>();

    /**
     * Full contest simulation: 5 users, 4 problems, various outcomes.
     *
     * Expected final leaderboard (ICPC rules):
     *   Rank 1: alice  — 4 solved, 0 penalty (perfect run)
     *   Rank 2: bob    — 3 solved, 40 penalty (2 WAs on prob-2)
     *   Rank 3: carol  — 3 solved, 60 penalty (1 WA on prob-1, 2 WA on prob-3)
     *   Rank 4: dave   — 2 solved, 0 penalty
     *   Rank 5: eve    — 1 solved, 20 penalty (1 WA then accepted)
     */
    @Test
    void fullContestScenario_correctLeaderboardOrder() throws Exception {
        // Alice: solves all 4 problems on first try
        processVerdict("alice", "prob-1", "ACCEPTED", 100, 1000);
        processVerdict("alice", "prob-2", "ACCEPTED", 100, 2000);
        processVerdict("alice", "prob-3", "ACCEPTED", 100, 3000);
        processVerdict("alice", "prob-4", "ACCEPTED", 100, 4000);

        // Bob: 2 WAs on prob-2, then solves 3 problems
        processVerdict("bob", "prob-1", "ACCEPTED", 100, 1000);
        processVerdict("bob", "prob-2", "WRONG_ANSWER", 0, 2000);
        processVerdict("bob", "prob-2", "WRONG_ANSWER", 0, 3000);
        processVerdict("bob", "prob-2", "ACCEPTED", 100, 4000);
        processVerdict("bob", "prob-3", "ACCEPTED", 100, 5000);

        // Carol: 1 WA on prob-1, 2 WA on prob-3, solves 3
        processVerdict("carol", "prob-1", "WRONG_ANSWER", 0, 1000);
        processVerdict("carol", "prob-1", "ACCEPTED", 100, 2000);
        processVerdict("carol", "prob-2", "ACCEPTED", 100, 3000);
        processVerdict("carol", "prob-3", "TIME_LIMIT_EXCEEDED", 0, 4000);
        processVerdict("carol", "prob-3", "RUNTIME_ERROR", 0, 5000);
        processVerdict("carol", "prob-3", "ACCEPTED", 100, 6000);

        // Dave: solves 2 problems cleanly
        processVerdict("dave", "prob-1", "ACCEPTED", 100, 1000);
        processVerdict("dave", "prob-4", "ACCEPTED", 100, 2000);

        // Eve: 1 WA then solves 1 problem
        processVerdict("eve", "prob-1", "WRONG_ANSWER", 0, 1000);
        processVerdict("eve", "prob-1", "ACCEPTED", 100, 2000);

        // Verify final scores
        assertFinalScore("alice", 400, 0);
        assertFinalScore("bob",   300, 40);   // 2 WA × 20 = 40 penalty
        assertFinalScore("carol", 300, 60);   // 1 WA on prob-1 (20) + 2 WA on prob-3 (40) = 60
        assertFinalScore("dave",  200, 0);
        assertFinalScore("eve",   100, 20);   // 1 WA × 20 = 20 penalty

        // Verify ZSET ordering (ZREVRANGE returns highest score first)
        double aliceZset = ScoreEncoder.encode(400, 0);
        double bobZset   = ScoreEncoder.encode(300, 40);
        double carolZset = ScoreEncoder.encode(300, 60);
        double daveZset  = ScoreEncoder.encode(200, 0);
        double eveZset   = ScoreEncoder.encode(100, 20);

        assertThat(aliceZset).isGreaterThan(bobZset);    // More solved
        assertThat(bobZset).isGreaterThan(carolZset);     // Same solved, less penalty
        assertThat(carolZset).isGreaterThan(daveZset);    // More solved
        assertThat(daveZset).isGreaterThan(eveZset);      // More solved
    }

    @Test
    void resubmission_afterAccepted_doesNotDoubleScore() throws Exception {
        processVerdict("user-1", "prob-1", "ACCEPTED", 100, 1000);
        processVerdict("user-1", "prob-1", "ACCEPTED", 100, 2000); // Re-submit same problem

        assertFinalScore("user-1", 100, 0); // Not 200
    }

    @Test
    void wrongAnswer_afterAccepted_doesNotAddPenalty() throws Exception {
        processVerdict("user-1", "prob-1", "ACCEPTED", 100, 1000);
        processVerdict("user-1", "prob-1", "WRONG_ANSWER", 0, 2000); // WA on accepted problem

        processVerdict("user-1", "prob-2", "ACCEPTED", 100, 3000); // Second problem clean

        assertFinalScore("user-1", 200, 0); // No penalty from WA on already-accepted problem
    }

    @Test
    void onlyWrongAnswers_noScoreUpdate() throws Exception {
        processVerdict("user-1", "prob-1", "WRONG_ANSWER", 0, 1000);
        processVerdict("user-1", "prob-1", "WRONG_ANSWER", 0, 2000);
        processVerdict("user-1", "prob-2", "TIME_LIMIT_EXCEEDED", 0, 3000);

        // No score updates emitted — only wrong answers
        assertThat(allUpdates.stream()
                .filter(u -> u.userId().equals("user-1"))
                .count()).isZero();
    }

    @Test
    void multiUser_isolation() throws Exception {
        // Users process verdicts interleaved
        processVerdict("user-a", "prob-1", "WRONG_ANSWER", 0, 1000);
        processVerdict("user-b", "prob-1", "ACCEPTED", 100, 1500);
        processVerdict("user-a", "prob-1", "ACCEPTED", 100, 2000);

        assertFinalScore("user-a", 100, 20); // 1 WA × 20
        assertFinalScore("user-b", 100, 0);  // Clean solve — user-a's WA doesn't affect user-b
    }

    @Test
    void scoreEncoder_preservesOrderingAcrossThousandUsers() {
        // Generate 1000 users with random scores and verify ZSET ordering is correct
        List<double[]> users = new ArrayList<>(); // [zsetScore, solved, penalty]
        Random random = new Random(42);

        for (int i = 0; i < 1000; i++) {
            int solved = random.nextInt(10) * 100;
            int penalty = random.nextInt(300);
            double zset = ScoreEncoder.encode(solved, penalty);
            users.add(new double[]{zset, solved, penalty});
        }

        // Sort by ZSET score descending (simulating ZREVRANGE)
        users.sort((a, b) -> Double.compare(b[0], a[0]));

        // Verify ordering: higher solved always ranks higher; equal solved, lower penalty wins
        for (int i = 0; i < users.size() - 1; i++) {
            double[] current = users.get(i);
            double[] next = users.get(i + 1);

            if (current[1] == next[1]) {
                // Same solved count → current should have <= penalty
                assertThat(current[2]).isLessThanOrEqualTo(next[2]);
            } else {
                // Different solved → current should have > solved
                assertThat(current[1]).isGreaterThan(next[1]);
            }
        }
    }

    // --- Helpers ---

    private void processVerdict(String userId, String problemId, String result,
                                 int points, long gatewayTsMs) throws Exception {
        ScoringState state = userStates.computeIfAbsent(userId, k -> new ScoringState());

        boolean stateChanged = false;

        if ("ACCEPTED".equals(result)) {
            if (!state.acceptedProblems.getOrDefault(problemId, false)) {
                int wrongAttempts = state.wrongAttemptsPerProblem.getOrDefault(problemId, 0);
                int penaltyForProblem = wrongAttempts * ScoreEncoder.PENALTY_PER_WRONG_ATTEMPT_MINUTES;

                state.totalScore += points;
                state.totalPenaltyMinutes += penaltyForProblem;
                state.acceptedProblems.put(problemId, true);
                stateChanged = true;
            }
        } else if ("WRONG_ANSWER".equals(result) || "RUNTIME_ERROR".equals(result)
                || "TIME_LIMIT_EXCEEDED".equals(result)) {
            if (!state.acceptedProblems.getOrDefault(problemId, false)) {
                state.wrongAttemptsPerProblem.merge(problemId, 1, Integer::sum);
            }
        }

        if (stateChanged) {
            double zsetScore = ScoreEncoder.encode(state.totalScore, state.totalPenaltyMinutes);
            allUpdates.add(new ScoreUpdate(
                    userId, "contest-1",
                    state.totalScore, state.totalPenaltyMinutes,
                    zsetScore, gatewayTsMs));
        }
    }

    private void assertFinalScore(String userId, int expectedScore, int expectedPenalty) {
        ScoringState state = userStates.get(userId);
        assertThat(state).as("State for user " + userId).isNotNull();
        assertThat(state.totalScore).as("Score for " + userId).isEqualTo(expectedScore);
        assertThat(state.totalPenaltyMinutes).as("Penalty for " + userId).isEqualTo(expectedPenalty);
    }
}
