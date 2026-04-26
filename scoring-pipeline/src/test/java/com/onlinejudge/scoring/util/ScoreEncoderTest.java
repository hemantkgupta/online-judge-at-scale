package com.onlinejudge.scoring.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the composite score encoding.
 *
 * Formula: score_float = (totalPoints * 10_000_000) - penaltyMinutes
 *
 * Verifies:
 * - Higher points always rank higher (regardless of penalty)
 * - At equal points, fewer penalty minutes rank higher
 * - Specific examples from the ScoreEncoder Javadoc
 * - The encoding produces correct ZREVRANGE ordering
 */
class ScoreEncoderTest {

    @Test
    void encode_basicExample() {
        // 300 pts, 45 min penalty -> 300 * 10_000_000 - 45 = 2_999_999_955
        double score = ScoreEncoder.encode(300, 45);
        assertThat(score).isEqualTo(2_999_999_955.0);
    }

    @Test
    void encode_higherPointsAlwaysRankHigher() {
        // 6 solved with max penalty should still outrank 5 solved with no penalty
        double sixSolved = ScoreEncoder.encode(600, 9999);
        double fiveSolved = ScoreEncoder.encode(500, 0);

        assertThat(sixSolved).isGreaterThan(fiveSolved);
    }

    @Test
    void encode_fewerPenaltyMinutesRankHigher_atEqualPoints() {
        double lessPenalty = ScoreEncoder.encode(300, 45);
        double morePenalty = ScoreEncoder.encode(300, 60);

        // ZREVRANGE returns higher score first
        // Less penalty = higher score = better rank
        assertThat(lessPenalty).isGreaterThan(morePenalty);
    }

    @Test
    void encode_zeroPointsZeroPenalty() {
        double score = ScoreEncoder.encode(0, 0);
        assertThat(score).isEqualTo(0.0);
    }

    @Test
    void encode_zeroPointsWithPenalty() {
        // 0 solved but penalty exists (wrong attempts before any acceptance)
        // This produces a negative score, which is fine — they rank below 0-solved-no-penalty
        double score = ScoreEncoder.encode(0, 40);
        assertThat(score).isEqualTo(-40.0);
    }

    @Test
    void encode_orderingIsCorrectForZrevrange() {
        // Simulate a leaderboard: 5 users with different scores
        double user1 = ScoreEncoder.encode(500, 100); // 5 solved, 100 min
        double user2 = ScoreEncoder.encode(500, 120); // 5 solved, 120 min (worse)
        double user3 = ScoreEncoder.encode(400, 0);   // 4 solved, 0 min
        double user4 = ScoreEncoder.encode(300, 45);  // 3 solved, 45 min
        double user5 = ScoreEncoder.encode(100, 20);  // 1 solved, 20 min

        // ZREVRANGE ordering (highest score first):
        assertThat(user1).isGreaterThan(user2); // Same solved, less penalty wins
        assertThat(user2).isGreaterThan(user3); // 5 solved always beats 4 solved
        assertThat(user3).isGreaterThan(user4);
        assertThat(user4).isGreaterThan(user5);
    }

    @Test
    void encode_multiplierSeparatesPointTiers() {
        // The 10M multiplier ensures no amount of penalty can make a higher-point
        // user rank below a lower-point user
        double highPoints = ScoreEncoder.encode(200, 9_999_999); // Absurd penalty
        double lowPoints = ScoreEncoder.encode(100, 0);          // Zero penalty

        // Even with almost 10M minutes of penalty, 200 points > 100 points
        assertThat(highPoints).isGreaterThan(lowPoints);
    }

    @Test
    void penaltyPerWrongAttemptConstant() {
        // Verify the constant matches ICPC rules
        assertThat(ScoreEncoder.PENALTY_PER_WRONG_ATTEMPT_MINUTES).isEqualTo(20);
    }

    @Test
    void encode_icpcScenario_twoUsersWithDifferentPenalties() {
        // User A: 5 problems, each first try (0 wrong attempts), penalty = 0
        double userA = ScoreEncoder.encode(500, 0);

        // User B: 5 problems, 2 wrong attempts on one problem, penalty = 2*20 = 40
        double userB = ScoreEncoder.encode(500, 40);

        // User A ranks higher (less penalty at equal solved count)
        assertThat(userA).isGreaterThan(userB);
        assertThat(userA - userB).isEqualTo(40.0); // Exact penalty difference
    }

    @Test
    void encode_docExample_300pts45min() {
        assertThat(ScoreEncoder.encode(300, 45)).isEqualTo(2_999_999_955.0);
    }

    @Test
    void encode_docExample_300pts60min() {
        assertThat(ScoreEncoder.encode(300, 60)).isEqualTo(2_999_999_940.0);
    }
}
