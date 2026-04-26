package com.onlinejudge.scoring.util;

/**
 * Encodes composite contest scoring into a single Redis ZSET float.
 *
 * Formula: score_float = (totalPoints * 10_000_000) - penaltyMinutes
 *
 * This encodes correct rank ordering into one comparable double:
 * - Higher points always rank higher (10M multiplier separates point tiers)
 * - At equal points, fewer penalty minutes rank higher (subtraction)
 *
 * Example:
 *   300 pts, 45 min penalty -> 300 * 10_000_000 - 45 = 2_999_999_955
 *   300 pts, 60 min penalty -> 300 * 10_000_000 - 60 = 2_999_999_940
 *
 * ZREVRANGE returns 2_999_999_955 first (fewer penalties = better rank). Correct.
 *
 * Constraint: totalPoints must be < 10_000_000 / 10_000 = 1000 for this
 * encoding to hold. Adjust the multiplier for contests with higher point caps.
 */
public class ScoreEncoder {

    private static final long SCORE_MULTIPLIER = 10_000_000L;

    public static double encode(int totalPoints, int penaltyMinutes) {
        return (double) ((long) totalPoints * SCORE_MULTIPLIER - penaltyMinutes);
    }

    public static final int PENALTY_PER_WRONG_ATTEMPT_MINUTES = 20;
}
