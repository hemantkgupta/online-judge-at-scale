package com.onlinejudge.scoring.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-user scoring state maintained by Flink's KeyedProcessFunction.
 *
 * <p>Stored in Flink's managed state (RocksDB backend in production).
 * Checkpointed to durable storage via ABS for exactly-once recovery.
 *
 * <p>Two top-level aggregates ({@link #totalScore}, {@link #totalPenaltyMinutes})
 * are derived from the per-problem {@link ProblemScoreState} entries — every
 * mutation to a {@code ProblemScoreState} must update the aggregates by the
 * delta. See {@code Scorer} for the canonical update logic.
 */
public class ScoringState implements Serializable {

    /** Running total points across all accepted problems. */
    public int totalScore = 0;

    /** Running total penalty minutes (wrong-attempt count for each accepted problem × 20). */
    public int totalPenaltyMinutes = 0;

    /** Per-problem detail: accepted event-time, wrong-attempt times, points. */
    public Map<String, ProblemScoreState> problems = new HashMap<>();
}
