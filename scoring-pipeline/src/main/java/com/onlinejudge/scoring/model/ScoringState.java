package com.onlinejudge.scoring.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-user scoring state maintained by Flink's KeyedProcessFunction.
 *
 * Stored in Flink's managed state (RocksDB backend in production).
 * Checkpointed to durable storage via ABS for exactly-once recovery.
 */
public class ScoringState implements Serializable {

    /** Running total points for this user across all accepted problems. */
    public int totalScore = 0;

    /** Total penalty minutes accumulated (wrong attempts x 20 min each). */
    public int totalPenaltyMinutes = 0;

    /** Number of wrong attempts per problem (for penalty calculation). */
    public Map<String, Integer> wrongAttemptsPerProblem = new HashMap<>();

    /** Problems already accepted (to prevent double-scoring on re-submission). */
    public Map<String, Boolean> acceptedProblems = new HashMap<>();
}
