package com.onlinejudge.scoring.model;

import com.onlinejudge.scoring.util.ScoreEncoder;

import java.io.Serializable;
import java.util.TreeSet;

/**
 * Per-problem scoring state held inside {@link ScoringState}.
 *
 * <p>Stores the <em>event-time</em> of the accepted submission (if any) and the
 * sorted set of wrong-attempt event-times for the same problem. Penalty is
 * computed from only those wrong attempts whose event-time is strictly before
 * the accepted submission's event-time.
 *
 * <p>This is the production-style design from Part 8 of the blog. It is correct
 * under out-of-order verdict streams (e.g. cross-region Cluster-Linking lag),
 * unlike a naive in-order count. If a late WRONG_ANSWER arrives <em>after</em>
 * an ACCEPTED with an <em>earlier</em> event-time, recomputing the penalty
 * adjusts the user's score upward to reflect the corrected attempt history.
 */
public class ProblemScoreState implements Serializable {

    /** Event-time of the earliest seen ACCEPTED verdict for this problem.
     *  {@link Long#MAX_VALUE} means "not yet accepted." */
    public long acceptedAtMs = Long.MAX_VALUE;

    /** Points for the accepted submission. Zero until {@link #acceptedAtMs} is set. */
    public int points = 0;

    /** All WRONG-style attempt event-times we've seen for this problem, sorted ascending. */
    public TreeSet<Long> wrongAttemptTimes = new TreeSet<>();

    public boolean isAccepted() {
        return acceptedAtMs != Long.MAX_VALUE;
    }

    /**
     * Penalty in minutes for this problem, counting only WAs that occurred
     * <em>before</em> the accepted time. Returns 0 if not yet accepted.
     */
    public int currentPenaltyMinutes() {
        if (!isAccepted()) return 0;
        int waBeforeAc = 0;
        for (Long t : wrongAttemptTimes) {
            if (t < acceptedAtMs) waBeforeAc++;
            else break;   // TreeSet is sorted ascending — no more WAs in scope.
        }
        return waBeforeAc * ScoreEncoder.PENALTY_PER_WRONG_ATTEMPT_MINUTES;
    }

    /** Points contributed to the user's total. Zero until accepted. */
    public int contributedPoints() {
        return isAccepted() ? points : 0;
    }
}
