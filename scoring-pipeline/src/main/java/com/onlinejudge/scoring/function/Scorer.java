package com.onlinejudge.scoring.function;

import com.onlinejudge.scoring.model.ProblemScoreState;
import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.model.ScoringState;
import com.onlinejudge.scoring.util.ScoreEncoder;

import java.util.Optional;
import java.util.Set;

/**
 * Pure-function scoring logic, extracted from {@link ScoringFunction} so that:
 * <ul>
 *   <li>The Flink wrapper only handles state load/save and emit.</li>
 *   <li>Unit tests can verify scoring without spinning up a Flink runtime.</li>
 * </ul>
 *
 * <p>Implements the production-style event-time-aware ICPC scoring described
 * in Part 8 of the blog:
 *
 * <ul>
 *   <li>Each problem has a {@link ProblemScoreState} with the earliest seen
 *       accepted event-time and the full sorted set of wrong-attempt times.</li>
 *   <li>Penalty for a problem = (count of WAs strictly before the accepted
 *       event-time) × 20 minutes. Only counted once the problem is accepted.</li>
 *   <li>An ACCEPTED arriving with an event-time strictly earlier than the
 *       current {@code acceptedAtMs} replaces it — the earliest accepted wins,
 *       per ICPC rules.</li>
 *   <li>A WRONG-style verdict arriving with an event-time strictly before the
 *       existing {@code acceptedAtMs} triggers a correction (penalty grows).</li>
 * </ul>
 *
 * <p>{@code apply()} mutates {@code state} in place and returns an
 * {@link Optional} containing a {@link ScoreUpdate} if and only if the user's
 * cumulative totals changed — that is, the downstream Redis sink should ZADD.
 *
 * <p>Idempotency: replaying the same event is a no-op. WA times are stored in a
 * {@code TreeSet}, so a duplicate is silently absorbed; an ACCEPTED at the same
 * event-time as the current {@code acceptedAtMs} fails the {@code < acceptedAtMs}
 * check and is ignored.
 */
public class Scorer {

    private static final Set<String> WRONG_VERDICTS =
            Set.of("WRONG_ANSWER", "RUNTIME_ERROR", "TIME_LIMIT_EXCEEDED");

    /**
     * Applies a verdict event to the user's scoring state, mutating it in place.
     *
     * @return a {@link ScoreUpdate} when the user's totalScore or totalPenaltyMinutes
     *         changed in a downstream-visible way; otherwise {@link Optional#empty()}.
     */
    public static Optional<ScoreUpdate> apply(ScoringState state,
                                              String userId,
                                              String contestId,
                                              String problemId,
                                              String result,
                                              int points,
                                              long gatewayTsMs) {

        ProblemScoreState p = state.problems.computeIfAbsent(problemId, k -> new ProblemScoreState());

        int oldPoints  = p.contributedPoints();
        int oldPenalty = p.currentPenaltyMinutes();

        if ("ACCEPTED".equals(result)) {
            if (gatewayTsMs < p.acceptedAtMs) {
                // First or earlier acceptance — record it.
                p.acceptedAtMs = gatewayTsMs;
                p.points = points;
            }
        } else if (WRONG_VERDICTS.contains(result)) {
            // Always record the WA event-time. Penalty recomputation handles whether
            // it falls before the accepted time.
            p.wrongAttemptTimes.add(gatewayTsMs);
        }

        int newPoints  = p.contributedPoints();
        int newPenalty = p.currentPenaltyMinutes();

        int pointsDelta  = newPoints  - oldPoints;
        int penaltyDelta = newPenalty - oldPenalty;

        if (pointsDelta == 0 && penaltyDelta == 0) {
            return Optional.empty();
        }

        state.totalScore           += pointsDelta;
        state.totalPenaltyMinutes  += penaltyDelta;

        double zsetScore = ScoreEncoder.encode(state.totalScore, state.totalPenaltyMinutes);
        return Optional.of(new ScoreUpdate(
                userId, contestId,
                state.totalScore, state.totalPenaltyMinutes,
                zsetScore, gatewayTsMs));
    }
}
