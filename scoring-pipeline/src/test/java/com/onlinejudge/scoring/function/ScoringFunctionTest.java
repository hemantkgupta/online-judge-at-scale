package com.onlinejudge.scoring.function;

import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.model.ScoringState;
import com.onlinejudge.scoring.util.ScoreEncoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the production-style event-time-aware scoring logic.
 *
 * <p>These tests exercise {@link Scorer#apply} directly — the pure function
 * extracted from {@link ScoringFunction} so scoring rules can be verified
 * without spinning up a Flink runtime.
 *
 * <p>Behaviors verified:
 * <ul>
 *   <li>ACCEPTED with no prior WA → emit with no penalty.</li>
 *   <li>WA alone → no emit (the problem isn't accepted yet).</li>
 *   <li>WA, WA, AC (in order) → one emit with 2×20 min penalty.</li>
 *   <li>AC, AC for same problem (with a <em>later</em> event-time) → second is ignored.</li>
 *   <li>WA arriving <em>after</em> the AC but with a strictly <em>earlier</em>
 *       event-time → <strong>correction</strong>: penalty grows, ScoreUpdate emitted.</li>
 *   <li>WA arriving with event-time after the AC → not counted (penalty unchanged).</li>
 *   <li>Multiple problems accumulate correctly.</li>
 *   <li>Composite ZSET score is correctly encoded and ordered.</li>
 *   <li>Replaying the same event twice is a no-op.</li>
 * </ul>
 */
class ScoringFunctionTest {

    private ScoringState state;
    private List<ScoreUpdate> emitted;

    @BeforeEach
    void setUp() {
        state = new ScoringState();
        emitted = new ArrayList<>();
    }

    /** Test helper: apply an event via the real Scorer and capture any emit. */
    private void processEvent(String userId, String problemId, String contestId,
                              String result, int points, long gatewayTsMs) {
        Optional<ScoreUpdate> update = Scorer.apply(
                state, userId, contestId, problemId, result, points, gatewayTsMs);
        update.ifPresent(emitted::add);
    }

    // --- In-order events (legacy expectations) ---

    @Test
    void accepted_emitsScoreUpdate() {
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);

        assertThat(emitted).hasSize(1);
        ScoreUpdate u = emitted.get(0);
        assertThat(u.userId()).isEqualTo("user-1");
        assertThat(u.contestId()).isEqualTo("contest-1");
        assertThat(u.totalScore()).isEqualTo(100);
        assertThat(u.penaltyMinutes()).isEqualTo(0);
    }

    @Test
    void wrongAnswer_doesNotEmitScoreUpdate() {
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 1000L);

        assertThat(emitted).isEmpty();
        // But the WA event-time IS recorded — so a later AC will see it.
        assertThat(state.problems.get("prob-1").wrongAttemptTimes).containsExactly(1000L);
    }

    @Test
    void accepted_afterWrongAttempts_includesPenalty() {
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 1000L);
        processEvent("user-1", "prob-1", "contest-1", "RUNTIME_ERROR", 0, 2000L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 3000L);

        assertThat(emitted).hasSize(1);
        ScoreUpdate u = emitted.get(0);
        assertThat(u.totalScore()).isEqualTo(100);
        assertThat(u.penaltyMinutes()).isEqualTo(2 * ScoreEncoder.PENALTY_PER_WRONG_ATTEMPT_MINUTES);
    }

    @Test
    void resubmission_afterAccepted_isIgnored() {
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);
        // Second ACCEPTED at a LATER time — ignored (first acceptance wins).
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 2000L);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).totalScore()).isEqualTo(100);
    }

    @Test
    void multipleProblems_accumulateCorrectly() {
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);
        processEvent("user-1", "prob-2", "contest-1", "WRONG_ANSWER", 0, 2000L);
        processEvent("user-1", "prob-2", "contest-1", "ACCEPTED", 100, 3000L);

        assertThat(emitted).hasSize(2);
        ScoreUpdate last = emitted.get(1);
        assertThat(last.totalScore()).isEqualTo(200);
        assertThat(last.penaltyMinutes()).isEqualTo(20);
    }

    @Test
    void wrongAnswer_afterAccepted_butLaterEventTime_doesNotCountPenalty() {
        // AC at T=1000, then a WA arrives later but with event-time AFTER the AC.
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 2000L);

        // No re-emit: WA at T=2000 is after AC at T=1000, so penalty is unaffected.
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).penaltyMinutes()).isEqualTo(0);
        // But it IS recorded; just doesn't contribute.
        assertThat(state.problems.get("prob-1").wrongAttemptTimes).containsExactly(2000L);
    }

    @Test
    void timeLimitExceeded_countsAsWrongAttempt() {
        processEvent("user-1", "prob-1", "contest-1", "TIME_LIMIT_EXCEEDED", 0, 1000L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 2000L);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).penaltyMinutes()).isEqualTo(20);
    }

    @Test
    void zsetScore_isCorrectlyEncoded() {
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 1000L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 500, 2000L);

        double expected = ScoreEncoder.encode(500, 20);
        assertThat(emitted.get(0).zsetScore()).isEqualTo(expected);
    }

    @Test
    void nullContestId_defaultsAtFlinkLayer() {
        // The Flink layer substitutes "global" when contestId is null in the payload;
        // the Scorer just takes the contestId argument verbatim.
        processEvent("user-1", "prob-1", "global", "ACCEPTED", 100, 1000L);
        assertThat(emitted.get(0).contestId()).isEqualTo("global");
    }

    @Test
    void multipleWrongAttempts_accumulatePenalty() {
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 100L);
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 200L);
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 300L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 400L);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).penaltyMinutes()).isEqualTo(60);
    }

    @Test
    void zsetScore_correctOrderingAfterMultipleProblems() {
        // user-1: 100 points, 0 penalty
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);
        double scoreA = emitted.get(0).zsetScore();

        setUp();
        // user-2: 100 points, 20 penalty (worse rank than user-1)
        processEvent("user-2", "prob-1", "contest-1", "WRONG_ANSWER", 0, 1000L);
        processEvent("user-2", "prob-1", "contest-1", "ACCEPTED", 100, 2000L);
        double scoreB = emitted.get(0).zsetScore();

        assertThat(scoreA).isGreaterThan(scoreB); // Fewer penalties = higher ZSET score = better rank
    }

    // --- Event-time correction (the production gap that was closed) ---

    @Test
    void lateWrongAnswer_beforeAcceptedEventTime_triggersCorrection() {
        // First two events arrive in order, but the third (a late WA) carries an
        // event-time that is BEFORE the previously-seen accepted event-time.
        // The penalty must be corrected upward and a fresh ScoreUpdate emitted.

        // T=1000: AC, no prior WA → score = 100, penalty = 0.
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1000L);
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).penaltyMinutes()).isEqualTo(0);

        // T=500 (late!): WA whose event-time precedes the AC.
        // The Scorer must record it AND recompute penalty → +20 min.
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 500L);

        assertThat(emitted).hasSize(2);
        ScoreUpdate correction = emitted.get(1);
        assertThat(correction.totalScore()).isEqualTo(100);          // Points unchanged
        assertThat(correction.penaltyMinutes()).isEqualTo(20);       // +20 from the late WA
    }

    @Test
    void earlierAcceptedArrivesLast_replacesAcceptedTimeAndRecomputesPenalty() {
        // T=1000: WA
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 1000L);
        // T=2000: WA
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 2000L);
        // T=3000: AC → penalty = 2×20 = 40
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 3000L);
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).penaltyMinutes()).isEqualTo(40);

        // Later, an earlier AC arrives — at T=1500 (between the two WAs).
        // ICPC rule: earliest acceptance wins. After the correction:
        //   acceptedAtMs = 1500
        //   WAs with t < 1500: just the T=1000 one
        //   penalty = 1×20 = 20
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 1500L);

        assertThat(emitted).hasSize(2);
        ScoreUpdate correction = emitted.get(1);
        assertThat(correction.totalScore()).isEqualTo(100);
        assertThat(correction.penaltyMinutes()).isEqualTo(20);
    }

    @Test
    void replayingSameEvent_isIdempotent() {
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 100L);
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 200L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 300L);

        int emitsBefore = emitted.size();
        int scoreBefore = state.totalScore;
        int penaltyBefore = state.totalPenaltyMinutes;

        // Replay each event verbatim — none should change the state or emit.
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 100L);
        processEvent("user-1", "prob-1", "contest-1", "WRONG_ANSWER", 0, 200L);
        processEvent("user-1", "prob-1", "contest-1", "ACCEPTED", 100, 300L);

        assertThat(emitted).hasSize(emitsBefore);
        assertThat(state.totalScore).isEqualTo(scoreBefore);
        assertThat(state.totalPenaltyMinutes).isEqualTo(penaltyBefore);
    }
}
