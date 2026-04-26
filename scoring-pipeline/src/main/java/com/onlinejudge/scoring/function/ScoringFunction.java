package com.onlinejudge.scoring.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.model.ScoringState;
import com.onlinejudge.scoring.util.ScoreEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink KeyedProcessFunction for stateful contest scoring.
 *
 * Keyed by userId - each user gets an isolated state partition.
 * Flink guarantees that all events for the same userId are processed
 * by the same task (no concurrent state updates per user).
 *
 * State:
 *   ValueState<ScoringState> - totalScore, penaltyMinutes, wrongAttempts, acceptedProblems
 *
 * Scoring rules (ICPC-style):
 *   - ACCEPTED: +points for problem, +20 min penalty per prior wrong attempt
 *   - WRONG_ANSWER: +1 wrong attempt for problem (counted only if eventually accepted)
 *   - Already accepted problem: ignore re-submission verdict
 *
 * Event-time: uses API Gateway timestamp (gatewayTsMs), not Flink processing time.
 * BoundedOutOfOrderness watermark (5 min) keeps the window open after contest close
 * to absorb late-arriving verdicts from in-flight VMs.
 */
public class ScoringFunction extends KeyedProcessFunction<String, byte[], ScoreUpdate> {

    private static final Logger log = LoggerFactory.getLogger(ScoringFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient ValueState<ScoringState> scoringState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ScoringState> descriptor = new ValueStateDescriptor<>(
                "scoring-state",
                TypeInformation.of(ScoringState.class)
        );
        scoringState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(byte[] eventBytes, Context ctx, Collector<ScoreUpdate> out) throws Exception {
        JsonNode event = MAPPER.readTree(eventBytes);

        String userId     = event.get("userId").asText();
        String problemId  = event.get("problemId").asText();
        String contestId  = event.has("contestId") && !event.get("contestId").isNull()
                ? event.get("contestId").asText() : "global";
        String result     = event.get("result").asText();
        int    points     = event.has("points") ? event.get("points").asInt() : 0;
        long   gatewayTs  = event.get("gatewayTsMs").asLong();

        // Restore or initialize per-user state
        ScoringState state = scoringState.value();
        if (state == null) {
            state = new ScoringState();
        }

        boolean stateChanged = false;

        if ("ACCEPTED".equals(result)) {
            if (!state.acceptedProblems.getOrDefault(problemId, false)) {
                // First acceptance - add points and penalty
                int wrongAttempts = state.wrongAttemptsPerProblem.getOrDefault(problemId, 0);
                int penaltyForThisProblem = wrongAttempts * ScoreEncoder.PENALTY_PER_WRONG_ATTEMPT_MINUTES;

                state.totalScore += points;
                state.totalPenaltyMinutes += penaltyForThisProblem;
                state.acceptedProblems.put(problemId, true);
                stateChanged = true;

                log.info("[scoring] ACCEPTED userId={} problem={} points={} penalty={}min totalScore={} totalPenalty={}min",
                        userId, problemId, points, penaltyForThisProblem,
                        state.totalScore, state.totalPenaltyMinutes);
            }
            // Ignore re-submission after already accepted

        } else if ("WRONG_ANSWER".equals(result) || "RUNTIME_ERROR".equals(result)
                || "TIME_LIMIT_EXCEEDED".equals(result)) {
            if (!state.acceptedProblems.getOrDefault(problemId, false)) {
                // Count wrong attempt only if problem not yet accepted
                state.wrongAttemptsPerProblem.merge(problemId, 1, Integer::sum);
                // Note: no score change here; penalty counted on acceptance
            }
        }

        // Always update state (even WRONG_ANSWER updates wrongAttempts)
        scoringState.update(state);

        if (stateChanged) {
            double zsetScore = ScoreEncoder.encode(state.totalScore, state.totalPenaltyMinutes);
            out.collect(new ScoreUpdate(
                    userId, contestId,
                    state.totalScore, state.totalPenaltyMinutes,
                    zsetScore, gatewayTs
            ));
        }
    }
}
