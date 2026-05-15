package com.onlinejudge.scoring.function;

import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.model.ScoringState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Flink {@link KeyedProcessFunction} for stateful contest scoring, keyed by {@code userId}.
 *
 * <p>This class is intentionally thin: it owns the Flink-runtime contract (state
 * descriptor, deserialization, emission) and delegates the actual scoring rules
 * to {@link Scorer}, which is a pure function unit-tested in isolation.
 *
 * <p>State: a single {@link ValueState} per user. Each {@code ScoringState} holds
 * running aggregates plus a per-problem map of accepted-event-time and
 * wrong-attempt-time sets — supporting the event-time-aware corrections needed
 * for out-of-order cross-region verdict streams (see Part 8 of the blog and the
 * doc on {@link Scorer}).
 *
 * <p>Event-time: the gateway ingestion timestamp ({@code gateway_ts_ms}) carried
 * inside each verdict payload is what Flink's watermark assigner reads
 * (configured in {@code ScoringJobApplication}). Late events arriving past the
 * {@code BoundedOutOfOrderness} watermark would be routed to a side output in a
 * production deployment; the local job lets them through and
 * {@link Scorer#apply} corrects the aggregates accordingly.
 */
public class ScoringFunction extends KeyedProcessFunction<String, byte[], ScoreUpdate> {

    private static final Logger log = LoggerFactory.getLogger(ScoringFunction.class);

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
    public void processElement(byte[] eventBytes,
                               Context ctx,
                               Collector<ScoreUpdate> out) throws Exception {
        VerdictEvent event = VerdictEvent.parseFrom(eventBytes);

        String userId    = event.getUserId();
        String problemId = event.getProblemId();
        String contestId = event.getContestId().isEmpty() ? "global" : event.getContestId();
        String result    = event.getResult();
        int    points    = event.getPoints();
        String phase     = event.getPhase();
        long   eventTs   = event.getEventTsMs() > 0 ? event.getEventTsMs() : event.getGatewayTsMs();

        if (!Scorer.isScoreablePhase(phase)) {
            log.debug("[scoring] ignored non-scoreable verdict submission={} user={} contest={} problem={} phase={} result={}",
                    event.getSubmissionId(), userId, contestId, problemId, phase, result);
            return;
        }

        ScoringState state = scoringState.value();
        if (state == null) {
            state = new ScoringState();
        }

        Optional<ScoreUpdate> update =
                Scorer.apply(state, userId, contestId, problemId, result, points, eventTs, phase);

        // Always persist — wrong-attempt times for not-yet-accepted problems
        // are recorded even when no ScoreUpdate is emitted.
        scoringState.update(state);

        update.ifPresent(u -> {
            log.info("[scoring] user={} contest={} totalScore={} penalty={}min",
                    u.userId(), u.contestId(), u.totalScore(), u.penaltyMinutes());
            out.collect(u);
        });
    }
}
