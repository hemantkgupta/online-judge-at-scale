package com.onlinejudge.contest.service;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Roadmap §4.23: verifies that {@link ContestStateMachine} kicks the
 * {@link SystemTestReplayPublisher} exactly on ACTIVE→CLOSED transitions,
 * and not on idempotent no-ops or rejected transitions.
 */
@ExtendWith(MockitoExtension.class)
class ContestStateMachineSystemTestReplayTest {

    @Mock private SystemTestReplayPublisher publisher;

    private ContestStateMachine stateMachine;
    private Contest contest;

    @BeforeEach
    void setUp() {
        stateMachine = new ContestStateMachine(publisher);

        contest = new Contest();
        contest.setId(UUID.randomUUID());
        contest.setTitle("Replay-trigger Test");
        contest.setStartTime(Instant.now().minus(2, ChronoUnit.HOURS));
        contest.setEndTime(Instant.now().minus(1, ChronoUnit.HOURS));
        contest.setDurationMinutes(60);
    }

    @Test
    void activeToClosed_triggersReplayExactlyOnce() {
        contest.setState(ContestState.ACTIVE);

        stateMachine.transition(contest, ContestState.CLOSED);

        verify(publisher, times(1)).replayContest(contest);
    }

    @Test
    void closedToClosed_isIdempotent_doesNotTriggerReplay() {
        contest.setState(ContestState.CLOSED);

        stateMachine.transition(contest, ContestState.CLOSED);

        verify(publisher, never()).replayContest(contest);
    }

    @Test
    void registrationToClosed_isRejected_doesNotTriggerReplay() {
        // REGISTRATION→CLOSED is not in the transition graph (only ACTIVE→CLOSED).
        // The state machine must reject it before any replay side-effect can fire —
        // a hard-cancel before the contest ran has no submissions to replay anyway.
        contest.setState(ContestState.REGISTRATION);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.CLOSED))
                .isInstanceOf(IllegalStateException.class);

        verify(publisher, never()).replayContest(contest);
    }

    @Test
    void activeToClosed_replayFailure_doesNotBlockTransition() {
        contest.setState(ContestState.ACTIVE);
        org.mockito.Mockito.doThrow(new RuntimeException("kafka down"))
                .when(publisher).replayContest(contest);

        // Must NOT propagate — the contest is genuinely CLOSED, replay can be
        // retried by an admin, and the worker's idempotency keys prevent
        // double-execution of any submissions that did make it through.
        stateMachine.transition(contest, ContestState.CLOSED);

        org.assertj.core.api.Assertions.assertThat(contest.getState()).isEqualTo(ContestState.CLOSED);
        verify(publisher).replayContest(contest);
    }

    @Test
    void createdToRegistration_doesNotTriggerReplay() {
        contest.setState(ContestState.CREATED);
        contest.setEncryptionKey("k");
        contest.setEncryptedBundleUrl("u");

        stateMachine.transition(contest, ContestState.REGISTRATION);

        verify(publisher, never()).replayContest(contest);
    }
}
