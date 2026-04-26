package com.onlinejudge.contest.service;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the contest lifecycle state machine.
 *
 * Verifies:
 * - Only forward transitions are allowed
 * - Preconditions are enforced per transition
 * - Idempotent transitions (already in target state)
 * - Invalid transitions are rejected
 * - Terminal state has no valid next states
 */
class ContestStateMachineTest {

    private ContestStateMachine stateMachine;
    private Contest contest;

    @BeforeEach
    void setUp() {
        stateMachine = new ContestStateMachine();
        contest = new Contest();
        contest.setId(UUID.randomUUID());
        contest.setTitle("Test Contest");
        contest.setStartTime(Instant.now().plus(1, ChronoUnit.HOURS));
        contest.setEndTime(Instant.now().plus(2, ChronoUnit.HOURS));
        contest.setDurationMinutes(60);
        contest.setState(ContestState.CREATED);
    }

    // --- Forward transitions ---

    @Test
    void created_to_registration_succeeds() {
        contest.setEncryptionKey("test-key");
        contest.setEncryptedBundleUrl("test-url");

        boolean result = stateMachine.transition(contest, ContestState.REGISTRATION);

        assertThat(result).isTrue();
        assertThat(contest.getState()).isEqualTo(ContestState.REGISTRATION);
    }

    @Test
    void registration_to_active_succeeds_withKey() {
        contest.setState(ContestState.REGISTRATION);
        contest.setEncryptionKey("aes-256-key-base64");
        contest.setEncryptedBundleUrl("local://bundles/test");

        boolean result = stateMachine.transition(contest, ContestState.ACTIVE);

        assertThat(result).isTrue();
        assertThat(contest.getState()).isEqualTo(ContestState.ACTIVE);
    }

    @Test
    void active_to_closed_succeeds() {
        contest.setState(ContestState.ACTIVE);

        boolean result = stateMachine.transition(contest, ContestState.CLOSED);

        assertThat(result).isTrue();
        assertThat(contest.getState()).isEqualTo(ContestState.CLOSED);
    }

    @Test
    void closed_to_results_succeeds() {
        contest.setState(ContestState.CLOSED);

        boolean result = stateMachine.transition(contest, ContestState.RESULTS);

        assertThat(result).isTrue();
        assertThat(contest.getState()).isEqualTo(ContestState.RESULTS);
    }

    // --- Invalid transitions ---

    @Test
    void created_to_active_rejected() {
        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.ACTIVE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid transition: CREATED → ACTIVE");
    }

    @Test
    void created_to_closed_rejected() {
        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.CLOSED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void active_to_registration_rejected_noBackwardTransition() {
        contest.setState(ContestState.ACTIVE);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.REGISTRATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid transition: ACTIVE → REGISTRATION");
    }

    @Test
    void results_to_anything_rejected_terminalState() {
        contest.setState(ContestState.RESULTS);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.ACTIVE))
                .isInstanceOf(IllegalStateException.class);
    }

    // --- Precondition checks ---

    @Test
    void registration_requiresStartTimeAndEndTime() {
        contest.setStartTime(null);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.REGISTRATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("startTime and endTime must be set");
    }

    @Test
    void registration_requiresPositiveDuration() {
        contest.setDurationMinutes(0);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.REGISTRATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("durationMinutes must be positive");
    }

    @Test
    void active_requiresEncryptionKey() {
        contest.setState(ContestState.REGISTRATION);
        contest.setEncryptionKey(null);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.ACTIVE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("encryption key not set");
    }

    @Test
    void active_requiresEncryptedBundleUrl() {
        contest.setState(ContestState.REGISTRATION);
        contest.setEncryptionKey("key");
        contest.setEncryptedBundleUrl(null);

        assertThatThrownBy(() -> stateMachine.transition(contest, ContestState.ACTIVE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("encrypted bundle URL not set");
    }

    // --- Idempotent transitions ---

    @Test
    void idempotent_alreadyInTargetState() {
        contest.setState(ContestState.ACTIVE);

        boolean result = stateMachine.transition(contest, ContestState.ACTIVE);

        assertThat(result).isTrue();
        assertThat(contest.getState()).isEqualTo(ContestState.ACTIVE);
    }

    // --- ContestState enum tests ---

    @Test
    void contestState_canTransitionTo_validTransitions() {
        assertThat(ContestState.CREATED.canTransitionTo(ContestState.REGISTRATION)).isTrue();
        assertThat(ContestState.REGISTRATION.canTransitionTo(ContestState.ACTIVE)).isTrue();
        assertThat(ContestState.ACTIVE.canTransitionTo(ContestState.CLOSED)).isTrue();
        assertThat(ContestState.CLOSED.canTransitionTo(ContestState.RESULTS)).isTrue();
    }

    @Test
    void contestState_canTransitionTo_invalidTransitions() {
        assertThat(ContestState.CREATED.canTransitionTo(ContestState.ACTIVE)).isFalse();
        assertThat(ContestState.CREATED.canTransitionTo(ContestState.CLOSED)).isFalse();
        assertThat(ContestState.ACTIVE.canTransitionTo(ContestState.CREATED)).isFalse();
        assertThat(ContestState.RESULTS.canTransitionTo(ContestState.CREATED)).isFalse();
    }

    @Test
    void contestState_results_isTerminal() {
        assertThat(ContestState.RESULTS.isTerminal()).isTrue();
        assertThat(ContestState.RESULTS.validNextStates()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = ContestState.class, names = {"CREATED", "REGISTRATION", "ACTIVE", "CLOSED"})
    void contestState_nonTerminalStates_haveValidNextStates(ContestState state) {
        assertThat(state.isTerminal()).isFalse();
        assertThat(state.validNextStates()).isNotEmpty();
    }
}
