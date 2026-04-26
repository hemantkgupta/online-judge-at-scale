package com.onlinejudge.contest.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The five lifecycle states of a contest.
 *
 * Transitions are strictly enforced — only forward transitions are allowed,
 * and each state has a fixed set of valid next states. This prevents the
 * state machine from entering illegal states under concurrent access.
 *
 * The transition graph:
 *   CREATED → REGISTRATION → ACTIVE → CLOSED → RESULTS
 *
 * Each transition triggers a fanout event to Kafka. Every API Gateway pod
 * and Push Service instance subscribes to these events and updates its
 * local cache. The 1-second TTL on the gateway cache is the failsafe
 * for missed events, not the primary invalidation mechanism.
 */
public enum ContestState {

    /**
     * Admin has defined the contest; not yet visible to users.
     * Nothing external happens — contest is invisible.
     */
    CREATED,

    /**
     * Users can register. Problem statements published encrypted to CDN.
     * Clients pre-fetch encrypted bundles during this window.
     */
    REGISTRATION,

    /**
     * T0 has passed. Submissions accepted. Decryption key published.
     * API Gateway permits submissions. Sandbox Manager scales pools.
     */
    ACTIVE,

    /**
     * T1 has passed. No new submissions. Pending VMs drain.
     * API Gateway returns 410 Gone. Flink continues draining late events.
     */
    CLOSED,

    /**
     * Final scores published. System tests complete.
     * Leaderboard frozen. Analytics queries permitted on full dataset.
     */
    RESULTS;

    /**
     * Valid transitions: each state maps to the set of states it can transition to.
     * Only forward transitions are allowed. This is the core invariant of the lifecycle.
     */
    private static final Map<ContestState, Set<ContestState>> VALID_TRANSITIONS = Map.of(
            CREATED,      EnumSet.of(REGISTRATION),
            REGISTRATION, EnumSet.of(ACTIVE),
            ACTIVE,       EnumSet.of(CLOSED),
            CLOSED,       EnumSet.of(RESULTS),
            RESULTS,      EnumSet.noneOf(ContestState.class)  // terminal
    );

    /**
     * Returns true if transitioning from this state to the target state is valid.
     */
    public boolean canTransitionTo(ContestState target) {
        return VALID_TRANSITIONS.getOrDefault(this, EnumSet.noneOf(ContestState.class))
                .contains(target);
    }

    /**
     * Returns the set of valid next states from this state.
     */
    public Set<ContestState> validNextStates() {
        return VALID_TRANSITIONS.getOrDefault(this, EnumSet.noneOf(ContestState.class));
    }

    /**
     * Returns true if this is a terminal state (no further transitions possible).
     */
    public boolean isTerminal() {
        return validNextStates().isEmpty();
    }
}
