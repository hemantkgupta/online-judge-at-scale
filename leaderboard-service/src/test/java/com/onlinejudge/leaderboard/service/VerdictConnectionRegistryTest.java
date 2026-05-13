package com.onlinejudge.leaderboard.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link VerdictConnectionRegistry}.
 *
 * <p>Verifies the per-instance userId↔sessionId bookkeeping that the
 * Push Service uses to gate verdict broadcasts:
 * <ul>
 *   <li>register/unregister keep both directions of the map consistent.</li>
 *   <li>A reconnect from the same user (new sessionId) replaces the old
 *       session and clears stale reverse entries.</li>
 *   <li>{@code isConnectedLocally} reflects current state for the gateway
 *       to decide whether to push or let the HTTP fallback handle it.</li>
 *   <li>Null/blank inputs are rejected without mutating state.</li>
 * </ul>
 */
class VerdictConnectionRegistryTest {

    private VerdictConnectionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new VerdictConnectionRegistry();
    }

    @Test
    void register_addsBothDirections() {
        registry.register("user-1", "sess-A");

        assertThat(registry.isConnectedLocally("user-1")).isTrue();
        assertThat(registry.localConnectionCount()).isEqualTo(1);
        assertThat(registry.connectedUsers()).containsExactly("user-1");
    }

    @Test
    void register_replacesPreviousSessionForSameUser() {
        registry.register("user-1", "sess-A");
        registry.register("user-1", "sess-B");

        // Only one user, only one session.
        assertThat(registry.localConnectionCount()).isEqualTo(1);
        assertThat(registry.isConnectedLocally("user-1")).isTrue();

        // The old session should not be able to unregister the user any more —
        // it was already cleared when sess-B took over.
        registry.unregisterBySession("sess-A");
        assertThat(registry.isConnectedLocally("user-1")).isTrue();

        // The new session unregisters cleanly.
        registry.unregisterBySession("sess-B");
        assertThat(registry.isConnectedLocally("user-1")).isFalse();
    }

    @Test
    void unregisterBySession_removesUserAndSession() {
        registry.register("user-1", "sess-A");
        registry.unregisterBySession("sess-A");

        assertThat(registry.isConnectedLocally("user-1")).isFalse();
        assertThat(registry.localConnectionCount()).isZero();
        assertThat(registry.connectedUsers()).isEmpty();
    }

    @Test
    void unregisterBySession_unknownSession_isNoOp() {
        registry.register("user-1", "sess-A");

        registry.unregisterBySession("does-not-exist");
        registry.unregisterBySession(null);

        assertThat(registry.isConnectedLocally("user-1")).isTrue();
        assertThat(registry.localConnectionCount()).isEqualTo(1);
    }

    @Test
    void isConnectedLocally_distinguishesKnownAndUnknownUsers() {
        registry.register("user-1", "sess-A");
        registry.register("user-2", "sess-B");

        assertThat(registry.isConnectedLocally("user-1")).isTrue();
        assertThat(registry.isConnectedLocally("user-2")).isTrue();
        assertThat(registry.isConnectedLocally("user-3")).isFalse();
        assertThat(registry.isConnectedLocally(null)).isFalse();
    }

    @Test
    void register_rejectsNullOrBlankInputsWithoutMutation() {
        registry.register(null, "sess-A");
        registry.register("", "sess-B");
        registry.register("  ", "sess-C");
        registry.register("user-1", null);

        assertThat(registry.localConnectionCount()).isZero();
        assertThat(registry.isConnectedLocally("user-1")).isFalse();
    }

    @Test
    void connectedUsers_returnsImmutableSnapshot() {
        registry.register("user-1", "sess-A");
        registry.register("user-2", "sess-B");

        var snapshot = registry.connectedUsers();
        assertThat(snapshot).containsExactlyInAnyOrder("user-1", "user-2");

        // Adding more users after the snapshot doesn't mutate the snapshot.
        registry.register("user-3", "sess-C");
        assertThat(snapshot).hasSize(2);
        assertThat(registry.connectedUsers()).hasSize(3);
    }

    @Test
    void multipleUsers_areTrackedIndependently() {
        registry.register("user-1", "sess-A");
        registry.register("user-2", "sess-B");
        registry.register("user-3", "sess-C");

        assertThat(registry.localConnectionCount()).isEqualTo(3);

        registry.unregisterBySession("sess-B");
        assertThat(registry.isConnectedLocally("user-1")).isTrue();
        assertThat(registry.isConnectedLocally("user-2")).isFalse();
        assertThat(registry.isConnectedLocally("user-3")).isTrue();
        assertThat(registry.localConnectionCount()).isEqualTo(2);
    }
}
