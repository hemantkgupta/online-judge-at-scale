package com.onlinejudge.leaderboard.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-instance registry of which users currently hold a live WebSocket
 * connection here. Tracks the {@code userId → sessionId} mapping the Push
 * Service uses to gate verdict pushes (Part 9 of the blog).
 *
 * <p>The production design assumes <b>co-partitioned routing</b> so each user
 * has exactly one gateway instance: the WebSocket load balancer hashes the
 * user-id the same way the Kafka consumer-group partitioner does, and both
 * land on the same instance. With that invariant, a verdict arriving on this
 * instance's Kafka partition will always find the matching connection in
 * <em>this</em> registry, and never any other.
 *
 * <p>The {@code SubmissionConsumer} (verdict push side) consults this registry
 * <em>before</em> publishing. If the user isn't connected here, the push is
 * skipped — the user's reconnect-resume HTTP poll
 * ({@code GET /api/v1/submissions/{id}/verdict}) will pick up the cached
 * verdict from Redis. This is the "WebSocket fast path + HTTP reliable
 * fallback" pattern.
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}. Connection registration is
 * driven by the STOMP {@code CONNECT}/{@code DISCONNECT} interceptor in
 * {@code WebSocketConfig}.
 */
@Slf4j
@Component
public class VerdictConnectionRegistry {

    /** userId → STOMP sessionId currently held by this gateway instance. */
    private final ConcurrentHashMap<String, String> userToSession = new ConcurrentHashMap<>();

    /** sessionId → userId, for cheap unregister on DISCONNECT. */
    private final ConcurrentHashMap<String, String> sessionToUser = new ConcurrentHashMap<>();

    /**
     * Registers a user's STOMP session on this instance. If the user already had
     * a connection here (browser tab refresh, mobile reconnect), the previous
     * session is silently replaced — the new one wins.
     */
    public void register(String userId, String sessionId) {
        if (userId == null || userId.isBlank() || sessionId == null) {
            log.warn("[push] Refusing to register session={} for userId={}", sessionId, userId);
            return;
        }
        String previousSession = userToSession.put(userId, sessionId);
        if (previousSession != null && !previousSession.equals(sessionId)) {
            sessionToUser.remove(previousSession);
            log.info("[push] Replaced previous session={} with session={} for user={}",
                    previousSession, sessionId, userId);
        }
        sessionToUser.put(sessionId, userId);
        log.info("[push] Registered user={} session={} (instance has {} connections)",
                userId, sessionId, userToSession.size());
    }

    /**
     * Removes the user associated with this STOMP session. Called on
     * {@code DISCONNECT} frames and on socket close.
     */
    public void unregisterBySession(String sessionId) {
        if (sessionId == null) return;
        String userId = sessionToUser.remove(sessionId);
        if (userId != null) {
            // Only clear userToSession if it still points at this session;
            // a tab refresh may have already overwritten it.
            userToSession.remove(userId, sessionId);
            log.info("[push] Unregistered user={} session={}", userId, sessionId);
        }
    }

    /** True iff this user has a live STOMP session on this gateway instance. */
    public boolean isConnectedLocally(String userId) {
        return userId != null && userToSession.containsKey(userId);
    }

    /** Total number of users currently connected to this instance. */
    public int localConnectionCount() {
        return userToSession.size();
    }

    /** Snapshot of currently-connected user IDs (for metrics/health). */
    public Set<String> connectedUsers() {
        return Set.copyOf(userToSession.keySet());
    }
}
