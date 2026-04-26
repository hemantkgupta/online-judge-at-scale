package com.onlinejudge.gateway.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process cache of contest states for the API Gateway's contest window check.
 *
 * This is Stage 4 of the gateway pipeline (Part 3 of the blog):
 *   1. T0 stamp
 *   2. JWT Auth
 *   3. Rate limit
 *   4. Contest window enforcement ← this component
 *
 * The cache is updated by two mechanisms:
 *
 * 1. Push (primary): The gateway subscribes to the `contest_events` Kafka topic.
 *    When the Contest Service transitions a contest's state, the event arrives
 *    here in sub-100ms. The cache is updated immediately.
 *
 * 2. Pull (failsafe): Every 1 second, stale entries older than the TTL are
 *    evicted. On next request, the cache misses and queries the Contest Service.
 *    This 1-second TTL is the worst-case window in which a stale "contest open"
 *    entry can let a late submission through.
 *
 * The push-pull combination means: typical invalidation latency is sub-100ms
 * (push), worst case is 1 second (TTL expiry after a missed push event).
 *
 * Thread safety: ConcurrentHashMap for the cache. Reads are lock-free.
 * Writes (from Kafka consumer + TTL eviction) are atomic per key.
 */
@Slf4j
@Component
public class ContestWindowFilter {

    /** Active contest IDs — only contests in ACTIVE state are in this set. */
    private final Set<String> activeContests = ConcurrentHashMap.newKeySet();

    /** Cache entries with timestamps for TTL eviction. */
    private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();

    @Value("${app.contest-window.cache-ttl-ms:1000}")
    private long cacheTtlMs;

    /**
     * Checks if a contest is currently accepting submissions.
     * Returns true only if the contest is in ACTIVE state.
     *
     * This is called on every submission request — must be O(1).
     */
    public boolean isContestActive(String contestId) {
        if (contestId == null) {
            // No contest ID → not a contest submission, allow it
            return true;
        }
        return activeContests.contains(contestId);
    }

    /**
     * Updates the cache when a contest state change event is received from Kafka.
     * Called by the Kafka consumer that subscribes to `contest_events`.
     */
    public void onContestStateChange(String contestId, String newState) {
        if ("ACTIVE".equals(newState)) {
            activeContests.add(contestId);
            cacheTimestamps.put(contestId, System.currentTimeMillis());
            log.info("[contest-window] Contest {} is now ACTIVE. Accepting submissions.", contestId);
        } else {
            activeContests.remove(contestId);
            cacheTimestamps.remove(contestId);
            log.info("[contest-window] Contest {} is now {}. Rejecting submissions.", contestId, newState);
        }
    }

    /**
     * TTL eviction: runs every second.
     * Removes entries older than the TTL. On next request for that contest,
     * the caller must query the Contest Service directly (cache miss).
     */
    @Scheduled(fixedDelay = 1000)
    public void evictStaleEntries() {
        long now = System.currentTimeMillis();
        cacheTimestamps.entrySet().removeIf(entry -> {
            if (now - entry.getValue() > cacheTtlMs) {
                activeContests.remove(entry.getKey());
                return true;
            }
            return false;
        });
    }

    /**
     * Returns the number of currently cached active contests.
     */
    public int activeContestCount() {
        return activeContests.size();
    }
}
