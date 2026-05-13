package com.onlinejudge.common.sharding;

import java.nio.charset.StandardCharsets;

/**
 * Computes which Push Service instance owns a given {@code userId}.
 *
 * <p>Per Part 9 of the blog, the Push Service achieves "co-partitioning":
 * <ul>
 *   <li>The {@code evaluated_results} Kafka topic is partitioned by {@code userId}
 *       hash, so all verdicts for user U land on a single partition.</li>
 *   <li>Kafka's consumer-group rebalancer assigns each partition to a specific
 *       gateway instance.</li>
 *   <li>The WebSocket load balancer applies the <em>same</em> consistent hash on
 *       {@code userId} when routing the client's WebSocket upgrade, so the
 *       instance holding the Kafka partition is also the instance holding the
 *       user's WebSocket session.</li>
 * </ul>
 *
 * <p>Net effect: when a verdict for user U arrives, exactly one Push Service
 * instance has both the Kafka message AND the WebSocket connection. No
 * external registry lookup is needed on the hot path.
 *
 * <p>This class encapsulates the hash function so both sides — the LB-config
 * generator and the gateway's "is this my user?" check — agree on the answer.
 * The function used is a stable 32-bit Murmur3-style mix over UTF-8 bytes,
 * mod by instance count.
 */
public final class PartitionAssigner {

    private final int instanceCount;

    public PartitionAssigner(int instanceCount) {
        if (instanceCount <= 0) {
            throw new IllegalArgumentException("instanceCount must be > 0");
        }
        this.instanceCount = instanceCount;
    }

    /** Returns the 0-indexed instance number that owns this user. */
    public int instanceFor(String userId) {
        if (userId == null) {
            throw new IllegalArgumentException("userId must not be null");
        }
        int h = hash32(userId.getBytes(StandardCharsets.UTF_8));
        // Math.floorMod handles negative hashes correctly.
        return Math.floorMod(h, instanceCount);
    }

    /**
     * Returns true iff this user is owned by the given instance index.
     * The gateway calls this with its own instance index to gate verdict push.
     */
    public boolean ownedBy(String userId, int myInstanceIndex) {
        return instanceFor(userId) == myInstanceIndex;
    }

    public int instanceCount() {
        return instanceCount;
    }

    // 32-bit FNV-1a mix — stable across JVMs, no external dependency.
    private static int hash32(byte[] data) {
        int h = 0x811c9dc5;            // FNV offset basis
        for (byte b : data) {
            h ^= (b & 0xff);
            h *= 0x01000193;           // FNV prime
        }
        return h;
    }
}
