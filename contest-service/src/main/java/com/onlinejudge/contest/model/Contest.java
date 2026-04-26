package com.onlinejudge.contest.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Contest entity — the smallest service with the largest blast radius.
 *
 * Every API Gateway pod reads this state. Every Push Service instance watches
 * for transitions. The read-write asymmetry (millions of reads per second,
 * a few writes per contest) drives the design: CockroachDB GLOBAL locality
 * gives sub-10ms reads from every region at the cost of slow cross-region
 * writes — acceptable because writes are rare.
 */
@Entity
@Table(name = "contests")
@Data
@NoArgsConstructor
public class Contest {

    @Id
    private UUID id;

    @Column(nullable = false, length = 256)
    private String title;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private ContestState state = ContestState.CREATED;

    /** Contest start time. REGISTRATION→ACTIVE transition fires at this instant. */
    @Column(name = "start_time", nullable = false)
    private Instant startTime;

    /** Contest end time. ACTIVE→CLOSED transition fires at this instant. */
    @Column(name = "end_time", nullable = false)
    private Instant endTime;

    /** Duration in minutes (derived: endTime - startTime). */
    @Column(name = "duration_minutes", nullable = false)
    private int durationMinutes;

    /**
     * AES-256 encryption key for the problem bundle.
     * Set during REGISTRATION. Published to CDN at ACTIVE transition.
     * Null before REGISTRATION. Stored encrypted-at-rest in CockroachDB.
     */
    @Column(name = "encryption_key", length = 512)
    private String encryptionKey;

    /**
     * R2/CDN URL where the encrypted problem bundle is stored.
     * Set during REGISTRATION when the encrypted bundle is uploaded.
     */
    @Column(name = "encrypted_bundle_url", length = 1024)
    private String encryptedBundleUrl;

    /** Monotonic version counter, incremented on every state transition. */
    @Version
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
