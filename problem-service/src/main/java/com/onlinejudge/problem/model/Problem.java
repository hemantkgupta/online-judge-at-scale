package com.onlinejudge.problem.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * A programming problem definition.
 *
 * Problems are GLOBAL in CockroachDB's locality model (same as Contest Service):
 * read-heavy and write-rare. Problem statements are read millions of times
 * during a contest; they're written once during contest setup. GLOBAL locality
 * gives sub-10ms reads from every region.
 *
 * The problem body is stored encrypted in R2/CDN. This entity stores metadata
 * + the R2 key, not the actual problem text or test cases.
 */
@Entity
@Table(name = "problems")
@Data
@NoArgsConstructor
public class Problem {

    @Id
    private UUID id;

    @Column(nullable = false, length = 256)
    private String title;

    @Column(name = "time_limit_ms", nullable = false)
    private int timeLimitMs;

    @Column(name = "memory_limit_mb", nullable = false)
    private int memoryLimitMb;

    @Column(nullable = false)
    private int points;

    /**
     * R2/S3 key where the encrypted problem statement is stored.
     * During REGISTRATION, the encrypted bundle is served via CDN.
     * At T0, the decryption key is published and clients decrypt locally.
     */
    @Column(name = "statement_r2_key", length = 512)
    private String statementR2Key;

    /** Number of test cases associated with this problem. */
    @Column(name = "test_case_count")
    private int testCaseCount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
