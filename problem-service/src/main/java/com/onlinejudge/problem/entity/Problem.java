package com.onlinejudge.problem.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * A programming problem definition.
 *
 * Problems are GLOBAL in CockroachDB's locality model: read-heavy, write-rare.
 * The body of the problem statement lives in GCS — this entity stores metadata
 * + judging limits only. Test cases live in {@link TestCase}.
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

    // CockroachDB's `INT` is BIGINT (8-byte). Hibernate's ddl-auto=validate
    // requires the entity field type to match — Java `long` -> BIGINT, Java
    // `int` -> INTEGER (INT4). These columns are declared `INT` in V3, so the
    // matching Java type is `long`. (Pre-V3 these were `int`, which forced a
    // compose-level ddl-auto=none workaround; V3 removes the workaround.)
    @Column(name = "time_limit_ms", nullable = false)
    private long timeLimitMs;

    @Column(name = "memory_limit_mb", nullable = false)
    private long memoryLimitMb;

    @Column(nullable = false)
    private long points;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
