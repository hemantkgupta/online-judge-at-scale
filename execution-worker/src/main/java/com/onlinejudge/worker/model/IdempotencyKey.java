package com.onlinejudge.worker.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
@Data
@NoArgsConstructor
public class IdempotencyKey {

    @Id
    @Column(length = 64)
    private String key;

    @Column(name = "submission_id", nullable = false)
    private UUID submissionId;

    @Column(nullable = false, length = 32)
    private String status;

    /** Roadmap §2.5 — reclaim cycles. Bumped on every stale-row reclaim;
     *  once it exceeds {@code app.idempotency.max-attempts} the next claim
     *  is treated as POISON and the consumer dead-letters the submission. */
    @Column(nullable = false)
    private int attempts;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();
}
