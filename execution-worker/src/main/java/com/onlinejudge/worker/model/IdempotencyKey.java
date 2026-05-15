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

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();
}
