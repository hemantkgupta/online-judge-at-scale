package com.onlinejudge.gateway.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "submissions")
@Data
@NoArgsConstructor
public class Submission {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "problem_id", nullable = false)
    private UUID problemId;

    @Column(name = "contest_id")
    private UUID contestId;

    @Column(nullable = false, length = 32)
    private String language;

    @Column(name = "s3_code_url", nullable = false)
    private String s3CodeUrl;

    @Column(nullable = false, length = 32)
    private String status = "PENDING";

    @Column(name = "gateway_ts_ms", nullable = false)
    private long gatewayTsMs;

    /**
     * Region the gateway pod that accepted this submission lives in. On a
     * multi-region CRDB cluster this column is the REGIONAL BY ROW key —
     * CRDB pins this row's replicas to the region named here. Single-node
     * locally; populated by {@link com.onlinejudge.gateway.service.RegionResolver}.
     */
    @Column(nullable = false, length = 32)
    private String region;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
}
