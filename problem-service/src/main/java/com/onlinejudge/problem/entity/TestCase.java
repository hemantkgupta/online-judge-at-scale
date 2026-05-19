package com.onlinejudge.problem.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * A single test case for a problem.
 *
 * Test cases are the most security-sensitive data in the system:
 *  - Stored encrypted in GCS as separate input/expected-output objects.
 *  - Delivered to execution workers via short-lived V4-signed GCS URLs.
 *  - Never exposed to contestants directly.
 *
 * Ordering matters. The split between pretests and system tests is now
 * configurable (tech-spec §14 L3) — see
 * {@code app.problem.pretest-ordinal-threshold} in {@code application.yml}.
 * The default preserves the original blog contract:
 *  ordinal 1–N:   pretests   (Phase 1 — run during contest, fast; N=10 by default)
 *  ordinal N+1+:  system tests (Phase 2 — exhaustive, post-contest)
 *
 * Indexed by {@code (problem_id, ordinal)} so the read path (worker fetching
 * pretests for a submission) is a single index scan.
 */
@Entity
@Table(
        name = "test_cases",
        indexes = {
                @Index(name = "idx_test_cases_problem_ordinal", columnList = "problem_id, ordinal")
        }
)
@Data
@NoArgsConstructor
public class TestCase {

    @Id
    private UUID id;

    @Column(name = "problem_id", nullable = false)
    private UUID problemId;

    /** 1-indexed. Lower = pretest, higher = system test. */
    @Column(nullable = false)
    private int ordinal;

    /** GCS object key (no leading slash, no bucket) for the test input. */
    @Column(name = "input_gcs_key", nullable = false, length = 512)
    private String inputGcsKey;

    /** GCS object key for the expected-output file. */
    @Column(name = "expected_output_gcs_key", nullable = false, length = 512)
    private String expectedOutputGcsKey;
}
