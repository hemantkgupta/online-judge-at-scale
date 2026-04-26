package com.onlinejudge.problem.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * A single test case for a problem.
 *
 * Test cases are the most security-sensitive data in the system:
 * - Before T0: encrypted in R2, inaccessible to contestants
 * - During contest: delivered to execution workers via pre-signed R2 URLs
 * - Never exposed to contestants directly (only verdicts are returned)
 *
 * Storage model:
 * - Input and expected output are stored in R2 as separate objects
 * - This entity stores the R2 keys (pointers), not the actual data
 * - Pre-signed URLs are generated on-demand for the Execution Service
 *
 * Test cases are ordered: lower ordinals are "pretests" (Phase 1),
 * higher ordinals are "system tests" (Phase 2).
 *   ordinal 1-10:  pretests  (fast, run during contest, <2s verdict)
 *   ordinal 11+:   system tests (exhaustive, run post-contest)
 */
@Entity
@Table(name = "test_cases")
@Data
@NoArgsConstructor
public class TestCase {

    @Id
    private UUID id;

    @Column(name = "problem_id", nullable = false)
    private UUID problemId;

    /** Test case ordering. Lower = pretest, higher = system test. */
    @Column(nullable = false)
    private int ordinal;

    /** R2 key for the input file. */
    @Column(name = "input_r2_key", nullable = false, length = 512)
    private String inputR2Key;

    /** R2 key for the expected output file. */
    @Column(name = "expected_output_r2_key", nullable = false, length = 512)
    private String expectedOutputR2Key;

    /** Maximum score for this test case (often uniform, sometimes weighted). */
    @Column(name = "max_score")
    private int maxScore;

    /** True if this is a pretest (shown during contest). */
    public boolean isPretest() {
        return ordinal <= 10;
    }
}
