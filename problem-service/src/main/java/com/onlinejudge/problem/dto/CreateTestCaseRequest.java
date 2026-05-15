package com.onlinejudge.problem.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Admin-only payload for {@code POST /api/v1/problems/{id}/test-cases}.
 *
 * IMPORTANT: this endpoint takes GCS object keys, not file bytes. The bytes
 * are uploaded out-of-band (operator CLI uploads them with the GCS SDK
 * directly, then registers the keys here). Keeping bytes off this endpoint
 * means the Problem Service never holds large request bodies in memory and
 * stays out of the data plane on writes too.
 */
public record CreateTestCaseRequest(
        @NotEmpty @Valid List<TestCaseEntry> testCases
) {
    public record TestCaseEntry(
            @Min(1) int ordinal,
            @NotBlank String inputGcsKey,
            @NotBlank String expectedOutputGcsKey
    ) {
    }
}
