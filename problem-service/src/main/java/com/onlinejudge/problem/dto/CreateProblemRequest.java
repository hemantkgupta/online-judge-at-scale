package com.onlinejudge.problem.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * Admin-only payload for {@code POST /api/v1/problems}. No auth wired up yet —
 * see Workstream G for the JWT/admin-role plumbing. Validation is the only
 * gate today.
 */
public record CreateProblemRequest(
        @NotBlank String title,
        @Min(1) int timeLimitMs,
        @Min(1) int memoryLimitMb,
        @Min(1) int points
) {
}
