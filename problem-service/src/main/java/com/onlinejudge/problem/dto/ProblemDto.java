package com.onlinejudge.problem.dto;

import com.onlinejudge.problem.entity.Problem;

import java.util.UUID;

/**
 * Wire-shape for a problem returned by {@code GET /api/v1/problems}.
 * Decoupled from the JPA entity so internal columns (timestamps, etc.) never
 * leak into HTTP responses by accident.
 */
public record ProblemDto(
        UUID id,
        String title,
        int timeLimitMs,
        int memoryLimitMb,
        int points
) {
    public static ProblemDto from(Problem p) {
        return new ProblemDto(p.getId(), p.getTitle(), p.getTimeLimitMs(), p.getMemoryLimitMb(), p.getPoints());
    }
}
