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
        // Entity fields are `long` (matching CRDB's BIGINT-flavoured `INT` so
        // Hibernate's ddl-auto=validate succeeds). Real values are bounded
        // (time_limit_ms < 60_000, memory_limit_mb < 16_384, points < 1_000_000)
        // so a narrowing cast on the way out is safe.
        return new ProblemDto(
                p.getId(),
                p.getTitle(),
                Math.toIntExact(p.getTimeLimitMs()),
                Math.toIntExact(p.getMemoryLimitMb()),
                Math.toIntExact(p.getPoints()));
    }
}
