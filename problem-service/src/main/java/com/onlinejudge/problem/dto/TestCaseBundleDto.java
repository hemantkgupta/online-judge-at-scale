package com.onlinejudge.problem.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response body for {@code GET /problems/{id}/test-cases}.
 *
 * <p>Wraps the per-ordinal {@link TestCaseUrlsDto} list with the
 * <em>per-problem</em> judging limits. Carrying limits at the top of the
 * envelope keeps each row's wire payload small (no duplication of the same
 * two integers across N rows) while making sure the execution worker
 * receives the contestant time/memory budgets that live in
 * {@code problems.time_limit_ms} / {@code problems.memory_limit_mb}.
 *
 * <p>Snake_case JSON because the Execution Service is the sole consumer
 * and Workstream B spec'd snake_case in the blog post.
 */
public record TestCaseBundleDto(
        @JsonProperty("time_limit_ms")   int timeLimitMs,
        @JsonProperty("memory_limit_mb") int memoryLimitMb,
        @JsonProperty("test_cases")      List<TestCaseUrlsDto> testCases
) {
}
