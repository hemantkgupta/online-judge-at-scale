package com.onlinejudge.problem.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One row in the response of {@code GET /problems/{id}/test-cases}.
 *
 * The two URLs are V4-signed GCS download URLs with a 5-minute TTL. The
 * field names are snake_case on the wire because the Execution Service
 * (Workstream B) is the sole consumer and it spec'd them that way in the
 * blog post; the Java field names follow Java conventions.
 */
public record TestCaseUrlsDto(
        int ordinal,
        @JsonProperty("input_url") String inputUrl,
        @JsonProperty("expected_output_url") String expectedOutputUrl
) {
}
