package com.onlinejudge.worker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP client for the Problem Service's {@code GET /api/v1/problems/{id}/test-cases}
 * endpoint (Workstream B).
 *
 * <p>The Problem Service returns V4-signed GCS download URLs with a 5-minute
 * TTL, wrapped in an envelope that also carries the per-problem
 * {@code time_limit_ms} and {@code memory_limit_mb} (roadmap §2.3 — those
 * limits live in the {@code problems} row and must reach the in-guest
 * agent + the Sandbox Manager's lease).
 *
 * <p>Wire shape (snake_case JSON):
 * <pre>
 * {
 *   "time_limit_ms":  1000,
 *   "memory_limit_mb": 256,
 *   "test_cases": [
 *     {"ordinal": 1, "input_url": "...", "expected_output_url": "..."},
 *     ...
 *   ]
 * }
 * </pre>
 *
 * <p>The {@code pretestOnly} flag tells the service to return only the first
 * N ordinals (Phase 1 / live during contest) vs the full suite (Phase 2 /
 * system tests). N is configured at the problem-service via
 * {@code app.problem.pretest-ordinal-threshold} (default 10).
 */
@Slf4j
@Component
public class ProblemServiceClient {

    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final HttpClient httpClient;

    public ProblemServiceClient(
            ObjectMapper objectMapper,
            @Value("${app.problem-service.url:http://oj-control-plane:8089}") String baseUrl) {
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Fetch the full test-case bundle (URLs + limits) for a problem.
     *
     * <p>The legacy array-shaped response (a bare JSON array of rows) is still
     * accepted: callers that haven't been redeployed past the 2.3 cut emit
     * that shape and we fall back to applying server-side defaults for the
     * limits (5000 ms / 256 MiB — matches {@code application.yml}). Once the
     * problem-service rollout is complete the legacy branch becomes dead code
     * and can be removed.
     */
    public TestCaseBundle fetchTestCases(String problemId, boolean pretestOnly) throws IOException {
        String encodedId = URLEncoder.encode(problemId, StandardCharsets.UTF_8);
        URI uri = URI.create(baseUrl + "/api/v1/problems/" + encodedId
                + "/test-cases?pretestOnly=" + pretestOnly);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IOException("problem-service " + uri + " HTTP "
                        + resp.statusCode() + ": " + resp.body());
            }
            JsonNode root = objectMapper.readTree(resp.body());

            // New shape: object with time_limit_ms / memory_limit_mb / test_cases.
            if (root.isObject() && root.has("test_cases")) {
                JsonNode arr = root.get("test_cases");
                if (!arr.isArray()) {
                    throw new IOException("problem-service test_cases is not an array: " + resp.body());
                }
                int timeLimitMs   = root.path("time_limit_ms").asInt(0);
                int memoryLimitMb = root.path("memory_limit_mb").asInt(0);
                // Tech-spec §14 L4: optional per-problem idempotency lease
                // override. Missing or null → null on the wire → fall back
                // to the worker's global processing-lease-seconds.
                Long leaseOverride = null;
                JsonNode leaseNode = root.get("lease_seconds_override");
                if (leaseNode != null && !leaseNode.isNull()) {
                    leaseOverride = leaseNode.asLong();
                }
                List<TestCaseUrls> rows = readRows(arr);
                log.debug("[problem-service] bundle problem={} pretestOnly={} cases={} t={}ms m={}MiB leaseOverride={}",
                        problemId, pretestOnly, rows.size(), timeLimitMs, memoryLimitMb, leaseOverride);
                return new TestCaseBundle(timeLimitMs, memoryLimitMb, rows, leaseOverride);
            }

            // Legacy shape: bare JSON array. Limits unknown — let
            // FirecrackerExecutionService apply its application.yml defaults.
            if (root.isArray()) {
                List<TestCaseUrls> rows = readRows(root);
                log.debug("[problem-service] legacy array shape problem={} pretestOnly={} cases={}",
                        problemId, pretestOnly, rows.size());
                return new TestCaseBundle(0, 0, rows, null);
            }

            throw new IOException("problem-service returned unexpected body: " + resp.body());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("problem-service call interrupted: " + uri, ie);
        }
    }

    private List<TestCaseUrls> readRows(JsonNode arr) {
        List<TestCaseUrls> out = new ArrayList<>(arr.size());
        for (JsonNode n : arr) {
            out.add(new TestCaseUrls(
                    n.path("ordinal").asInt(),
                    n.path("input_url").asText(""),
                    n.path("expected_output_url").asText("")));
        }
        return out;
    }

    /** One row of the Problem Service's test-cases response. */
    public record TestCaseUrls(int ordinal, String inputUrl, String expectedOutputUrl) {}

    /**
     * Top-level envelope: per-problem judging limits + the ordered test-case
     * list. {@code timeLimitMs} and {@code memoryLimitMb} are 0 only when the
     * server is on the pre-2.3 array shape; callers MUST treat 0 as
     * "unknown — fall back to backend defaults".
     *
     * <p>{@code leaseSecondsOverride} (tech-spec §14 L4) is null when the
     * problem has no override OR when the server is on a build that doesn't
     * emit the field — both cases mean "use the worker's global
     * {@code app.idempotency.processing-lease-seconds}".
     */
    public record TestCaseBundle(int timeLimitMs, int memoryLimitMb,
                                 List<TestCaseUrls> testCases,
                                 Long leaseSecondsOverride) {
        /** Back-compat 3-arg form — used by tests that predate the §14 L4 field. */
        public TestCaseBundle(int timeLimitMs, int memoryLimitMb, List<TestCaseUrls> testCases) {
            this(timeLimitMs, memoryLimitMb, testCases, null);
        }
    }
}
