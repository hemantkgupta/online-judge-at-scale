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
 * TTL. The wire shape is snake_case JSON
 * {@code [{"ordinal":1, "input_url":"...", "expected_output_url":"..."}]}.
 *
 * <p>The {@code pretestOnly} flag tells the service to return only the first
 * 10 ordinals (Phase 1 / live during contest) vs the full suite (Phase 2 /
 * system tests).
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

    public List<TestCaseUrls> fetchTestCases(String problemId, boolean pretestOnly) throws IOException {
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
            JsonNode arr = objectMapper.readTree(resp.body());
            if (!arr.isArray()) {
                throw new IOException("problem-service returned non-array body: " + resp.body());
            }
            List<TestCaseUrls> out = new ArrayList<>(arr.size());
            for (JsonNode n : arr) {
                out.add(new TestCaseUrls(
                        n.path("ordinal").asInt(),
                        n.path("input_url").asText(""),
                        n.path("expected_output_url").asText("")));
            }
            log.debug("[problem-service] {} test cases for problem={} pretestOnly={}",
                    out.size(), problemId, pretestOnly);
            return out;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("problem-service call interrupted: " + uri, ie);
        }
    }

    /** One row of the Problem Service's test-cases response. */
    public record TestCaseUrls(int ordinal, String inputUrl, String expectedOutputUrl) {}
}
