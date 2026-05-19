package com.onlinejudge.gateway.controller;

import com.onlinejudge.gateway.observability.GatewayMetrics;
import com.onlinejudge.gateway.security.IdempotencyFilter;
import com.onlinejudge.gateway.service.RateLimitService;
import com.onlinejudge.gateway.service.RegionResolver;
import com.onlinejudge.gateway.service.SubmissionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@code GET /api/v1/submissions/{id}/verdict} fallback endpoint.
 *
 * <p>Per Part 9 of the blog: WebSocket is the fast path; HTTP polling is the reliable
 * fallback. The endpoint reads from Redis under {@code verdict:{submissionId}}, where
 * the leaderboard-service's verdict-push consumer caches every verdict with a 24-hour TTL.
 *
 * <p>Behaviors verified here:
 * <ul>
 *   <li>200 with the JSON payload when a verdict exists.</li>
 *   <li>404 + a {@code PENDING} placeholder when no verdict is cached
 *       (submission still queued, executing, or older than TTL).</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class SubmissionControllerVerdictTest {

    @Mock private SubmissionService submissionService;
    @Mock private RateLimitService rateLimitService;
    @Mock private StringRedisTemplate redisTemplate;
    @Mock private ValueOperations<String, String> valueOps;
    @Mock private IdempotencyFilter idempotencyFilter;

    private SubmissionController controller;

    @BeforeEach
    void setUp() {
        controller = new SubmissionController(
                submissionService, rateLimitService, redisTemplate,
                new RegionResolver("us-east-1"), idempotencyFilter,
                new GatewayMetrics());
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
    }

    @Test
    void getVerdict_present_returnsJsonPayload() {
        String stored = "{\"submissionId\":\"sub-1\",\"userId\":\"u\",\"result\":\"ACCEPTED\"}";
        when(valueOps.get("verdict:sub-1")).thenReturn(stored);

        ResponseEntity<String> response = controller.getVerdict("sub-1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
        assertThat(response.getBody()).isEqualTo(stored);
    }

    @Test
    void getVerdict_absent_returns404WithPendingPayload() {
        when(valueOps.get("verdict:sub-unknown")).thenReturn(null);

        ResponseEntity<String> response = controller.getVerdict("sub-unknown");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
        assertThat(response.getBody())
                .contains("\"status\":\"PENDING\"")
                .contains("\"submissionId\":\"sub-unknown\"");
    }
}
