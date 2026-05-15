package com.onlinejudge.gateway.controller;

import com.onlinejudge.gateway.dto.SubmissionRequest;
import com.onlinejudge.gateway.dto.SubmissionResponse;
import com.onlinejudge.gateway.security.IdempotencyFilter;
import com.onlinejudge.gateway.service.RateLimitService;
import com.onlinejudge.gateway.service.RegionResolver;
import com.onlinejudge.gateway.service.SubmissionService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/submissions")
@RequiredArgsConstructor
public class SubmissionController {

    /** Redis key prefix the verdict-push consumer writes verdicts under (24-hour TTL). */
    private static final String VERDICT_STORE_KEY_PREFIX = "verdict:";

    private final SubmissionService submissionService;
    private final RateLimitService rateLimitService;
    private final StringRedisTemplate redisTemplate;
    private final RegionResolver regionResolver;
    private final IdempotencyFilter idempotencyFilter;

    /**
     * POST /api/v1/submissions
     *
     * Returns 202 Accepted immediately after durably persisting the submission.
     * The actual code execution is asynchronous. The verdict is delivered
     * via WebSocket (connect to /ws/leaderboard).
     *
     * Rate limit: per-user AND per-IP via atomic Lua script (429 if either bucket exceeded).
     */
    @PostMapping
    public ResponseEntity<?> submit(@Valid @RequestBody SubmissionRequest request,
                                    @AuthenticationPrincipal String userId,
                                    HttpServletRequest httpRequest) throws Exception {

        String idempotencyKey = httpRequest.getHeader("Idempotency-Key");
        IdempotencyFilter.ClaimResult claim = idempotencyFilter.checkAndClaim(idempotencyKey);
        if (claim.status() == IdempotencyFilter.ClaimStatus.COMPLETED) {
            return ResponseEntity.accepted().body(new SubmissionResponse(
                    claim.submissionId(),
                    "PENDING",
                    claim.gatewayTsMs(),
                    "Submission accepted. Verdict will be delivered via WebSocket."
            ));
        }
        if (claim.status() == IdempotencyFilter.ClaimStatus.IN_PROGRESS) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "status", "IN_PROGRESS",
                    "message", "A submission with this Idempotency-Key is already being accepted"
            ));
        }

        String sourceIp = clientIp(httpRequest);

        // Dual-scoped rate limit: per-user AND per-IP, checked atomically.
        // The userId here is the JWT subject (not anything in the request body)
        // so a single compromised account can't rotate userIds to dodge the limit.
        if (!rateLimitService.isAllowed(userId, sourceIp)) {
            idempotencyFilter.releaseClaim(idempotencyKey);
            log.warn("[gateway] Rate limit exceeded for user={} ip={}", userId, sourceIp);
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body("Rate limit exceeded");
        }

        try {
            String region = regionResolver.resolve(httpRequest);
            SubmissionResponse response = submissionService.accept(request, userId, region);
            idempotencyFilter.recordSubmission(idempotencyKey, response.getSubmissionId(), response.getGatewayTsMs());
            return ResponseEntity.accepted().body(response);
        } catch (Exception ex) {
            idempotencyFilter.releaseClaim(idempotencyKey);
            throw ex;
        }
    }

    /**
     * Best-effort client IP extraction. Honors {@code X-Forwarded-For} when the gateway sits
     * behind a CDN/anycast edge (production), falls back to the raw socket address otherwise.
     */
    private static String clientIp(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            int comma = xff.indexOf(',');
            return (comma > 0 ? xff.substring(0, comma) : xff).trim();
        }
        return req.getRemoteAddr();
    }

    /**
     * GET /api/v1/submissions/{id}/verdict — HTTP fallback to the WebSocket verdict push.
     *
     * <p>Per the blog's Part 9: "WebSocket is the fast path; HTTP pull is the reliable
     * fallback." Clients on flaky networks, behind WebSocket-blocking proxies, or that
     * reconnected after a brief disconnection use this endpoint to recover any verdict
     * they may have missed on their WebSocket subscription.
     *
     * <p>The leaderboard-service's {@code VerdictPushConsumer} writes each verdict to
     * the Redis key {@code verdict:{submissionId}} with a 24-hour TTL. This endpoint
     * just reads that key.
     *
     * <p>Returns {@code 200 OK} with the JSON verdict payload (same shape the execution
     * worker publishes), or {@code 404 Not Found} if no verdict has been produced yet
     * for this submission (still queued, mid-execution, or older than the TTL).
     */
    @GetMapping("/{submissionId}/verdict")
    public ResponseEntity<String> getVerdict(@PathVariable String submissionId) {
        String verdictJson = redisTemplate.opsForValue().get(VERDICT_STORE_KEY_PREFIX + submissionId);
        if (verdictJson == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"status\":\"PENDING\",\"submissionId\":\"" + submissionId + "\"}");
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(verdictJson);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}
