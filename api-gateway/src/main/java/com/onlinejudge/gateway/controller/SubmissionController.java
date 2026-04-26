package com.onlinejudge.gateway.controller;

import com.onlinejudge.gateway.dto.SubmissionRequest;
import com.onlinejudge.gateway.dto.SubmissionResponse;
import com.onlinejudge.gateway.service.RateLimitService;
import com.onlinejudge.gateway.service.SubmissionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/submissions")
@RequiredArgsConstructor
public class SubmissionController {

    private final SubmissionService submissionService;
    private final RateLimitService rateLimitService;

    /**
     * POST /api/v1/submissions
     *
     * Returns 202 Accepted immediately after durably persisting the submission.
     * The actual code execution is asynchronous. The verdict is delivered
     * via WebSocket (connect to /ws/leaderboard).
     *
     * Rate limit: 10 submissions per user per minute (429 if exceeded).
     */
    @PostMapping
    public ResponseEntity<?> submit(@Valid @RequestBody SubmissionRequest request) throws Exception {

        // Rate limit check (Redis sliding window per userId)
        if (!rateLimitService.isAllowed(request.getUserId())) {
            log.warn("[gateway] Rate limit exceeded for user={}", request.getUserId());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body("Rate limit exceeded: max 10 submissions per minute");
        }

        SubmissionResponse response = submissionService.accept(request);
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}
