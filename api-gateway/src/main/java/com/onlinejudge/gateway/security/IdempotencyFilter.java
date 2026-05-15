package com.onlinejudge.gateway.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Client-side idempotency guard at the HTTP boundary.
 *
 * Protects against the case where a contestant clicks "Submit" and their
 * browser's connection drops before the 202 Accepted response arrives.
 * The browser retries with the same Idempotency-Key header. Without this
 * guard, the retry creates a duplicate submission.
 *
 * The client generates a UUID idempotency key before sending the request
 * and includes it as `Idempotency-Key: <uuid>`. This component:
 *
 * 1. Checks Redis for the key. If present, returns the original submission ID
 *    (the retry gets the same response as the original request).
 * 2. If not present, atomically sets the key with a 24-hour TTL.
 *    The caller proceeds with the submission.
 *
 * Redis is used (not CockroachDB) because:
 * - The check must be sub-1ms to fit in the gateway's latency budget
 * - Loss of a Redis key (restart) causes a duplicate, not data loss
 * - The 24-hour TTL auto-cleans stale keys
 *
 * This is Layer 1 of the two-layer idempotency system:
 *   Layer 1: Client-side (here) — HTTP request deduplication
 *   Layer 2: Consumer-side (IdempotencyService) — Kafka redelivery deduplication
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotencyFilter {

    private final StringRedisTemplate redisTemplate;

    private static final String KEY_PREFIX = "idempotency:";
    private static final String PENDING_VALUE = "pending";
    private static final String COMPLETED_PREFIX = "completed:";
    private static final Duration KEY_TTL = Duration.ofHours(24);

    public enum ClaimStatus {
        PROCEED,
        IN_PROGRESS,
        COMPLETED
    }

    public record ClaimResult(ClaimStatus status, String submissionId, long gatewayTsMs) {
        public static ClaimResult proceed() {
            return new ClaimResult(ClaimStatus.PROCEED, null, 0L);
        }

        public static ClaimResult inProgress() {
            return new ClaimResult(ClaimStatus.IN_PROGRESS, null, 0L);
        }

        public static ClaimResult completed(String submissionId, long gatewayTsMs) {
            return new ClaimResult(ClaimStatus.COMPLETED, submissionId, gatewayTsMs);
        }
    }

    /**
     * Checks if an idempotency key has already been used.
     * If completed, returns the original submission ID.
     * If in progress, tells the caller not to create another submission.
     * If absent, claims the key atomically and allows the caller to proceed.
     *
     * @param idempotencyKey the client-provided UUID
     * @return the claim state for this request
     */
    public ClaimResult checkAndClaim(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return ClaimResult.proceed(); // No idempotency key -> proceed normally
        }

        String key = KEY_PREFIX + idempotencyKey;

        // Check if already exists
        String existing = redisTemplate.opsForValue().get(key);
        if (existing != null) {
            log.info("[idempotency] Duplicate request detected: key={} existing={}",
                    idempotencyKey, existing);
            return resultFor(existing);
        }

        // Not found — claim it atomically with setIfAbsent (NX + EX)
        Boolean claimed = redisTemplate.opsForValue()
                .setIfAbsent(key, PENDING_VALUE, KEY_TTL);

        if (Boolean.TRUE.equals(claimed)) {
            return ClaimResult.proceed(); // First request — proceed with submission
        }

        if (Boolean.FALSE.equals(claimed)) {
            // Race condition: another request claimed it between our GET and SETNX
            String raceWinner = redisTemplate.opsForValue().get(key);
            if (raceWinner != null) {
                return resultFor(raceWinner);
            }
        }

        return ClaimResult.inProgress();
    }

    /**
     * Records the submission ID for an idempotency key after the submission succeeds.
     * Future retries with the same key will get this submission ID.
     */
    public void recordSubmission(String idempotencyKey, String submissionId, long gatewayTsMs) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) return;

        String key = KEY_PREFIX + idempotencyKey;
        redisTemplate.opsForValue().set(key, COMPLETED_PREFIX + submissionId + ":" + gatewayTsMs, KEY_TTL);
    }

    public void recordSubmission(String idempotencyKey, String submissionId) {
        recordSubmission(idempotencyKey, submissionId, 0L);
    }

    /**
     * Releases a pending claim when the request fails before a submission is created.
     */
    public void releaseClaim(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) return;

        String key = KEY_PREFIX + idempotencyKey;
        String existing = redisTemplate.opsForValue().get(key);
        if (PENDING_VALUE.equals(existing)) {
            redisTemplate.delete(key);
        }
    }

    private static ClaimResult resultFor(String value) {
        if (PENDING_VALUE.equals(value)) {
            return ClaimResult.inProgress();
        }
        if (value.startsWith(COMPLETED_PREFIX)) {
            String[] parts = value.split(":", 3);
            if (parts.length == 3) {
                try {
                    return ClaimResult.completed(parts[1], Long.parseLong(parts[2]));
                } catch (NumberFormatException ignored) {
                    return ClaimResult.completed(parts[1], 0L);
                }
            }
        }
        return ClaimResult.completed(value, 0L);
    }
}
