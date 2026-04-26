package com.onlinejudge.gateway.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

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
    private static final Duration KEY_TTL = Duration.ofHours(24);

    /**
     * Checks if an idempotency key has already been used.
     * If yes, returns the original submission ID.
     * If no, claims the key atomically and returns empty.
     *
     * @param idempotencyKey the client-provided UUID
     * @return the existing submission ID if this is a retry, or empty if first request
     */
    public Optional<String> checkAndClaim(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Optional.empty(); // No idempotency key → proceed normally
        }

        String key = KEY_PREFIX + idempotencyKey;

        // Check if already exists
        String existing = redisTemplate.opsForValue().get(key);
        if (existing != null) {
            log.info("[idempotency] Duplicate request detected: key={} existing={}",
                    idempotencyKey, existing);
            return Optional.of(existing);
        }

        // Not found — claim it atomically with setIfAbsent (NX + EX)
        Boolean claimed = redisTemplate.opsForValue()
                .setIfAbsent(key, "pending", KEY_TTL);

        if (Boolean.FALSE.equals(claimed)) {
            // Race condition: another request claimed it between our GET and SETNX
            String raceWinner = redisTemplate.opsForValue().get(key);
            if (raceWinner != null && !"pending".equals(raceWinner)) {
                return Optional.of(raceWinner);
            }
        }

        return Optional.empty(); // First request — proceed with submission
    }

    /**
     * Records the submission ID for an idempotency key after the submission succeeds.
     * Future retries with the same key will get this submission ID.
     */
    public void recordSubmission(String idempotencyKey, String submissionId) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) return;

        String key = KEY_PREFIX + idempotencyKey;
        redisTemplate.opsForValue().set(key, submissionId, KEY_TTL);
    }
}
