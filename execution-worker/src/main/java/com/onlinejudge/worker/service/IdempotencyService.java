package com.onlinejudge.worker.service;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Exactly-once guard for execution workers.
 *
 * <p>Before executing any submission, the worker attempts to claim a phase-scoped
 * row in {@code idempotency_keys}. Existing rows are interpreted by status:
 *
 * <ul>
 *   <li>{@code completed}: the phase finished and Kafka can ack the duplicate.</li>
 *   <li>{@code processing}: another worker may still be running it; do not ack.</li>
 *   <li>stale {@code processing}: reclaim the row and retry execution.</li>
 * </ul>
 *
 * <p>The key is scoped by execution <em>phase</em> ({@code pretest} or
 * {@code system}) so the same submission can legitimately run twice — once
 * for pretests during the contest, and once for the full system-test suite
 * after acceptance. Within a phase, Kafka redelivery is deduped.
 *
 * <p>This handles Kafka at-least-once redelivery: if a worker crashes while
 * {@code processing}, the row eventually becomes stale and can be retried
 * instead of being mistaken for a completed duplicate.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final EntityManager entityManager;

    @Value("${app.idempotency.processing-lease-seconds:300}")
    private long processingLeaseSeconds = 300;

    public enum ClaimStatus {
        CLAIMED,
        COMPLETED,
        IN_PROGRESS
    }

    public record ClaimDecision(ClaimStatus status, Instant leaseStartedAt) {
        public boolean claimed() {
            return status == ClaimStatus.CLAIMED;
        }
    }

    @Transactional
    public ClaimDecision claimSubmission(String submissionId, String phase) {
        String compositeKey = compositeKey(submissionId, phase);
        UUID sid = UUID.fromString(submissionId);
        Instant claimedAt = nowForDatabase();

        try {
            int rows = entityManager.createNativeQuery(
                    "INSERT INTO idempotency_keys (key, submission_id, status, created_at) " +
                    "VALUES (:key, :sid, 'processing', :claimedAt) " +
                    "ON CONFLICT (key) DO NOTHING"
            )
            .setParameter("key", compositeKey)
            .setParameter("sid", sid)
            .setParameter("claimedAt", claimedAt)
            .executeUpdate();

            if (rows == 1) {
                return new ClaimDecision(ClaimStatus.CLAIMED, claimedAt);
            }

        } catch (DataIntegrityViolationException ex) {
            log.info("[idempotency] Concurrent claim lost for submission={} phase={}", submissionId, phase);
        }

        return claimExisting(compositeKey, submissionId, phase, claimedAt);
    }

    @Transactional
    public boolean markCompleted(String submissionId, String phase, Instant leaseStartedAt) {
        int rows = entityManager.createNativeQuery(
                "UPDATE idempotency_keys SET status = 'completed' " +
                "WHERE key = :key AND status = 'processing' AND created_at = :leaseStartedAt"
        )
        .setParameter("key", compositeKey(submissionId, phase))
        .setParameter("leaseStartedAt", leaseStartedAt)
        .executeUpdate();

        if (rows == 0) {
            log.warn("[idempotency] Completion ignored for stale/lost claim submission={} phase={}",
                    submissionId, phase);
            return false;
        }
        return true;
    }

    private ClaimDecision claimExisting(String compositeKey, String submissionId,
                                        String phase, Instant claimedAt) {
        Instant staleBefore = claimedAt.minus(Duration.ofSeconds(processingLeaseSeconds));
        int reclaimed = entityManager.createNativeQuery(
                "UPDATE idempotency_keys SET status = 'processing', created_at = :claimedAt " +
                "WHERE key = :key AND status = 'processing' AND created_at < :staleBefore"
        )
        .setParameter("key", compositeKey)
        .setParameter("claimedAt", claimedAt)
        .setParameter("staleBefore", staleBefore)
        .executeUpdate();

        if (reclaimed == 1) {
            log.info("[idempotency] Reclaimed stale claim submission={} phase={}", submissionId, phase);
            return new ClaimDecision(ClaimStatus.CLAIMED, claimedAt);
        }

        String status = currentStatus(compositeKey);
        if ("completed".equals(status)) {
            log.info("[idempotency] Skipping completed duplicate submission={} phase={}", submissionId, phase);
            return new ClaimDecision(ClaimStatus.COMPLETED, null);
        }

        log.info("[idempotency] Submission still processing submission={} phase={} status={}",
                submissionId, phase, status);
        return new ClaimDecision(ClaimStatus.IN_PROGRESS, null);
    }

    private String currentStatus(String compositeKey) {
        @SuppressWarnings("unchecked")
        List<Object> rows = entityManager.createNativeQuery(
                "SELECT status FROM idempotency_keys WHERE key = :key"
        )
        .setParameter("key", compositeKey)
        .getResultList();

        if (rows.isEmpty()) {
            return null;
        }
        return String.valueOf(rows.get(0));
    }

    private static Instant nowForDatabase() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    private static String compositeKey(String submissionId, String phase) {
        return submissionId + ":" + phase;
    }
}
