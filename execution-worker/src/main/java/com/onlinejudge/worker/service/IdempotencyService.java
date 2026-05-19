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
 *   <li>{@code poisoned}: terminal failure (attempts cap exceeded). The
 *       submission has been moved to the DLQ; ack and skip.</li>
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
 *
 * <p><b>Roadmap §2.5 — attempts cap.</b> Each row carries an {@code attempts}
 * counter. Every reclaim of a stale row bumps it; once it exceeds
 * {@code app.idempotency.max-attempts}, the next claim returns
 * {@link ClaimStatus#POISON} and the consumer dead-letters the submission
 * instead of looping forever.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final EntityManager entityManager;

    @Value("${app.idempotency.processing-lease-seconds:300}")
    private long processingLeaseSeconds = 300;

    /** Roadmap §2.5: hard upper bound on reclaim cycles before a row is
     *  treated as poison and dead-lettered. The default mirrors the rough
     *  Kafka redelivery budget we want to spend on any single bad event. */
    @Value("${app.idempotency.max-attempts:5}")
    private int maxAttempts = 5;

    public enum ClaimStatus {
        CLAIMED,
        COMPLETED,
        IN_PROGRESS,
        /** Roadmap §2.5: attempts cap exceeded; consumer should DLQ + ack. */
        POISON
    }

    public record ClaimDecision(ClaimStatus status, Instant leaseStartedAt) {
        public boolean claimed() {
            return status == ClaimStatus.CLAIMED;
        }
    }

    @Transactional
    public ClaimDecision claimSubmission(String submissionId, String phase) {
        return claimSubmission(submissionId, phase, null);
    }

    /**
     * Tech-spec §14 L4: variant that accepts a per-problem override for the
     * processing-lease window. When {@code leaseSecondsOverride} is non-null
     * AND within the CRDB CHECK bounds (30..3600 s), it replaces the global
     * {@code app.idempotency.processing-lease-seconds} for the staleness
     * calculation on this single claim. {@code null} or out-of-band values
     * fall back to the global — backwards-compatible with callers that
     * don't pass an override.
     */
    @Transactional
    public ClaimDecision claimSubmission(String submissionId, String phase,
                                         Long leaseSecondsOverride) {
        String compositeKey = compositeKey(submissionId, phase);
        UUID sid = UUID.fromString(submissionId);
        Instant claimedAt = nowForDatabase();

        try {
            int rows = entityManager.createNativeQuery(
                    "INSERT INTO idempotency_keys (key, submission_id, status, attempts, created_at) " +
                    "VALUES (:key, :sid, 'processing', 1, :claimedAt) " +
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

        long effectiveLeaseSeconds = effectiveLeaseSeconds(leaseSecondsOverride, submissionId);
        return claimExisting(compositeKey, submissionId, phase, claimedAt, effectiveLeaseSeconds);
    }

    /**
     * Resolve the lease window for this claim. Override wins when it is
     * non-null and within the CRDB CHECK bounds; otherwise fall back to the
     * global. Out-of-band values are logged + ignored rather than rejected
     * because the worker is downstream of problem-service and a malformed
     * row shouldn't take the consumer offline.
     */
    long effectiveLeaseSeconds(Long override, String submissionId) {
        if (override == null) {
            return processingLeaseSeconds;
        }
        long v = override;
        if (v < 30L || v > 3600L) {
            log.warn("[idempotency] lease override out of bounds submission={} value={} (using global {})",
                    submissionId, v, processingLeaseSeconds);
            return processingLeaseSeconds;
        }
        log.debug("[idempotency] using per-problem lease override submission={} value={}",
                submissionId, v);
        return v;
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

    /**
     * Roadmap §2.5: poison terminal state. Called once we've decided the
     * submission can't be processed (attempts cap exceeded). Distinct
     * from {@code completed} so operators can grep for it in CRDB.
     */
    @Transactional
    public void markPoison(String submissionId, String phase) {
        entityManager.createNativeQuery(
                "UPDATE idempotency_keys SET status = 'poisoned' WHERE key = :key"
        )
        .setParameter("key", compositeKey(submissionId, phase))
        .executeUpdate();
        log.warn("[idempotency] Marked POISONED submission={} phase={}", submissionId, phase);
    }

    /**
     * Roadmap §2.5 + Problem 4: drop the claim entirely so a transient
     * resource-shortage (e.g., sandbox pool exhausted) doesn't burn an
     * idempotency attempt. We DELETE the row instead of just decrementing
     * attempts so the next redelivery gets a fresh INSERT path with
     * attempts=1.
     *
     * @return true if a row was deleted (we genuinely held the claim);
     *         false if some other process had already moved it on
     */
    @Transactional
    public boolean releaseClaim(String submissionId, String phase, Instant leaseStartedAt) {
        int rows = entityManager.createNativeQuery(
                "DELETE FROM idempotency_keys " +
                "WHERE key = :key AND status = 'processing' AND created_at = :leaseStartedAt"
        )
        .setParameter("key", compositeKey(submissionId, phase))
        .setParameter("leaseStartedAt", leaseStartedAt)
        .executeUpdate();

        if (rows == 0) {
            log.warn("[idempotency] releaseClaim ignored — claim lost submission={} phase={}",
                    submissionId, phase);
            return false;
        }
        log.info("[idempotency] Released claim submission={} phase={} (transient backpressure)",
                submissionId, phase);
        return true;
    }

    private ClaimDecision claimExisting(String compositeKey, String submissionId,
                                        String phase, Instant claimedAt,
                                        long effectiveLeaseSeconds) {
        Instant staleBefore = claimedAt.minus(Duration.ofSeconds(effectiveLeaseSeconds));

        // Roadmap §2.5: only reclaim when attempts is still under the cap.
        // The UPDATE bumps attempts atomically; if max-attempts is already
        // reached the row is left alone and the follow-up status read
        // surfaces it for poison handling.
        int reclaimed = entityManager.createNativeQuery(
                "UPDATE idempotency_keys " +
                "SET status = 'processing', created_at = :claimedAt, attempts = attempts + 1 " +
                "WHERE key = :key AND status = 'processing' " +
                "  AND created_at < :staleBefore AND attempts < :maxAttempts"
        )
        .setParameter("key", compositeKey)
        .setParameter("claimedAt", claimedAt)
        .setParameter("staleBefore", staleBefore)
        .setParameter("maxAttempts", maxAttempts)
        .executeUpdate();

        if (reclaimed == 1) {
            log.info("[idempotency] Reclaimed stale claim submission={} phase={}", submissionId, phase);
            return new ClaimDecision(ClaimStatus.CLAIMED, claimedAt);
        }

        Row r = currentRow(compositeKey);
        if (r == null) {
            // Race: row vanished between our INSERT and the SELECT (e.g.
            // someone else just called releaseClaim()). Treat as IN_PROGRESS
            // so Kafka redelivers and we re-INSERT next time.
            return new ClaimDecision(ClaimStatus.IN_PROGRESS, null);
        }
        if ("completed".equals(r.status())) {
            log.info("[idempotency] Skipping completed duplicate submission={} phase={}", submissionId, phase);
            return new ClaimDecision(ClaimStatus.COMPLETED, null);
        }
        if ("poisoned".equals(r.status())) {
            log.warn("[idempotency] Poisoned duplicate submission={} phase={}", submissionId, phase);
            return new ClaimDecision(ClaimStatus.POISON, null);
        }
        // status=processing. If attempts >= cap AND the row is stale,
        // upgrade to POISON. (Non-stale processing means another worker
        // is actively running it — leave that alone.)
        if (r.attempts() >= maxAttempts && r.createdAt() != null
                && r.createdAt().isBefore(staleBefore)) {
            log.warn("[idempotency] Attempts cap reached submission={} phase={} attempts={}",
                    submissionId, phase, r.attempts());
            return new ClaimDecision(ClaimStatus.POISON, null);
        }

        log.info("[idempotency] Submission still processing submission={} phase={} status={} attempts={}",
                submissionId, phase, r.status(), r.attempts());
        return new ClaimDecision(ClaimStatus.IN_PROGRESS, null);
    }

    private record Row(String status, int attempts, Instant createdAt) {}

    @SuppressWarnings("unchecked")
    private Row currentRow(String compositeKey) {
        List<Object[]> rows = entityManager.createNativeQuery(
                "SELECT status, attempts, created_at FROM idempotency_keys WHERE key = :key"
        )
        .setParameter("key", compositeKey)
        .getResultList();

        if (rows.isEmpty()) {
            return null;
        }
        Object[] r = rows.get(0);
        String status = r[0] == null ? "" : String.valueOf(r[0]);
        int attempts  = r[1] == null ? 0  : ((Number) r[1]).intValue();
        Instant createdAt = null;
        if (r.length > 2 && r[2] != null) {
            if (r[2] instanceof Instant ts) {
                createdAt = ts;
            } else if (r[2] instanceof java.sql.Timestamp ts) {
                createdAt = ts.toInstant();
            }
        }
        return new Row(status, attempts, createdAt);
    }

    private static Instant nowForDatabase() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    private static String compositeKey(String submissionId, String phase) {
        return submissionId + ":" + phase;
    }
}
