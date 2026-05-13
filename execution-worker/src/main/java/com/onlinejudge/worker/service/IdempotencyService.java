package com.onlinejudge.worker.service;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Exactly-once guard for execution workers.
 *
 * <p>Before executing any submission, the worker attempts to INSERT a row
 * into {@code idempotency_keys} with ON CONFLICT DO NOTHING semantics.
 *
 * <p>If the INSERT succeeds (rows affected = 1): proceed with execution.
 * If the INSERT conflicts (rows affected = 0): this submission was already
 * processed (for this phase). Skip execution and commit the Kafka offset.
 *
 * <p>The key is scoped by execution <em>phase</em> ({@code pretest} or
 * {@code system}) so the same submission can legitimately run twice — once
 * for pretests during the contest, and once for the full system-test suite
 * after acceptance. Within a phase, Kafka redelivery is deduped.
 *
 * <p>This handles Kafka at-least-once redelivery: if a worker crashes after
 * executing but before committing the offset, the same submission is
 * redelivered. The idempotency check prevents double execution.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final EntityManager entityManager;

    @Transactional
    public boolean claimSubmission(String submissionId, String phase) {
        String compositeKey = compositeKey(submissionId, phase);
        try {
            int rows = entityManager.createNativeQuery(
                    "INSERT INTO idempotency_keys (key, submission_id, status, created_at) " +
                    "VALUES (:key, :sid, 'processing', NOW()) " +
                    "ON CONFLICT (key) DO NOTHING"
            )
            .setParameter("key", compositeKey)
            .setParameter("sid", UUID.fromString(submissionId))
            .executeUpdate();

            if (rows == 0) {
                log.info("[idempotency] Skipping duplicate submission={} phase={}", submissionId, phase);
                return false;
            }
            return true;

        } catch (DataIntegrityViolationException ex) {
            log.info("[idempotency] Concurrent claim lost for submission={} phase={}", submissionId, phase);
            return false;
        }
    }

    @Transactional
    public void markCompleted(String submissionId, String phase) {
        entityManager.createNativeQuery(
                "UPDATE idempotency_keys SET status = 'completed' WHERE key = :key"
        )
        .setParameter("key", compositeKey(submissionId, phase))
        .executeUpdate();
    }

    private static String compositeKey(String submissionId, String phase) {
        return submissionId + ":" + phase;
    }
}
