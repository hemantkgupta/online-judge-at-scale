package com.onlinejudge.worker.service;

import com.onlinejudge.worker.model.IdempotencyKey;
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
 * Before executing any submission, the worker attempts to INSERT a row
 * into idempotency_keys with ON CONFLICT DO NOTHING semantics.
 *
 * If the INSERT succeeds (rows affected = 1): proceed with execution.
 * If the INSERT conflicts (rows affected = 0): this submission was already
 * processed. Skip execution and commit the Kafka offset.
 *
 * This handles Kafka at-least-once redelivery: if a worker crashes after
 * executing but before committing the offset, the same submission is
 * redelivered. The idempotency check prevents double execution.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final EntityManager entityManager;

    @Transactional
    public boolean claimSubmission(String submissionId) {
        try {
            // Native upsert: INSERT ... ON CONFLICT DO NOTHING
            int rows = entityManager.createNativeQuery(
                    "INSERT INTO idempotency_keys (key, submission_id, status, created_at) " +
                    "VALUES (:key, :sid, 'processing', NOW()) " +
                    "ON CONFLICT (key) DO NOTHING"
            )
            .setParameter("key", submissionId)
            .setParameter("sid", UUID.fromString(submissionId))
            .executeUpdate();

            if (rows == 0) {
                log.info("[idempotency] Skipping duplicate submission={}", submissionId);
                return false;
            }
            return true;

        } catch (DataIntegrityViolationException ex) {
            // Race between two workers for the same submission - the other worker won
            log.info("[idempotency] Concurrent claim lost for submission={}", submissionId);
            return false;
        }
    }

    @Transactional
    public void markCompleted(String submissionId) {
        entityManager.createNativeQuery(
                "UPDATE idempotency_keys SET status = 'completed' WHERE key = :key"
        )
        .setParameter("key", submissionId)
        .executeUpdate();
    }
}
