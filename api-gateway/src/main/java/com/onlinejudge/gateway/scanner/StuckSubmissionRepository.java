package com.onlinejudge.gateway.scanner;

import com.onlinejudge.gateway.model.Submission;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Repository scoped to the §3.9 reconciliation sweep.
 *
 * <p>Kept separate from {@link com.onlinejudge.gateway.repository.SubmissionRepository}
 * so the hot-path submission writer stays a clean
 * {@code JpaRepository<Submission, UUID>} with zero scanner-specific
 * dependencies. The scanner's queries are deliberately narrow —
 * {@code findStuckSubmissions} drives the partial index added in V8,
 * and the two write operations are single-row updates.
 *
 * <p>Both this repository and {@code SubmissionRepository} bind to the same
 * underlying {@code submissions} table — Spring Data is happy with multiple
 * repositories targeting one entity.
 */
public interface StuckSubmissionRepository extends JpaRepository<Submission, UUID> {

    /**
     * Selects up to {@code batchSize} submissions that are PENDING past
     * {@code staleBefore} and have not yet hit the per-row attempt cap.
     *
     * <p>Ordered by {@code created_at ASC} so the oldest (most-stuck) rows
     * drain first when a backlog has built up. Backed by
     * {@code idx_submissions_stuck_pending} from V8.
     */
    @Query(value = """
            SELECT * FROM submissions
             WHERE status = 'PENDING'
               AND created_at < :staleBefore
               AND reconcile_attempts < :maxAttempts
             ORDER BY created_at ASC
             LIMIT :batchSize
            """, nativeQuery = true)
    List<Submission> findStuckSubmissions(@Param("staleBefore") Instant staleBefore,
                                          @Param("maxAttempts") int maxAttempts,
                                          @Param("batchSize") int batchSize);

    /**
     * Increment the per-row reconcile attempt counter after a successful
     * Kafka republish. A single-row update so it doesn't block the table.
     */
    @Modifying
    @Transactional
    @Query(value = """
            UPDATE submissions
               SET reconcile_attempts = reconcile_attempts + 1
             WHERE id = :id
            """, nativeQuery = true)
    int bumpReconcileAttempts(@Param("id") UUID id);

    /**
     * Terminal failure path: the scanner has exhausted
     * {@code app.reconciliation.max-attempts} republishes for this row.
     * Status flips to FAILED and the row drops out of the scanner's
     * working set forever.
     */
    @Modifying
    @Transactional
    @Query(value = """
            UPDATE submissions
               SET status = 'FAILED'
             WHERE id = :id
            """, nativeQuery = true)
    int markFailed(@Param("id") UUID id);
}
