package com.onlinejudge.worker.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the exactly-once idempotency guard.
 *
 * Verifies: first claim succeeds, duplicate (submissionId, phase) is skipped,
 * concurrent claim race is handled gracefully, markCompleted updates status,
 * and the composite key is scoped by phase so the same submission can run
 * once in Phase 1 (pretest) and once in Phase 2 (system).
 */
@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    private EntityManager entityManager;

    @Mock
    private Query insertQuery;

    @Mock
    private Query updateQuery;

    @InjectMocks
    private IdempotencyService idempotencyService;

    private String submissionId;

    @BeforeEach
    void setUp() {
        submissionId = UUID.randomUUID().toString();
    }

    @Test
    void claimSubmission_returnsTrue_whenFirstClaim() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1); // 1 row inserted

        boolean claimed = idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(claimed).isTrue();
    }

    @Test
    void claimSubmission_returnsFalse_whenDuplicate() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);

        boolean claimed = idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(claimed).isFalse();
    }

    @Test
    void claimSubmission_returnsFalse_onConcurrentRace() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenThrow(
                new DataIntegrityViolationException("duplicate key"));

        boolean claimed = idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(claimed).isFalse();
    }

    @Test
    void claimSubmission_usesCorrectSqlWithOnConflict() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        idempotencyService.claimSubmission(submissionId, "pretest");

        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("ON CONFLICT") && sql.contains("DO NOTHING")
        ));
    }

    @Test
    void claimSubmission_keyIsScopedByPhase() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        idempotencyService.claimSubmission(submissionId, "pretest");

        verify(insertQuery).setParameter("key", submissionId + ":pretest");
        verify(insertQuery).setParameter(eq("sid"), eq(UUID.fromString(submissionId)));
    }

    @Test
    void claimSubmission_sameSubmissionDifferentPhase_isAllowed() {
        // The two phases use distinct composite keys, so both can claim the same
        // submissionId — exactly what the Phase 1 → Phase 2 promotion needs.
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        boolean pretestClaim = idempotencyService.claimSubmission(submissionId, "pretest");
        boolean systemClaim  = idempotencyService.claimSubmission(submissionId, "system");

        assertThat(pretestClaim).isTrue();
        assertThat(systemClaim).isTrue();
        verify(insertQuery).setParameter("key", submissionId + ":pretest");
        verify(insertQuery).setParameter("key", submissionId + ":system");
    }

    @Test
    void markCompleted_updatesStatusToCompleted() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);

        idempotencyService.markCompleted(submissionId, "pretest");

        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("UPDATE") && sql.contains("completed")
        ));
        verify(updateQuery).setParameter("key", submissionId + ":pretest");
    }

    @Test
    void claimSubmission_duplicateDetectionPreventsDoubleExecution() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate())
                .thenReturn(1)
                .thenReturn(0);

        boolean first  = idempotencyService.claimSubmission(submissionId, "pretest");
        boolean second = idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }
}
