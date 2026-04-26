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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for the exactly-once idempotency guard.
 *
 * Verifies: first claim succeeds, duplicate submission_id is skipped,
 * concurrent claim race is handled gracefully, markCompleted updates status.
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

        boolean claimed = idempotencyService.claimSubmission(submissionId);

        assertThat(claimed).isTrue();
    }

    @Test
    void claimSubmission_returnsFalse_whenDuplicate() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0); // 0 rows = ON CONFLICT DO NOTHING fired

        boolean claimed = idempotencyService.claimSubmission(submissionId);

        assertThat(claimed).isFalse();
    }

    @Test
    void claimSubmission_returnsFalse_onConcurrentRace() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenThrow(
                new DataIntegrityViolationException("duplicate key"));

        boolean claimed = idempotencyService.claimSubmission(submissionId);

        assertThat(claimed).isFalse();
    }

    @Test
    void claimSubmission_usesCorrectSqlWithOnConflict() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        idempotencyService.claimSubmission(submissionId);

        // Verify the SQL contains ON CONFLICT DO NOTHING
        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("ON CONFLICT") && sql.contains("DO NOTHING")
        ));
    }

    @Test
    void claimSubmission_setsKeyAndSubmissionIdParameters() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        idempotencyService.claimSubmission(submissionId);

        verify(insertQuery).setParameter("key", submissionId);
        verify(insertQuery).setParameter(eq("sid"), eq(UUID.fromString(submissionId)));
    }

    @Test
    void markCompleted_updatesStatusToCompleted() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);

        idempotencyService.markCompleted(submissionId);

        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("UPDATE") && sql.contains("completed")
        ));
        verify(updateQuery).setParameter("key", submissionId);
    }

    @Test
    void claimSubmission_duplicateDetectionPreventsDoubleExecution() {
        // Simulate: first call succeeds, second call (same submissionId) detects duplicate
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate())
                .thenReturn(1)   // First claim succeeds
                .thenReturn(0);  // Second claim blocked by ON CONFLICT

        boolean first = idempotencyService.claimSubmission(submissionId);
        boolean second = idempotencyService.claimSubmission(submissionId);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }
}
