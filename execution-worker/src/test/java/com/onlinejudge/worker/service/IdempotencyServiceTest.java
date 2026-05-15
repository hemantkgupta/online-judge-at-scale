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

import java.time.Instant;
import java.util.List;
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

    @Mock
    private Query selectQuery;

    @InjectMocks
    private IdempotencyService idempotencyService;

    private String submissionId;

    @BeforeEach
    void setUp() {
        submissionId = UUID.randomUUID().toString();
    }

    @Test
    void claimSubmission_returnsClaimed_whenFirstClaim() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1); // 1 row inserted

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.CLAIMED);
        assertThat(decision.leaseStartedAt()).isNotNull();
    }

    @Test
    void claimSubmission_returnsCompleted_whenDuplicateAlreadyCompleted() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery)
                .thenReturn(selectQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(0);
        when(selectQuery.setParameter(anyString(), any())).thenReturn(selectQuery);
        when(selectQuery.getResultList()).thenReturn(List.of("completed"));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.COMPLETED);
    }

    @Test
    void claimSubmission_returnsInProgress_whenDuplicateStillProcessing() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery)
                .thenReturn(selectQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(0);
        when(selectQuery.setParameter(anyString(), any())).thenReturn(selectQuery);
        when(selectQuery.getResultList()).thenReturn(List.of("processing"));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.IN_PROGRESS);
    }

    @Test
    void claimSubmission_reclaimsStaleProcessingLease() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.CLAIMED);
        verify(entityManager).createNativeQuery(contains("created_at < :staleBefore"));
    }

    @Test
    void claimSubmission_returnsInProgress_onConcurrentRace() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery)
                .thenReturn(selectQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenThrow(
                new DataIntegrityViolationException("duplicate key"));
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(0);
        when(selectQuery.setParameter(anyString(), any())).thenReturn(selectQuery);
        when(selectQuery.getResultList()).thenReturn(List.of("processing"));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.IN_PROGRESS);
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
    void markCompleted_updatesStatusToCompleted() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);
        Instant leaseStartedAt = Instant.now();

        boolean completed = idempotencyService.markCompleted(submissionId, "pretest", leaseStartedAt);

        assertThat(completed).isTrue();
        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("UPDATE") && sql.contains("completed") && sql.contains("created_at")
        ));
        verify(updateQuery).setParameter("key", submissionId + ":pretest");
        verify(updateQuery).setParameter("leaseStartedAt", leaseStartedAt);
    }

    @Test
    void markCompleted_returnsFalseWhenLeaseWasLost() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(0);

        boolean completed = idempotencyService.markCompleted(submissionId, "pretest", Instant.now());

        assertThat(completed).isFalse();
    }
}
