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
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;
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
 *
 * Roadmap §2.5 additions: reclaim is bounded by an attempts counter, the
 * service exposes releaseClaim() for transient-shortage cleanup, and
 * markPoison() flips the row to a DLQ-friendly terminal state.
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
        // Mirrors the @Value default — Mockito @InjectMocks doesn't run
        // the field initializer expression past primitive defaults.
        ReflectionTestUtils.setField(idempotencyService, "maxAttempts", 5);
        ReflectionTestUtils.setField(idempotencyService, "processingLeaseSeconds", 300L);
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
        when(selectQuery.getResultList()).thenReturn(java.util.Collections.singletonList(
                new Object[] { "completed", 1, Timestamp.from(Instant.now()) }));

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
        when(selectQuery.getResultList()).thenReturn(java.util.Collections.singletonList(
                new Object[] { "processing", 1, Timestamp.from(Instant.now()) }));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.IN_PROGRESS);
    }

    @Test
    void claimSubmission_reclaimsStaleProcessingLease_whileBelowCap() {
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
        verify(entityManager).createNativeQuery(contains("attempts < :maxAttempts"));
    }

    @Test
    void claimSubmission_returnsPoison_whenAttemptsCapExceededAndRowStale() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery)
                .thenReturn(selectQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        // Reclaim attempt rejected because attempts >= cap.
        when(updateQuery.executeUpdate()).thenReturn(0);
        when(selectQuery.setParameter(anyString(), any())).thenReturn(selectQuery);
        // A row with attempts=5 (>= cap) and a stale created_at.
        Instant stale = Instant.now().minusSeconds(3_600);
        when(selectQuery.getResultList()).thenReturn(java.util.Collections.singletonList(
                new Object[] { "processing", 5, Timestamp.from(stale) }));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.POISON);
    }

    @Test
    void claimSubmission_returnsPoison_whenStatusAlreadyPoisoned() {
        when(entityManager.createNativeQuery(anyString()))
                .thenReturn(insertQuery)
                .thenReturn(updateQuery)
                .thenReturn(selectQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(0);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(0);
        when(selectQuery.setParameter(anyString(), any())).thenReturn(selectQuery);
        when(selectQuery.getResultList()).thenReturn(java.util.Collections.singletonList(
                new Object[] { "poisoned", 5, Timestamp.from(Instant.now()) }));

        IdempotencyService.ClaimDecision decision =
                idempotencyService.claimSubmission(submissionId, "pretest");

        assertThat(decision.status()).isEqualTo(IdempotencyService.ClaimStatus.POISON);
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
        when(selectQuery.getResultList()).thenReturn(java.util.Collections.singletonList(
                new Object[] { "processing", 1, Timestamp.from(Instant.now()) }));

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
                        && sql.contains("attempts")
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

    @Test
    void releaseClaim_deletesProcessingRow() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);

        boolean released = idempotencyService.releaseClaim(submissionId, "pretest", Instant.now());

        assertThat(released).isTrue();
        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("DELETE") && sql.contains("status = 'processing'")
        ));
    }

    @Test
    void effectiveLeaseSeconds_usesOverrideWhenInBand() {
        // Default lease = 300; an in-band override (60..3600) should win.
        long resolved = idempotencyService.effectiveLeaseSeconds(900L, submissionId);
        assertThat(resolved).isEqualTo(900L);
    }

    @Test
    void effectiveLeaseSeconds_fallsBackToGlobalForNullOrOutOfBoundOverride() {
        // Null → global.
        assertThat(idempotencyService.effectiveLeaseSeconds(null, submissionId)).isEqualTo(300L);
        // Out-of-band low (CRDB CHECK lower bound is 30) → global, not propagated.
        assertThat(idempotencyService.effectiveLeaseSeconds(5L, submissionId)).isEqualTo(300L);
        // Out-of-band high (CRDB CHECK upper bound is 3600) → global.
        assertThat(idempotencyService.effectiveLeaseSeconds(99_999L, submissionId)).isEqualTo(300L);
    }

    @Test
    void claimSubmission_threeArgOverloadIsBackwardsCompatible() {
        // The two-arg call must continue to behave exactly like the new
        // three-arg form with a null override — backwards compat for any
        // caller that hasn't been redeployed past tech-spec §14 L4.
        when(entityManager.createNativeQuery(anyString())).thenReturn(insertQuery);
        when(insertQuery.setParameter(anyString(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);

        IdempotencyService.ClaimDecision twoArg =
                idempotencyService.claimSubmission(submissionId, "pretest");
        IdempotencyService.ClaimDecision threeArgNull =
                idempotencyService.claimSubmission(submissionId, "pretest", null);

        assertThat(twoArg.status()).isEqualTo(IdempotencyService.ClaimStatus.CLAIMED);
        assertThat(threeArgNull.status()).isEqualTo(IdempotencyService.ClaimStatus.CLAIMED);
    }

    @Test
    void markPoison_flipsStatusToPoisoned() {
        when(entityManager.createNativeQuery(anyString())).thenReturn(updateQuery);
        when(updateQuery.setParameter(anyString(), any())).thenReturn(updateQuery);
        when(updateQuery.executeUpdate()).thenReturn(1);

        idempotencyService.markPoison(submissionId, "pretest");

        verify(entityManager).createNativeQuery(argThat(sql ->
                sql.contains("UPDATE") && sql.contains("'poisoned'")
        ));
    }
}
