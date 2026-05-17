package com.onlinejudge.gateway.scanner;

import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.gateway.model.Submission;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the §3.9 stuck-submission reconciliation scanner.
 *
 * <p>All Mockito — no Spring context. The {@code @Scheduled} annotation
 * just needs to compile; we exercise {@link ReconciliationScanner#sweep()}
 * directly.
 */
@ExtendWith(MockitoExtension.class)
class ReconciliationScannerTest {

    private static final String PRETEST_TOPIC = "submissions.pretest";
    private static final String DLQ_TOPIC = "submissions.dlq";
    private static final int MAX_ATTEMPTS = 10;
    private static final int BATCH_SIZE = 500;
    private static final long STALE_AFTER_SECONDS = 900L;

    @Mock
    private StuckSubmissionRepository repo;

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @InjectMocks
    private ReconciliationScanner scanner;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(scanner, "pretestTopic", PRETEST_TOPIC);
        ReflectionTestUtils.setField(scanner, "dlqTopic", DLQ_TOPIC);
        ReflectionTestUtils.setField(scanner, "staleAfterSeconds", STALE_AFTER_SECONDS);
        ReflectionTestUtils.setField(scanner, "batchSize", BATCH_SIZE);
        ReflectionTestUtils.setField(scanner, "maxAttempts", MAX_ATTEMPTS);

        // Default: every send succeeds. Individual tests override.
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private Submission stuckRow(int attempts) {
        Submission s = new Submission();
        s.setId(UUID.randomUUID());
        s.setUserId(UUID.randomUUID());
        s.setProblemId(UUID.randomUUID());
        s.setContestId(UUID.randomUUID());
        s.setLanguage("python");
        s.setS3CodeUrl("gs://bucket/code.py");
        s.setStatus("PENDING");
        s.setGatewayTsMs(1_700_000_000_000L);
        s.setRegion("us-east-1");
        s.setCreatedAt(Instant.now().minusSeconds(1800));
        s.setReconcileAttempts(attempts);
        return s;
    }

    @Test
    void sweep_returnsZeroCountsWhenNoStuckSubmissions() {
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(Collections.emptyList());

        ReconciliationScanner.ScanResult result = scanner.sweep();

        assertThat(result.swept()).isZero();
        assertThat(result.republished()).isZero();
        assertThat(result.dlq()).isZero();
        verifyNoInteractions(kafkaTemplate);
        verify(repo, never()).bumpReconcileAttempts(any());
        verify(repo, never()).markFailed(any());
    }

    @Test
    void sweep_republishesEachStuckRowAndBumpsAttempts() {
        Submission a = stuckRow(0);
        Submission b = stuckRow(2);
        Submission c = stuckRow(5);
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(List.of(a, b, c));

        ReconciliationScanner.ScanResult result = scanner.sweep();

        assertThat(result.swept()).isEqualTo(3);
        assertThat(result.republished()).isEqualTo(3);
        assertThat(result.dlq()).isZero();

        verify(kafkaTemplate).send(eq(PRETEST_TOPIC), eq(a.getUserId().toString()), any(byte[].class));
        verify(kafkaTemplate).send(eq(PRETEST_TOPIC), eq(b.getUserId().toString()), any(byte[].class));
        verify(kafkaTemplate).send(eq(PRETEST_TOPIC), eq(c.getUserId().toString()), any(byte[].class));

        verify(repo).bumpReconcileAttempts(a.getId());
        verify(repo).bumpReconcileAttempts(b.getId());
        verify(repo).bumpReconcileAttempts(c.getId());
        verify(repo, never()).markFailed(any());
    }

    @Test
    void sweep_routesToDlqWhenAttemptCountAtCap() {
        // Simulates the defence-in-depth branch: a row that somehow made
        // it past the SQL filter at the cap. Scanner must mark FAILED +
        // emit DLQ envelope; no pretest send.
        Submission exhausted = stuckRow(MAX_ATTEMPTS);
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(List.of(exhausted));

        ReconciliationScanner.ScanResult result = scanner.sweep();

        assertThat(result.swept()).isEqualTo(1);
        assertThat(result.republished()).isZero();
        assertThat(result.dlq()).isEqualTo(1);

        verify(repo).markFailed(exhausted.getId());
        verify(kafkaTemplate).send(eq(DLQ_TOPIC), eq(exhausted.getId().toString()), any(byte[].class));
        verify(kafkaTemplate, never()).send(eq(PRETEST_TOPIC), anyString(), any(byte[].class));
        verify(repo, never()).bumpReconcileAttempts(any());
    }

    @Test
    void sweep_doesNotBumpAttemptsWhenKafkaSendFails() {
        // Kafka send fails — the catch in sweep() swallows the exception
        // (so one bad row doesn't poison the batch) and crucially does
        // NOT call bumpReconcileAttempts. Next sweep retries the row.
        Submission s = stuckRow(0);
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(List.of(s));
        when(kafkaTemplate.send(eq(PRETEST_TOPIC), eq(s.getUserId().toString()), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));

        ReconciliationScanner.ScanResult result = scanner.sweep();

        assertThat(result.swept()).isEqualTo(1);
        assertThat(result.republished()).isZero();
        assertThat(result.dlq()).isZero();
        verify(repo, never()).bumpReconcileAttempts(any());
        verify(repo, never()).markFailed(any());
    }

    @Test
    void sweep_payloadIsProtobufSubmissionEventWithCorrectFields() throws Exception {
        Submission s = stuckRow(3);
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(List.of(s));

        ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
        scanner.sweep();
        verify(kafkaTemplate).send(eq(PRETEST_TOPIC), eq(s.getUserId().toString()), bytesCaptor.capture());

        SubmissionEvent decoded = SubmissionEvent.parseFrom(bytesCaptor.getValue());
        assertThat(decoded.getSubmissionId()).isEqualTo(s.getId().toString());
        assertThat(decoded.getUserId()).isEqualTo(s.getUserId().toString());
        assertThat(decoded.getProblemId()).isEqualTo(s.getProblemId().toString());
        assertThat(decoded.getContestId()).isEqualTo(s.getContestId().toString());
        assertThat(decoded.getS3CodeUrl()).isEqualTo("gs://bucket/code.py");
        assertThat(decoded.getLanguage()).isEqualTo("python");
        assertThat(decoded.getGatewayTsMs()).isEqualTo(1_700_000_000_000L);
        assertThat(decoded.getRegion()).isEqualTo("us-east-1");
        assertThat(decoded.getPhase()).isEqualTo("pretest");
    }

    @Test
    void sweep_passesConfiguredStaleWindowAndCapsToRepository() {
        // The scanner must hand `now - staleAfterSeconds` and the configured
        // maxAttempts/batchSize through verbatim — these are the levers
        // operators tune in application.yml.
        when(repo.findStuckSubmissions(any(Instant.class), anyInt(), anyInt()))
                .thenReturn(Collections.emptyList());

        Instant before = Instant.now();
        scanner.sweep();
        Instant after = Instant.now();

        ArgumentCaptor<Instant> staleCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(repo).findStuckSubmissions(staleCaptor.capture(), eq(MAX_ATTEMPTS), eq(BATCH_SIZE));
        Instant cutoff = staleCaptor.getValue();
        // Cutoff must sit inside [before-stale, after-stale] — a tight
        // bound that doesn't depend on a fixed clock.
        assertThat(cutoff).isBetween(
                before.minusSeconds(STALE_AFTER_SECONDS),
                after.minusSeconds(STALE_AFTER_SECONDS));
    }

    @Test
    void sweep_oneBadRowDoesNotStopRemainingRows() {
        // One row's send fails, the next two succeed — the scanner must
        // not abort the batch. Bump count: 2 (only successful sends).
        Submission bad = stuckRow(0);
        Submission good1 = stuckRow(0);
        Submission good2 = stuckRow(0);
        when(repo.findStuckSubmissions(any(Instant.class), eq(MAX_ATTEMPTS), eq(BATCH_SIZE)))
                .thenReturn(List.of(bad, good1, good2));
        when(kafkaTemplate.send(eq(PRETEST_TOPIC), eq(bad.getUserId().toString()), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker hiccup")));

        ReconciliationScanner.ScanResult result = scanner.sweep();

        assertThat(result.swept()).isEqualTo(3);
        assertThat(result.republished()).isEqualTo(2);
        assertThat(result.dlq()).isZero();
        verify(repo, times(2)).bumpReconcileAttempts(any(UUID.class));
        verify(repo, never()).bumpReconcileAttempts(bad.getId());
    }
}
