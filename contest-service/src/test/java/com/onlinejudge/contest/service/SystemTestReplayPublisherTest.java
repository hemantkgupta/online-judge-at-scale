package com.onlinejudge.contest.service;

import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.contest.config.KafkaProducerConfig.KafkaTopics;
import com.onlinejudge.contest.dto.AcceptedSubmissionRow;
import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.repository.ProblemContestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SystemTestReplayPublisher} (Roadmap §4.23).
 *
 * Verifies that closing a contest fans out an at-least-once system-phase
 * SubmissionEvent for every ACCEPTED pretest submission to each problem
 * registered in the contest, and that the Kafka payload round-trips through
 * the proto parser the execution-worker uses.
 */
@ExtendWith(MockitoExtension.class)
class SystemTestReplayPublisherTest {

    private static final String SYSTEM_TOPIC = "submissions.system";

    @Mock private ProblemContestRepository problemContestRepository;
    @Mock private SubmissionReplayRepository submissionReplayRepository;
    @Mock private KafkaTemplate<String, byte[]> kafkaTemplate;

    private SystemTestReplayPublisher publisher;
    private Contest contest;

    @BeforeEach
    void setUp() {
        publisher = new SystemTestReplayPublisher(
                problemContestRepository,
                submissionReplayRepository,
                kafkaTemplate,
                new KafkaTopics(SYSTEM_TOPIC, "contest_events"));

        contest = new Contest();
        contest.setId(UUID.randomUUID());
        contest.setTitle("Replay Test Contest");
    }

    @Test
    void replayContest_twoProblemsThreeAcceptedEach_publishesSixSystemEvents() {
        UUID problemA = UUID.randomUUID();
        UUID problemB = UUID.randomUUID();
        when(problemContestRepository.findProblemIdsByContestId(contest.getId()))
                .thenReturn(List.of(problemA, problemB));

        List<AcceptedSubmissionRow> rowsA = buildAcceptedRows(problemA, 3);
        List<AcceptedSubmissionRow> rowsB = buildAcceptedRows(problemB, 3);
        when(submissionReplayRepository.pageAccepted(eq(problemA), any(), any(), anyInt()))
                .thenReturn(rowsA)
                .thenReturn(List.of());
        when(submissionReplayRepository.pageAccepted(eq(problemB), any(), any(), anyInt()))
                .thenReturn(rowsB)
                .thenReturn(List.of());
        when(kafkaTemplate.send(eq(SYSTEM_TOPIC), any(String.class), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        int replayed = publisher.replayContest(contest);

        assertThat(replayed).isEqualTo(6);
        verify(kafkaTemplate, times(6))
                .send(eq(SYSTEM_TOPIC), any(String.class), any(byte[].class));
    }

    @Test
    void replayContest_emptyContest_noSends() {
        when(problemContestRepository.findProblemIdsByContestId(contest.getId()))
                .thenReturn(List.of());

        int replayed = publisher.replayContest(contest);

        assertThat(replayed).isZero();
        verify(kafkaTemplate, never()).send(any(String.class), any(String.class), any(byte[].class));
    }

    @Test
    void replayContest_noAcceptedSubmissions_noSends() {
        UUID problemA = UUID.randomUUID();
        when(problemContestRepository.findProblemIdsByContestId(contest.getId()))
                .thenReturn(List.of(problemA));
        when(submissionReplayRepository.pageAccepted(eq(problemA), any(), any(), anyInt()))
                .thenReturn(List.of());

        int replayed = publisher.replayContest(contest);

        assertThat(replayed).isZero();
        verify(kafkaTemplate, never()).send(any(String.class), any(String.class), any(byte[].class));
    }

    @Test
    void replayContest_kafkaSendFailsPartway_propagatesAndDoesNotSwallow() {
        UUID problemA = UUID.randomUUID();
        when(problemContestRepository.findProblemIdsByContestId(contest.getId()))
                .thenReturn(List.of(problemA));
        when(submissionReplayRepository.pageAccepted(eq(problemA), any(), any(), anyInt()))
                .thenReturn(buildAcceptedRows(problemA, 5))
                .thenReturn(List.of());

        CompletableFuture<Object> bad = new CompletableFuture<>();
        bad.completeExceptionally(new RuntimeException("broker down"));
        java.util.concurrent.atomic.AtomicInteger sendCount = new java.util.concurrent.atomic.AtomicInteger();
        when(kafkaTemplate.send(eq(SYSTEM_TOPIC), any(String.class), any(byte[].class)))
                .thenAnswer(inv -> {
                    int n = sendCount.incrementAndGet();
                    return n == 4 ? bad : CompletableFuture.completedFuture(null);
                });

        assertThatThrownBy(() -> publisher.replayContest(contest))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to publish system replay");

        // Confirm we stopped at the failure rather than blindly trying the 5th.
        verify(kafkaTemplate, times(4))
                .send(eq(SYSTEM_TOPIC), any(String.class), any(byte[].class));
    }

    @Test
    void replayContest_payloadIsValidSubmissionEventWithPhaseSystem() throws Exception {
        UUID problemA = UUID.randomUUID();
        when(problemContestRepository.findProblemIdsByContestId(contest.getId()))
                .thenReturn(List.of(problemA));
        AcceptedSubmissionRow row = buildAcceptedRows(problemA, 1).get(0);
        when(submissionReplayRepository.pageAccepted(eq(problemA), any(), any(), anyInt()))
                .thenReturn(List.of(row))
                .thenReturn(List.of());
        when(kafkaTemplate.send(eq(SYSTEM_TOPIC), any(String.class), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        publisher.replayContest(contest);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(SYSTEM_TOPIC), keyCaptor.capture(), payloadCaptor.capture());

        SubmissionEvent parsed = SubmissionEvent.parseFrom(payloadCaptor.getValue());
        assertThat(parsed.getPhase()).isEqualTo("system");
        assertThat(parsed.getSubmissionId()).isEqualTo(row.id().toString());
        assertThat(parsed.getUserId()).isEqualTo(row.userId().toString());
        assertThat(parsed.getProblemId()).isEqualTo(row.problemId().toString());
        assertThat(parsed.getContestId()).isEqualTo(contest.getId().toString());
        assertThat(parsed.getS3CodeUrl()).isEqualTo(row.s3CodeUrl());
        assertThat(parsed.getLanguage()).isEqualTo(row.language());
        assertThat(parsed.getRegion()).isEqualTo(row.region());
        assertThat(parsed.getGatewayTsMs()).isEqualTo(row.gatewayTsMs());
        // Keyed by user_id for partition affinity with the original write.
        assertThat(keyCaptor.getValue()).isEqualTo(row.userId().toString());
    }

    private List<AcceptedSubmissionRow> buildAcceptedRows(UUID problemId, int count) {
        List<AcceptedSubmissionRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new AcceptedSubmissionRow(
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    problemId,
                    null,  // original contest_id may be null
                    "java",
                    "s3://bucket/code/" + i + ".java",
                    1_700_000_000_000L + i,
                    "us-east-1",
                    1_700_000_000_000L + i
            ));
        }
        return rows;
    }
}
