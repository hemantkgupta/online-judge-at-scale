package com.onlinejudge.contest.service;

import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.contest.config.KafkaProducerConfig.KafkaTopics;
import com.onlinejudge.contest.dto.AcceptedSubmissionRow;
import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.repository.ProblemContestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Roadmap §4.23: when a contest flips to {@code CLOSED}, walk every
 * ACCEPTED pretest submission for the contest's problems and re-publish them
 * as {@code phase="system"} {@link SubmissionEvent}s on the
 * {@code submissions.system} topic. The execution-worker's Phase 2 consumer
 * picks them up and runs the full system-test suite; final scoring is the
 * system-phase verdict.
 *
 * <h3>Idempotency</h3>
 * The worker's phase-scoped IdempotencyService keys are
 * {@code submissionId:system}, so re-publishing the same submission twice
 * is safe — the second claim path no-ops. We therefore favour at-least-once
 * publish over the complexity of a publish-side dedupe table.
 *
 * <h3>Backpressure</h3>
 * The scan is paginated ({@link SubmissionReplayRepository#PAGE_SIZE} rows
 * at a time) so a contest with millions of submissions doesn't hold a single
 * giant ResultSet open. Each Kafka send is awaited synchronously so a broker
 * stall halts the scan rather than buffering unboundedly in the producer's
 * accumulator.
 *
 * <h3>Failure semantics</h3>
 * If a send fails partway, the method propagates the exception. The
 * partially-replayed submissions remain enqueued; the worker's idempotency
 * guard catches the duplicate when the replay is retried.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SystemTestReplayPublisher {

    private static final String PHASE_SYSTEM = "system";
    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final ProblemContestRepository problemContestRepository;
    private final SubmissionReplayRepository submissionReplayRepository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final KafkaTopics topics;

    /**
     * Replays every ACCEPTED pretest submission for the given contest's
     * problems as {@code phase="system"} events on {@code submissions.system}.
     *
     * @param contest the contest that just transitioned to CLOSED
     * @return the number of submissions successfully re-published
     */
    public int replayContest(Contest contest) {
        UUID contestId = contest.getId();
        List<UUID> problemIds = problemContestRepository.findProblemIdsByContestId(contestId);

        if (problemIds.isEmpty()) {
            log.info("[replay] Contest {} has no problems in contest_problems; nothing to replay",
                    contestId);
            return 0;
        }

        int totalReplayed = 0;
        for (UUID problemId : problemIds) {
            totalReplayed += replayProblem(contestId, problemId);
        }

        log.info("[replay] replayed contest={} problems={} submissions={}",
                contestId, problemIds.size(), totalReplayed);
        return totalReplayed;
    }

    private int replayProblem(UUID contestId, UUID problemId) {
        int replayed = 0;
        Long cursorCreatedAtMs = null;
        UUID cursorId = null;

        while (true) {
            List<AcceptedSubmissionRow> page = submissionReplayRepository.pageAccepted(
                    problemId, cursorCreatedAtMs, cursorId, SubmissionReplayRepository.PAGE_SIZE);

            if (page.isEmpty()) break;

            for (AcceptedSubmissionRow row : page) {
                publish(contestId, row);
                replayed++;
            }

            if (page.size() < SubmissionReplayRepository.PAGE_SIZE) break;

            AcceptedSubmissionRow last = page.get(page.size() - 1);
            cursorCreatedAtMs = last.createdAtMs();
            cursorId = last.id();
        }

        return replayed;
    }

    private void publish(UUID replayContestId, AcceptedSubmissionRow row) {
        SubmissionEvent event = SubmissionEvent.newBuilder()
                .setSubmissionId(row.id().toString())
                .setUserId(row.userId().toString())
                .setProblemId(row.problemId().toString())
                // Tag with the replaying contest's ID so downstream telemetry
                // associates the system-test verdict with the correct contest,
                // even if the original submission was filed against null or
                // another contest.
                .setContestId(replayContestId.toString())
                .setS3CodeUrl(row.s3CodeUrl() == null ? "" : row.s3CodeUrl())
                .setLanguage(row.language() == null ? "" : row.language())
                .setGatewayTsMs(row.gatewayTsMs())
                .setRegion(row.region() == null ? "" : row.region())
                .setPhase(PHASE_SYSTEM)
                .build();

        byte[] payload = event.toByteArray();
        // Key by user_id to keep one user's submissions on the same partition,
        // mirroring the gateway's original write-side keying.
        Future<?> future = kafkaTemplate.send(topics.systemTopic(),
                row.userId().toString(), payload);
        awaitSend(future, row.id());
    }

    /**
     * Block until the broker acks the send (or fails) so a partial broker
     * outage halts the replay loop rather than buffering unboundedly in the
     * producer accumulator.
     */
    private void awaitSend(Future<?> future, UUID submissionId) {
        if (future == null) return;
        try {
            future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted publishing system replay for " + submissionId, ie);
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException("Failed to publish system replay for " + submissionId, ex);
        }
    }
}
