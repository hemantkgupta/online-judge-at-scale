package com.onlinejudge.worker.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.onlinejudge.common.events.Events.AnalyticsEvent;
import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.worker.service.DockerExecutionService;
import com.onlinejudge.worker.service.ExecutionBackend;
import com.onlinejudge.worker.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Execution worker: polls the regional Kafka submission topics and executes code.
 *
 * <p>Wire format on Kafka is <b>Protobuf</b> (Part 2 of the blog): inbound
 * {@code SubmissionEvent}, outbound {@code VerdictEvent} + {@code AnalyticsEvent}.
 * Schema lives in {@code common/src/main/proto/events.proto}.
 *
 * <p>Two phases run on separate topics with separate consumer groups (per Part 7):
 * <ul>
 *   <li><b>Phase 1 (pretest)</b> — high-priority, sub-2s verdict during the contest.
 *       On {@code ACCEPTED}, the worker enqueues the submission to the system-test topic.</li>
 *   <li><b>Phase 2 (system)</b> — full system tests. Lower-priority, post-pretest or post-contest.</li>
 * </ul>
 *
 * <p>Pull model: each worker polls Kafka only when it has an execution slot free
 * ({@code max-poll-records=1} ensures one submission per consumer thread). When a worker is busy
 * executing, it stops polling — backpressure is automatic.
 *
 * <p>Exactly-once via the idempotency check before executing. Kafka offset is committed only
 * <em>after</em> the verdict is published. The idempotency key is scoped by phase
 * ({@code submissionId:pretest} / {@code submissionId:system}) so a single submission can
 * legitimately run twice — once per phase.
 *
 * <p>Compatibility: the CockroachDB changefeed CDC path (see
 * {@code database/changefeed-setup.sql}) emits CRDB row envelopes as JSON, not
 * proto — when that path is enabled the inbound bytes are JSON. The consumer
 * detects format (proto first, JSON envelope fallback) so both opt-in paths work.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubmissionConsumer {

    private static final String PHASE_PRETEST = "pretest";
    private static final String PHASE_SYSTEM  = "system";

    private final IdempotencyService idempotencyService;
    private final ExecutionBackend executionService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.evaluated-results}")
    private String evaluatedResultsTopic;

    @Value("${app.kafka.topic.analytics}")
    private String analyticsTopic;

    @Value("${app.kafka.topic.system}")
    private String systemTestTopic;

    /** Phase 1: high-priority pretest pipeline (live during the contest). */
    @KafkaListener(
        topics = "${app.kafka.topic.pretest}",
        groupId = "execution-worker-pretest",
        concurrency = "4"
    )
    public void consumePretest(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        processSubmission(record, ack, PHASE_PRETEST);
    }

    /**
     * Phase 2: full system-test pipeline (deferred). Triggered automatically when a
     * Phase 1 submission is ACCEPTED, or replayable post-contest by re-publishing the
     * original submission events to {@code submissions.system}.
     */
    @KafkaListener(
        topics = "${app.kafka.topic.system}",
        groupId = "execution-worker-system",
        concurrency = "2"
    )
    public void consumeSystem(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        processSubmission(record, ack, PHASE_SYSTEM);
    }

    private void processSubmission(ConsumerRecord<String, byte[]> record,
                                    Acknowledgment ack,
                                    String phase) {
        String submissionId = null;
        try {
            SubmissionEvent event = parseSubmissionEvent(record.value());
            submissionId      = event.getSubmissionId();
            String userId    = event.getUserId();
            String problemId = event.getProblemId();
            String contestId = event.getContestId();
            String language  = event.getLanguage();
            long gatewayTsMs = event.getGatewayTsMs();
            String region    = event.getRegion();

            log.info("[worker:{}] Received submission={} user={} lang={} region={}",
                    phase, submissionId, userId, language, region);

            // Idempotency check: skip if this submission was already processed FOR THIS PHASE.
            if (!idempotencyService.claimSubmission(submissionId, phase)) {
                ack.acknowledge();
                return;
            }

            String sampleCode  = getSampleCode(language);
            String sampleInput = "5\n3 1 4 1 5";
            DockerExecutionService.ExecutionResult result =
                    executionService.execute(submissionId, language, sampleCode, sampleInput);

            String verdict = determineVerdict(result, 2000);
            int points = verdict.equals("ACCEPTED") ? 100 : 0;

            log.info("[worker:{}] Verdict submission={} result={} time={}ms",
                    phase, submissionId, verdict, result.executionTimeMs());

            // Publish verdict to evaluated_results topic (keyed by userId for Flink ordering).
            VerdictEvent verdictEvent = VerdictEvent.newBuilder()
                    .setSubmissionId(submissionId)
                    .setUserId(userId)
                    .setProblemId(problemId)
                    .setContestId(contestId == null ? "" : contestId)
                    .setResult(verdict)
                    .setExecutionTimeMs(result.executionTimeMs())
                    .setMemoryUsedMb(result.memoryUsedMb())
                    .setGatewayTsMs(gatewayTsMs)
                    .setPoints(points)
                    .setPhase(phase)
                    .setRegion(region)
                    .setEventTsMs(gatewayTsMs)
                    .build();
            kafkaTemplate.send(evaluatedResultsTopic, userId, verdictEvent.toByteArray());

            // Analytics (fire-and-forget, separate consumer group).
            AnalyticsEvent analyticsEvent = AnalyticsEvent.newBuilder()
                    .setSubmissionId(submissionId)
                    .setUserId(userId)
                    .setProblemId(problemId)
                    .setContestId(contestId == null ? "" : contestId)
                    .setLanguage(language)
                    .setVerdict(verdict)
                    .setExecutionTimeMs(result.executionTimeMs())
                    .setMemoryUsedMb(result.memoryUsedMb())
                    .setEventTsMs(System.currentTimeMillis())
                    .setRegion(region)
                    .setPhase(phase)
                    .build();
            kafkaTemplate.send(analyticsTopic, submissionId, analyticsEvent.toByteArray());

            // Phase 1 → Phase 2 promotion: when a pretest passes, enqueue to the
            // system-test topic for the full suite. Re-serialize the SubmissionEvent
            // with phase=system so the downstream consumer sees the correct phase
            // in tracing/logs even before the consumer's own phase label kicks in.
            if (phase.equals(PHASE_PRETEST) && verdict.equals("ACCEPTED")) {
                SubmissionEvent system = event.toBuilder().setPhase(PHASE_SYSTEM).build();
                kafkaTemplate.send(systemTestTopic, userId, system.toByteArray());
                log.info("[worker:pretest] Submission {} accepted; enqueued to {} for Phase 2",
                        submissionId, systemTestTopic);
            }

            idempotencyService.markCompleted(submissionId, phase);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[worker:{}] Error processing submission={}: {}",
                    phase, submissionId, ex.getMessage(), ex);
            // Do NOT ack — Kafka will redeliver. Idempotency key prevents double execution.
        }
    }

    /**
     * Parses the Kafka record bytes as a {@link SubmissionEvent}. Two producers
     * write to {@code submissions.pretest} / {@code submissions.system}:
     *
     * <ul>
     *   <li><b>API gateway polling outbox publisher</b> (default) — emits raw
     *       proto bytes directly. {@link SubmissionEvent#parseFrom(byte[])} succeeds.</li>
     *   <li><b>CockroachDB native changefeed</b> (opt-in, see
     *       {@code database/changefeed-setup.sql}) — emits a JSON envelope
     *       {@code {"after": {..., "payload": "<json-string>"}}}. The submission
     *       payload is a JSON string inside the {@code payload} column. We
     *       transcode that JSON to a {@link SubmissionEvent} in memory so the
     *       rest of the consumer treats both paths identically.</li>
     * </ul>
     */
    SubmissionEvent parseSubmissionEvent(byte[] raw) throws Exception {
        // Fast path: real proto from the polling publisher.
        try {
            return SubmissionEvent.parseFrom(raw);
        } catch (InvalidProtocolBufferException ignored) {
            // Fall through to JSON envelope detection.
        }

        JsonNode node = objectMapper.readTree(raw);
        JsonNode inner;
        if (node.has("after") && node.get("after").has("payload")) {
            // CRDB changefeed envelope: {"after": {"payload": "<json>", ...}}
            inner = objectMapper.readTree(node.get("after").get("payload").asText());
        } else {
            inner = node;
        }
        return SubmissionEvent.newBuilder()
                .setSubmissionId(inner.path("submissionId").asText(""))
                .setUserId(inner.path("userId").asText(""))
                .setProblemId(inner.path("problemId").asText(""))
                .setContestId(inner.path("contestId").asText(""))
                .setS3CodeUrl(inner.path("s3CodeUrl").asText(""))
                .setLanguage(inner.path("language").asText(""))
                .setGatewayTsMs(inner.path("gatewayTsMs").asLong(0L))
                .setRegion(inner.path("region").asText(""))
                .setPhase("pretest")
                .build();
    }

    private String determineVerdict(DockerExecutionService.ExecutionResult result, int timeLimitMs) {
        return switch (result.status()) {
            case "TIME_LIMIT_EXCEEDED" -> "TIME_LIMIT_EXCEEDED";
            case "RUNTIME_ERROR"       -> "RUNTIME_ERROR";
            case "INTERNAL_ERROR"      -> "RUNTIME_ERROR";
            case "OK" -> result.executionTimeMs() > timeLimitMs
                    ? "TIME_LIMIT_EXCEEDED" : "ACCEPTED";
            default -> "WRONG_ANSWER";
        };
    }

    // Placeholder: real worker fetches code from S3/R2 using s3CodeUrl from event
    private String getSampleCode(String language) {
        return switch (language) {
            case "python" -> "import sys\ndata = sys.stdin.read().split()\nprint(sum(int(x) for x in data[1:]))";
            case "java"   -> "import java.util.*; public class Solution { public static void main(String[] a) { Scanner sc = new Scanner(System.in); int n=sc.nextInt(); long s=0; for(int i=0;i<n;i++) s+=sc.nextInt(); System.out.println(s); } }";
            default       -> "print(42)";
        };
    }
}
