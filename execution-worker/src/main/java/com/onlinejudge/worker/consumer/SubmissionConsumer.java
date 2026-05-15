package com.onlinejudge.worker.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.onlinejudge.common.events.Events.AnalyticsEvent;
import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.DockerExecutionService;
import com.onlinejudge.worker.service.ExecutionBackend;
import com.onlinejudge.worker.service.GcsClient;
import com.onlinejudge.worker.service.IdempotencyService;
import com.onlinejudge.worker.service.TestCaseFetcher;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

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
    /** Workstream H: worker.verdicts.published_total + adjacent metrics. */
    private final WorkerMetrics metrics;
    /** Workstream B: pulls source code from GCS by gs:// URI. Nullable in tests. */
    private final GcsClient gcsClient;
    /** Workstream B: resolves test-case URLs and SHA-256s expected outputs. Nullable in tests. */
    private final TestCaseFetcher testCaseFetcher;

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

            // Idempotency check: ack only terminal completed duplicates. A fresh
            // in-progress duplicate is left unacked so Kafka can redeliver it
            // after the processing lease expires.
            IdempotencyService.ClaimDecision claim =
                    idempotencyService.claimSubmission(submissionId, phase);
            if (claim.status() == IdempotencyService.ClaimStatus.COMPLETED) {
                ack.acknowledge();
                return;
            }
            if (claim.status() == IdempotencyService.ClaimStatus.IN_PROGRESS) {
                ack.nack(Duration.ofSeconds(5));
                return;
            }

            String sourceCode = resolveSourceCode(event.getS3CodeUrl());

            // Workstream B: fetch the real test-case suite from Problem Service +
            // GCS. When testCaseFetcher is null (legacy smoke harness / unit tests
            // without a problem service), fall back to a single empty-stdin spec
            // with an empty expected_hash. The agent will report WA on that case
            // — fine for a smoke test, the run itself completes.
            List<TestCaseSpec> testCases;
            if (testCaseFetcher != null) {
                try {
                    testCases = testCaseFetcher.fetch(event.getProblemId(), phase);
                } catch (Exception ex) {
                    log.error("[worker:{}] test-case fetch failed submission={} problem={}: {}",
                            phase, submissionId, event.getProblemId(), ex.getMessage());
                    ack.nack(Duration.ofSeconds(5));
                    return;
                }
            } else {
                testCases = List.of(new TestCaseSpec(1, "", ""));
            }

            DockerExecutionService.ExecutionResult result =
                    executionService.execute(submissionId, language, sourceCode, testCases);

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
            sendAndWait(evaluatedResultsTopic, userId, verdictEvent.toByteArray());
            // Workstream H: count the verdict once the broker has ack'd.
            // sendAndWait throws on failure, so reaching this line means
            // the send genuinely succeeded.
            metrics.incVerdictsPublished(verdict, language, phase);

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
                sendAndWait(systemTestTopic, userId, system.toByteArray());
                log.info("[worker:pretest] Submission {} accepted; enqueued to {} for Phase 2",
                        submissionId, systemTestTopic);
            }

            if (!idempotencyService.markCompleted(submissionId, phase, claim.leaseStartedAt())) {
                throw new IllegalStateException("Lost idempotency claim before completion");
            }
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[worker:{}] Error processing submission={}: {}",
                    phase, submissionId, ex.getMessage(), ex);
            ack.nack(Duration.ofSeconds(5));
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

    String resolveSourceCode(String s3CodeUrl) {
        if (s3CodeUrl == null || s3CodeUrl.isBlank()) {
            throw new IllegalArgumentException("SubmissionEvent.s3CodeUrl is required");
        }

        if (s3CodeUrl.startsWith("data:")) {
            return decodeDataUrl(s3CodeUrl);
        }

        String lower = s3CodeUrl.toLowerCase(Locale.ROOT);
        if (lower.startsWith("gs://")) {
            // Workstream B: the API gateway (Workstream G) writes contestant
            // source to GCS and stamps a gs://<bucket>/<key> URI on the event.
            if (gcsClient == null) {
                throw new UnsupportedOperationException(
                        "gs:// code URL but GcsClient not wired (running without GcsConfig?)");
            }
            try {
                return new String(gcsClient.fetch(s3CodeUrl), StandardCharsets.UTF_8);
            } catch (java.io.IOException ex) {
                throw new RuntimeException("GCS source fetch failed: " + s3CodeUrl, ex);
            }
        }

        if (lower.startsWith("s3://") || lower.startsWith("r2://") ||
                lower.startsWith("http://") || lower.startsWith("https://")) {
            throw new UnsupportedOperationException(
                    "TODO: object-store code fetch is not wired in execution-worker yet");
        }

        if (lower.startsWith("local://")) {
            throw new UnsupportedOperationException(
                    "local:// code URLs do not embed source; local mode must emit data: URLs");
        }

        throw new UnsupportedOperationException("Unsupported code URL scheme");
    }

    private static String decodeDataUrl(String dataUrl) {
        int comma = dataUrl.indexOf(',');
        if (comma < 0) {
            throw new IllegalArgumentException("Malformed data URL: missing comma separator");
        }

        String metadata = dataUrl.substring(5, comma).toLowerCase(Locale.ROOT);
        String payload = dataUrl.substring(comma + 1);
        if (metadata.contains(";base64")) {
            try {
                return new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Malformed data URL: invalid base64 payload", ex);
            }
        }

        return percentDecode(payload);
    }

    private static String percentDecode(String payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length());
        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            if (c == '%') {
                if (i + 2 >= payload.length()) {
                    throw new IllegalArgumentException("Malformed data URL: incomplete percent escape");
                }
                int hi = Character.digit(payload.charAt(i + 1), 16);
                int lo = Character.digit(payload.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw new IllegalArgumentException("Malformed data URL: invalid percent escape");
                }
                out.write((hi << 4) + lo);
                i += 2;
            } else {
                out.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private void sendAndWait(String topic, String key, byte[] payload) throws Exception {
        kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
    }
}
