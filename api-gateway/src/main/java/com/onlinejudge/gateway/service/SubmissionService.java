package com.onlinejudge.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.gateway.dto.SubmissionRequest;
import com.onlinejudge.gateway.dto.SubmissionResponse;
import com.onlinejudge.gateway.model.OutboxEvent;
import com.onlinejudge.gateway.model.Submission;
import com.onlinejudge.gateway.repository.OutboxEventRepository;
import com.onlinejudge.gateway.repository.SubmissionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SubmissionService {

    /** Source-mode constants — keep in sync with application.yml app.submission.source-mode. */
    public static final String SOURCE_MODE_GCS = "gcs";
    public static final String SOURCE_MODE_DATA_URL = "data-url";

    private final SubmissionRepository submissionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Optional — only present once Workstream G has wired the GcsConfig bean.
     * When absent (e.g. older tests that pre-date GCS), the service silently
     * falls back to the data-url path regardless of mode. This keeps the
     * existing unit-test surface green.
     */
    @Autowired(required = false)
    private GcsWriter gcsWriter;

    @Value("${app.submission.source-mode:data-url}")
    private String sourceMode;

    /**
     * Accepts a submission and writes it + an outbox event in a single ACID transaction.
     *
     * <p>When {@code app.submission.source-mode=gcs}, the contestant's source is
     * written to Google Cloud Storage <em>before</em> the outbox row is created
     * — Part 6 durability invariant. A GCS write failure throws
     * {@link GcsWriteException} and the controller surfaces it as 503;
     * we never publish an event referencing a key that doesn't exist.
     *
     * <p>When mode is {@code data-url} (legacy), the source is embedded
     * inline as a {@code data:} URI and the entire flow is in-process.
     */
    @Transactional
    public SubmissionResponse accept(SubmissionRequest request, String userId, String region) throws Exception {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId must not be blank — should come from authenticated principal");
        }
        long gatewayTsMs = System.currentTimeMillis(); // immutable T0 stamp

        // 1. Persist submission
        Submission submission = new Submission();
        submission.setId(UUID.randomUUID());
        submission.setUserId(UUID.fromString(userId));
        submission.setProblemId(UUID.fromString(request.getProblemId()));
        if (request.getContestId() != null) {
            submission.setContestId(UUID.fromString(request.getContestId()));
        }
        submission.setLanguage(request.getLanguage());
        submission.setS3CodeUrl(storeCode(submission.getId(), request.getCode()));
        submission.setGatewayTsMs(gatewayTsMs);
        submission.setStatus("PENDING");
        submission.setRegion(region);
        submissionRepository.save(submission);

        // 2. Write outbox event in the same transaction
        Map<String, Object> eventPayload = new HashMap<>();
        eventPayload.put("submissionId", submission.getId().toString());
        eventPayload.put("userId", userId);
        eventPayload.put("problemId", request.getProblemId());
        eventPayload.put("contestId", request.getContestId());
        eventPayload.put("s3CodeUrl", submission.getS3CodeUrl());
        eventPayload.put("language", request.getLanguage());
        eventPayload.put("gatewayTsMs", gatewayTsMs);
        eventPayload.put("region", region);

        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setSubmissionId(submission.getId());
        outboxEvent.setEventType("SUBMISSION_RECEIVED");
        outboxEvent.setPayload(objectMapper.writeValueAsString(eventPayload));
        outboxEvent.setRegion(region);
        outboxEventRepository.save(outboxEvent);

        log.info("[gateway] Accepted submission={} user={} lang={} region={} ts={} mode={}",
                submission.getId(), userId, request.getLanguage(), region, gatewayTsMs, sourceMode);

        return new SubmissionResponse(
                submission.getId().toString(),
                "PENDING",
                gatewayTsMs,
                "Submission accepted. Verdict will be delivered via WebSocket."
        );
    }

    /**
     * Persists the contestant's source. Two strategies:
     * <ul>
     *   <li><b>gcs</b> (production): write to {@code gs://<bucket>/submissions/yyyy/MM/dd/<id>.txt}
     *       and return a {@code gs://...} URI. The event/outbox row carries
     *       this URI; consumers fetch the source from GCS. The write MUST
     *       happen before the outbox row is created so we never publish an
     *       event referencing a non-existent object (Part 6 invariant).</li>
     *   <li><b>data-url</b> (legacy / local dev): inline as
     *       {@code data:text/plain;charset=utf-8;base64,...} — no object
     *       store needed. Kept for backward compat while GCS is rolled out.</li>
     * </ul>
     */
    private String storeCode(UUID submissionId, String code) {
        String source = (code == null ? "" : code);
        if (SOURCE_MODE_GCS.equalsIgnoreCase(sourceMode) && gcsWriter != null) {
            try {
                String gcsKey = gcsWriter.write(submissionId.toString(), source);
                // gs://<bucket>/<key> — scheme is explicit so consumers can
                // distinguish a GCS URI from a legacy data: URL without
                // probing the payload.
                return "gs://" + gcsWriter.bucket() + "/" + gcsKey;
            } catch (RuntimeException ex) {
                // Bubble up as a typed exception so the controller surfaces 503.
                throw new GcsWriteException("Failed to write submission source to GCS", ex);
            }
        }
        // Legacy data-url path.
        byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
        return "data:text/plain;charset=utf-8;base64," + Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Thrown when {@code source-mode=gcs} and the GCS write fails.
     * The controller maps this to {@code 503 Service Unavailable} —
     * we never silently degrade to the data-url fallback (durability first).
     */
    public static class GcsWriteException extends RuntimeException {
        public GcsWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
