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

    private final SubmissionRepository submissionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Accepts a submission and writes it + an outbox event in a single ACID transaction.
     *
     * The outbox publisher job (OutboxPublisherJob) will pick up the event
     * and publish it to Kafka asynchronously.
     *
     * This eliminates the dual-write problem: either both the submission and
     * the outbox event are durable, or neither is.
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

        log.info("[gateway] Accepted submission={} user={} lang={} region={} ts={}",
                submission.getId(), userId, request.getLanguage(), region, gatewayTsMs);

        return new SubmissionResponse(
                submission.getId().toString(),
                "PENDING",
                gatewayTsMs,
                "Submission accepted. Verdict will be delivered via WebSocket."
        );
    }

    /**
     * In production: upload code to Cloudflare R2 / S3 and return the URL.
     * Locally: store code inline as a data-URI so we don't need an object store.
     */
    private String storeCode(UUID submissionId, String code) {
        byte[] bytes = (code == null ? "" : code).getBytes(StandardCharsets.UTF_8);
        return "data:text/plain;charset=utf-8;base64," + Base64.getEncoder().encodeToString(bytes);
        // Production: s3Client.putObject(bucket, key, code); return "s3://bucket/key";
    }
}
