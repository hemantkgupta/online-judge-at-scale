package com.onlinejudge.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.gateway.model.OutboxEvent;
import com.onlinejudge.gateway.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Poll-based Transactional Outbox publisher (development substitute for CDC).
 *
 * <p>Every poll interval, reads a batch of unpublished outbox events, publishes
 * them to Kafka, and marks them as published.
 *
 * <p>Production alternative: a CockroachDB native changefeed (see
 * {@code database/changefeed-setup.sql}) tails the Raft log and emits the same
 * outbox rows to Kafka directly — no application code in the hot path.
 * To switch this local stack to changefeed mode, set
 * {@code app.outbox.publisher.enabled=false} and run the SQL setup script;
 * the execution-worker's {@code SubmissionConsumer} auto-detects the
 * changefeed envelope.
 *
 * <p>Partitioning: we key by {@code userId} so all submissions from the same
 * user land on the same Kafka partition and are consumed in order. The
 * changefeed configuration uses the same partition key for the same reason.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisherJob {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.pretest}")
    private String pretestTopic;

    @Value("${app.outbox.batch-size:50}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findUnpublished(batchSize);
        if (pending.isEmpty()) return;

        log.debug("[outbox] Publishing {} events", pending.size());

        for (OutboxEvent event : pending) {
            try {
                JsonNode payload = objectMapper.readTree(event.getPayload());
                String userId = payload.get("userId").asText();

                // Transcode the JSON-shaped outbox payload to a Protobuf
                // SubmissionEvent so the Kafka wire format is proto (Part 2 of
                // the blog). The outbox table itself stays JSON so the opt-in
                // CockroachDB changefeed path (database/changefeed-setup.sql)
                // also keeps working — changefeed mode bypasses this job and
                // emits CRDB envelopes (JSON), which SubmissionConsumer's
                // unwrapEnvelope() still handles.
                byte[] protoBytes = SubmissionEvent.newBuilder()
                        .setSubmissionId(payload.get("submissionId").asText(""))
                        .setUserId(userId)
                        .setProblemId(payload.path("problemId").asText(""))
                        .setContestId(payload.path("contestId").asText(""))
                        .setS3CodeUrl(payload.path("s3CodeUrl").asText(""))
                        .setLanguage(payload.path("language").asText(""))
                        .setGatewayTsMs(payload.path("gatewayTsMs").asLong(0))
                        .setRegion(payload.path("region").asText(""))
                        .setPhase("pretest")
                        .build()
                        .toByteArray();

                // Key by userId for partition ordering guarantee. Wait for the
                // broker ack before marking the outbox row as published.
                kafkaTemplate.send(pretestTopic, userId, protoBytes)
                        .get(10, TimeUnit.SECONDS);

                event.setPublished(true);
                outboxEventRepository.save(event);

                log.debug("[outbox] Published submission={} to topic={} ({} bytes proto)",
                        event.getSubmissionId(), pretestTopic, protoBytes.length);

            } catch (Exception ex) {
                log.error("[outbox] Failed to publish event={}: {}", event.getId(), ex.getMessage());
                // Leave published=false; will retry on next poll
            }
        }
    }
}
