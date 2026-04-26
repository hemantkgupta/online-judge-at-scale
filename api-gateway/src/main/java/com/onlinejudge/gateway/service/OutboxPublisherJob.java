package com.onlinejudge.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.gateway.model.OutboxEvent;
import com.onlinejudge.gateway.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Poll-based Transactional Outbox publisher.
 *
 * Every poll interval, reads a batch of unpublished outbox events,
 * publishes them to Kafka, and marks them as published.
 *
 * Production alternative: Debezium CDC reads the DB WAL and publishes
 * events to Kafka with near-zero latency and no polling overhead.
 * The poll approach adds ~1s of latency but requires no additional
 * infrastructure (no Kafka Connect cluster).
 *
 * Partitioning: we key by userId so all submissions from the same user
 * land on the same Kafka partition and are consumed in order.
 */
@Slf4j
@Component
@RequiredArgsConstructor
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

                // Key by userId for partition ordering guarantee
                kafkaTemplate.send(pretestTopic, userId, event.getPayload().getBytes());

                event.setPublished(true);
                outboxEventRepository.save(event);

                log.debug("[outbox] Published submission={} to topic={}",
                        event.getSubmissionId(), pretestTopic);

            } catch (Exception ex) {
                log.error("[outbox] Failed to publish event={}: {}", event.getId(), ex.getMessage());
                // Leave published=false; will retry on next poll
            }
        }
    }
}
