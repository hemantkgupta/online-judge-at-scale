package com.onlinejudge.analytics.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.analytics.service.ClickHouseWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumes analytics_events from Kafka and batches writes to ClickHouse.
 *
 * This consumer group is completely independent from the scoring pipeline.
 * The execution worker publishes to analytics_events as fire-and-forget;
 * analytics processing never blocks or delays the verdict path.
 *
 * Production: ClickHouse Kafka Engine + Materialized View consumes directly
 * from Kafka without this consumer process. Local substitute: Spring Kafka
 * consumer + HTTP batch insert.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyticsConsumer {

    private final ClickHouseWriter clickHouseWriter;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "${app.kafka.topic.analytics}",
        groupId = "analytics-pipeline",
        concurrency = "2"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        try {
            JsonNode event = objectMapper.readTree(record.value());

            Map<String, Object> analyticsRecord = new HashMap<>();
            analyticsRecord.put("submissionId",   event.get("submissionId").asText());
            analyticsRecord.put("userId",         event.get("userId").asText());
            analyticsRecord.put("problemId",      event.get("problemId").asText());
            analyticsRecord.put("contestId",      event.has("contestId") && !event.get("contestId").isNull()
                                                  ? event.get("contestId").asText() : "");
            analyticsRecord.put("language",       event.get("language").asText());
            analyticsRecord.put("verdict",        event.get("verdict").asText());
            analyticsRecord.put("executionTimeMs", event.get("executionTimeMs").asInt());
            analyticsRecord.put("memoryUsedMb",   event.get("memoryUsedMb").asInt());
            analyticsRecord.put("eventTsMs",      event.get("eventTsMs").asLong());

            clickHouseWriter.buffer(analyticsRecord);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[analytics] Error processing record: {}", ex.getMessage());
            // Ack anyway - analytics is non-critical; don't block the consumer
            ack.acknowledge();
        }
    }
}
