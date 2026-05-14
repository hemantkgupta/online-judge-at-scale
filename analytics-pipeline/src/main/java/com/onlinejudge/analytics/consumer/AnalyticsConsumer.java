package com.onlinejudge.analytics.consumer;

import com.onlinejudge.analytics.service.ClickHouseWriter;
import com.onlinejudge.common.events.Events.AnalyticsEvent;
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

    @KafkaListener(
        topics = "${app.kafka.topic.analytics}",
        groupId = "analytics-pipeline",
        concurrency = "2"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        try {
            AnalyticsEvent event = AnalyticsEvent.parseFrom(record.value());

            Map<String, Object> analyticsRecord = new HashMap<>();
            analyticsRecord.put("submissionId",   event.getSubmissionId());
            analyticsRecord.put("userId",         event.getUserId());
            analyticsRecord.put("problemId",      event.getProblemId());
            analyticsRecord.put("contestId",      event.getContestId());
            analyticsRecord.put("language",       event.getLanguage());
            analyticsRecord.put("verdict",        event.getVerdict());
            analyticsRecord.put("executionTimeMs", event.getExecutionTimeMs());
            analyticsRecord.put("memoryUsedMb",   event.getMemoryUsedMb());
            analyticsRecord.put("eventTsMs",      event.getEventTsMs());

            clickHouseWriter.buffer(analyticsRecord);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[analytics] Error processing record: {}", ex.getMessage());
            // Ack anyway - analytics is non-critical; don't block the consumer
            ack.acknowledge();
        }
    }
}
