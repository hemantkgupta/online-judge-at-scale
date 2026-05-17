package com.onlinejudge.contest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Kafka producer configuration for contest-service.
 *
 * The producer itself ({@code KafkaTemplate<String, byte[]>}) is auto-wired
 * by Spring Boot from {@code spring.kafka.*} properties in
 * {@code application.yml} — bootstrap servers, the {@code byte[]} value
 * serializer, and {@code acks=all} for durable system-test replay events
 * (Roadmap §4.23). We don't override the template here; this class only
 * exposes the topic names so callers depend on a typed bean rather than
 * sprinkling string literals through the codebase.
 *
 * The two relevant topics for contest-service:
 *   * {@code contest_events} — state-transition fanout (existing).
 *   * {@code submissions.system} — Phase 2 system-test promotion target,
 *     shared with execution-worker's promotion path. Must match the topic
 *     name configured in {@code execution-worker/application.yml} under
 *     {@code app.kafka.topic.system}.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTopics kafkaTopics(
            @Value("${app.kafka.topic.system:submissions.system}") String systemTopic,
            @Value("${app.kafka.topic.contest-events:contest_events}") String contestEventsTopic) {
        return new KafkaTopics(systemTopic, contestEventsTopic);
    }

    /**
     * Bundle of topic names resolved from configuration. Inject this rather
     * than calling {@code @Value} at every use site so the producer code stays
     * unit-testable without a Spring context.
     */
    public record KafkaTopics(String systemTopic, String contestEventsTopic) {}
}
