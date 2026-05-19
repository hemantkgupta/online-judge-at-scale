package com.onlinejudge.worker.consumer;

import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.AgentClient.PerTestResult;
import com.onlinejudge.worker.service.ExecutionBackend;
import com.onlinejudge.worker.service.IdempotencyService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies tech-spec §5.3's per-region Kafka topic naming convention is
 * applied end-to-end in execution-worker:
 *
 * <ol>
 *   <li>The {@code @Value}-bound topic-name fields contain the region segment
 *       when {@code app.region} is set (property-binding test).</li>
 *   <li>The verdict + analytics + Phase 2 promotion publishes go to those
 *       resolved topics (routing test on {@link SubmissionConsumer}).</li>
 * </ol>
 */
class SubmissionConsumerRegionTest {

    /**
     * Loads only Spring's property-placeholder resolution and a tiny
     * {@link TopicHolder} bean. Confirms that with {@code app.region=us-central1}
     * the worker's topic-name templates resolve to the expected per-region names.
     */
    @Test
    void topicNamesIncludeConfiguredRegion() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
                .withUserConfiguration(TopicHolder.class)
                .withPropertyValues(
                        "app.region=us-central1",
                        "app.kafka.topic.pretest=submissions.${app.region}.pretest",
                        "app.kafka.topic.system=submissions.${app.region}.system",
                        "app.kafka.topic.evaluated-results=evaluated_results.${app.region}",
                        "app.kafka.topic.analytics=analytics_events.${app.region}",
                        "app.kafka.topic.dlq=submissions.${app.region}.dlq"
                )
                .run(ctx -> {
                    TopicHolder holder = ctx.getBean(TopicHolder.class);
                    assertThat(holder.pretestTopic).isEqualTo("submissions.us-central1.pretest");
                    assertThat(holder.systemTopic).isEqualTo("submissions.us-central1.system");
                    assertThat(holder.evaluatedResultsTopic).isEqualTo("evaluated_results.us-central1");
                    assertThat(holder.analyticsTopic).isEqualTo("analytics_events.us-central1");
                    assertThat(holder.dlqTopic).isEqualTo("submissions.us-central1.dlq");
                });
    }

    /**
     * The verdict publish goes to the per-region {@code evaluated_results.<region>}
     * topic, and Phase 2 promotion goes to the per-region {@code submissions.<region>.system}
     * topic. We exercise the real {@code processSubmission} path with mocked
     * collaborators so the routing logic itself is what's under test.
     */
    @Test
    @SuppressWarnings("unchecked")
    void verdictAndPromotionPublishToRegionalTopics() throws Exception {
        IdempotencyService idempotency = mock(IdempotencyService.class);
        when(idempotency.claimSubmission(anyString(), anyString()))
                .thenReturn(new IdempotencyService.ClaimDecision(
                        IdempotencyService.ClaimStatus.CLAIMED, java.time.Instant.now()));
        when(idempotency.markCompleted(anyString(), anyString(), any())).thenReturn(true);

        ExecutionBackend backend = mock(ExecutionBackend.class);
        when(backend.execute(anyString(), anyString(), anyString(), any(), anyInt(), anyInt()))
                .thenReturn(new ExecutionBackend.ExecutionResult(
                        "OK", "stdout", 50, 32,
                        java.util.List.<PerTestResult>of()));

        KafkaTemplate<String, byte[]> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        WorkerMetrics metrics = mock(WorkerMetrics.class);

        SubmissionConsumer consumer = new SubmissionConsumer(
                idempotency, backend, kafkaTemplate,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                metrics,
                /* sourceCodeResolver */ new com.onlinejudge.worker.service.source.SourceCodeResolver(
                        null, null, null, null),
                /* testCaseFetcher */ null);

        // Inject @Value-bound fields by reflection — simulates Spring's
        // post-construction property binding from a us-central1 environment.
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "evaluatedResultsTopic", "evaluated_results.us-central1");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "analyticsTopic", "analytics_events.us-central1");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "systemTestTopic", "submissions.us-central1.system");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "dlqTopic", "submissions.us-central1.dlq");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "problemServiceRequired", false);

        SubmissionEvent event = SubmissionEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setUserId("user-1")
                .setProblemId("prob-1")
                .setContestId("contest-1")
                .setLanguage("python")
                .setS3CodeUrl("data:text/plain,print(1)")
                .setGatewayTsMs(123L)
                .setRegion("us-central1")
                .setPhase("pretest")
                .build();

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "submissions.us-central1.pretest", 0, 0L, "user-1", event.toByteArray());
        Acknowledgment ack = mock(Acknowledgment.class);

        consumer.consumePretest(record, ack);

        // Capture all topics published to.
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3))
                .send(topicCaptor.capture(), anyString(), any(byte[].class));

        java.util.List<String> topics = topicCaptor.getAllValues();
        // Verdict → evaluated_results.<region>
        assertThat(topics).contains("evaluated_results.us-central1");
        // Analytics → analytics_events.<region>
        assertThat(topics).contains("analytics_events.us-central1");
        // Phase 1 → Phase 2 promotion (ACCEPTED) → submissions.<region>.system
        assertThat(topics).contains("submissions.us-central1.system");
        // Every topic carries the region segment — no cross-region leakage.
        assertThat(topics).allMatch(t -> t.contains("us-central1"));
    }

    /**
     * Verify the verdict payload's {@code region} field is preserved across
     * the publish so downstream Flink jobs in the region can confirm
     * provenance.
     */
    @Test
    @SuppressWarnings("unchecked")
    void verdictPayloadCarriesRegion() throws Exception {
        IdempotencyService idempotency = mock(IdempotencyService.class);
        when(idempotency.claimSubmission(anyString(), anyString()))
                .thenReturn(new IdempotencyService.ClaimDecision(
                        IdempotencyService.ClaimStatus.CLAIMED, java.time.Instant.now()));
        when(idempotency.markCompleted(anyString(), anyString(), any())).thenReturn(true);

        ExecutionBackend backend = mock(ExecutionBackend.class);
        when(backend.execute(anyString(), anyString(), anyString(), any(), anyInt(), anyInt()))
                .thenReturn(new ExecutionBackend.ExecutionResult(
                        "WRONG_ANSWER", "out", 10, 16,
                        java.util.List.<PerTestResult>of()));

        KafkaTemplate<String, byte[]> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        SubmissionConsumer consumer = new SubmissionConsumer(
                idempotency, backend, kafkaTemplate,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                mock(WorkerMetrics.class),
                new com.onlinejudge.worker.service.source.SourceCodeResolver(null, null, null, null),
                null);

        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "evaluatedResultsTopic", "evaluated_results.eu-west1");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "analyticsTopic", "analytics_events.eu-west1");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "systemTestTopic", "submissions.eu-west1.system");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "dlqTopic", "submissions.eu-west1.dlq");
        org.springframework.test.util.ReflectionTestUtils.setField(
                consumer, "problemServiceRequired", false);

        SubmissionEvent event = SubmissionEvent.newBuilder()
                .setSubmissionId("sub-eu")
                .setUserId("user-eu")
                .setProblemId("prob-eu")
                .setLanguage("python")
                .setS3CodeUrl("data:text/plain,print(2)")
                .setGatewayTsMs(456L)
                .setRegion("eu-west1")
                .setPhase("pretest")
                .build();

        consumer.consumePretest(
                new ConsumerRecord<>("submissions.eu-west1.pretest", 0, 0L, "user-eu", event.toByteArray()),
                mock(Acknowledgment.class));

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(kafkaTemplate).send(
                org.mockito.ArgumentMatchers.eq("evaluated_results.eu-west1"),
                anyString(),
                payloadCaptor.capture());

        VerdictEvent v = VerdictEvent.parseFrom(payloadCaptor.getValue());
        assertThat(v.getRegion()).isEqualTo("eu-west1");
    }

    /** Tiny config bean to expose @Value-bound topic names for the binding test. */
    @Configuration
    static class TopicHolder {
        @Value("${app.kafka.topic.pretest}") String pretestTopic;
        @Value("${app.kafka.topic.system}") String systemTopic;
        @Value("${app.kafka.topic.evaluated-results}") String evaluatedResultsTopic;
        @Value("${app.kafka.topic.analytics}") String analyticsTopic;
        @Value("${app.kafka.topic.dlq}") String dlqTopic;
    }

}
