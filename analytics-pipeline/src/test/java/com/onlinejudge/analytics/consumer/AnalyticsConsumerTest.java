package com.onlinejudge.analytics.consumer;

import com.onlinejudge.analytics.service.ClickHouseWriter;
import com.onlinejudge.common.events.Events.AnalyticsEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link AnalyticsConsumer}.
 *
 * <p>Wire format is Protobuf {@link AnalyticsEvent} (Part 2). The consumer is
 * intentionally non-blocking: analytics is fire-and-forget, so even malformed
 * events must still ack the Kafka offset rather than block the consumer group
 * on a bad message. These tests verify both the happy extraction path and the
 * graceful-on-error contract.
 */
@ExtendWith(MockitoExtension.class)
class AnalyticsConsumerTest {

    @Mock private ClickHouseWriter clickHouseWriter;
    @Mock private Acknowledgment ack;

    private AnalyticsConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AnalyticsConsumer(clickHouseWriter);
    }

    private static byte[] proto(AnalyticsEvent.Builder b) {
        return b.build().toByteArray();
    }

    @Test
    void consume_validEvent_buffersAndAcks() {
        byte[] payload = proto(AnalyticsEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setUserId("user-abc")
                .setProblemId("p-1")
                .setContestId("c-1")
                .setLanguage("python")
                .setVerdict("ACCEPTED")
                .setExecutionTimeMs(42)
                .setMemoryUsedMb(12)
                .setEventTsMs(1719849599000L)
                .setRegion("us-east-1")
                .setPhase("pretest"));

        ConsumerRecord<String, byte[]> record =
                new ConsumerRecord<>("analytics_events", 0, 0L, "sub-1", payload);

        consumer.consume(record, ack);

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(clickHouseWriter).buffer(captor.capture());
        verify(ack).acknowledge();

        Map<String, Object> buffered = captor.getValue();
        assertThat(buffered).containsEntry("submissionId", "sub-1");
        assertThat(buffered).containsEntry("userId", "user-abc");
        assertThat(buffered).containsEntry("problemId", "p-1");
        assertThat(buffered).containsEntry("contestId", "c-1");
        assertThat(buffered).containsEntry("language", "python");
        assertThat(buffered).containsEntry("verdict", "ACCEPTED");
        assertThat(buffered).containsEntry("executionTimeMs", 42);
        assertThat(buffered).containsEntry("memoryUsedMb", 12);
        assertThat(buffered).containsEntry("eventTsMs", 1719849599000L);
    }

    @Test
    void consume_missingContestId_substitutesEmptyString() {
        // Proto string fields default to "" when unset, so "missing contestId"
        // surfaces as the empty string on the downstream record.
        byte[] payload = proto(AnalyticsEvent.newBuilder()
                .setSubmissionId("sub-2")
                .setUserId("u")
                .setProblemId("p")
                .setLanguage("cpp")
                .setVerdict("WRONG_ANSWER")
                .setExecutionTimeMs(10)
                .setMemoryUsedMb(5)
                .setEventTsMs(1));

        ConsumerRecord<String, byte[]> record =
                new ConsumerRecord<>("analytics_events", 0, 0L, "sub-2", payload);

        consumer.consume(record, ack);

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(clickHouseWriter).buffer(captor.capture());
        assertThat(captor.getValue()).containsEntry("contestId", "");
        verify(ack).acknowledge();
    }

    @Test
    void consume_malformedBytes_acksWithoutBuffering() {
        // A truncated length-delimited field — tag byte 0x0A (field 1,
        // wire-type LENGTH_DELIMITED) followed by an unterminated varint
        // length. Proto parsing throws InvalidProtocolBufferException;
        // the consumer must ack and keep moving (analytics is non-critical).
        byte[] payload = new byte[] { 0x0A, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        ConsumerRecord<String, byte[]> record =
                new ConsumerRecord<>("analytics_events", 0, 0L, "x", payload);

        consumer.consume(record, ack);

        verify(clickHouseWriter, never()).buffer(any());
        verify(ack).acknowledge();
    }

    private static Map<String, Object> any() {
        return org.mockito.ArgumentMatchers.any();
    }
}
