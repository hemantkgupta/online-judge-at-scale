package com.onlinejudge.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.gateway.model.OutboxEvent;
import com.onlinejudge.gateway.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the poll-based Transactional Outbox publisher.
 *
 * Verifies: picks up unpublished rows, publishes to Kafka keyed by userId,
 * marks rows as published, handles empty batches and failures gracefully.
 */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherJobTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private OutboxPublisherJob outboxPublisherJob;

    @Captor
    private ArgumentCaptor<OutboxEvent> savedEventCaptor;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(outboxPublisherJob, "pretestTopic", "submissions.pretest");
        ReflectionTestUtils.setField(outboxPublisherJob, "batchSize", 50);
    }

    private OutboxEvent createTestEvent(String userId) {
        OutboxEvent event = new OutboxEvent();
        event.setId(UUID.randomUUID());
        event.setSubmissionId(UUID.randomUUID());
        event.setEventType("SUBMISSION_RECEIVED");
        event.setPayload("{\"userId\":\"" + userId + "\",\"submissionId\":\"" + event.getSubmissionId() + "\"}");
        event.setPublished(false);
        event.setCreatedAt(Instant.now());
        return event;
    }

    @Test
    void publishPendingEvents_picksUpUnpublishedAndMarksPublished() {
        OutboxEvent event = createTestEvent("user-1");
        when(outboxEventRepository.findUnpublished(50)).thenReturn(List.of(event));

        outboxPublisherJob.publishPendingEvents();

        // Verify published to Kafka with userId as key
        verify(kafkaTemplate).send(eq("submissions.pretest"), eq("user-1"), any(byte[].class));

        // Verify marked as published
        verify(outboxEventRepository).save(savedEventCaptor.capture());
        assertThat(savedEventCaptor.getValue().isPublished()).isTrue();
        assertThat(savedEventCaptor.getValue().getId()).isEqualTo(event.getId());
    }

    @Test
    void publishPendingEvents_publishesMultipleEventsInOrder() {
        OutboxEvent event1 = createTestEvent("user-a");
        OutboxEvent event2 = createTestEvent("user-b");
        OutboxEvent event3 = createTestEvent("user-a");

        when(outboxEventRepository.findUnpublished(50)).thenReturn(List.of(event1, event2, event3));

        outboxPublisherJob.publishPendingEvents();

        // All three published
        verify(kafkaTemplate, times(3)).send(eq("submissions.pretest"), anyString(), any(byte[].class));

        // All three marked as published
        verify(outboxEventRepository, times(3)).save(savedEventCaptor.capture());
        assertThat(savedEventCaptor.getAllValues()).allMatch(OutboxEvent::isPublished);
    }

    @Test
    void publishPendingEvents_doesNothingWhenNoPendingEvents() {
        when(outboxEventRepository.findUnpublished(50)).thenReturn(Collections.emptyList());

        outboxPublisherJob.publishPendingEvents();

        // No Kafka publish
        verifyNoInteractions(kafkaTemplate);
        // No save (no events to mark)
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void publishPendingEvents_keysMessageByUserId() {
        OutboxEvent event = createTestEvent("user-xyz-123");
        when(outboxEventRepository.findUnpublished(50)).thenReturn(List.of(event));

        outboxPublisherJob.publishPendingEvents();

        // The Kafka partition key must be userId for per-user ordering
        verify(kafkaTemplate).send(eq("submissions.pretest"), eq("user-xyz-123"), any(byte[].class));
    }

    @Test
    void publishPendingEvents_continuesOnKafkaFailure() {
        OutboxEvent event1 = createTestEvent("user-1");
        OutboxEvent event2 = createTestEvent("user-2");

        when(outboxEventRepository.findUnpublished(50)).thenReturn(List.of(event1, event2));
        // First event fails to publish
        when(kafkaTemplate.send(eq("submissions.pretest"), eq("user-1"), any(byte[].class)))
                .thenThrow(new RuntimeException("Kafka unavailable"));

        outboxPublisherJob.publishPendingEvents();

        // Second event should still be attempted
        verify(kafkaTemplate).send(eq("submissions.pretest"), eq("user-2"), any(byte[].class));

        // Only event2 should be marked as published (event1 failed)
        verify(outboxEventRepository, times(1)).save(savedEventCaptor.capture());
        assertThat(savedEventCaptor.getValue().getSubmissionId()).isEqualTo(event2.getSubmissionId());
    }
}
