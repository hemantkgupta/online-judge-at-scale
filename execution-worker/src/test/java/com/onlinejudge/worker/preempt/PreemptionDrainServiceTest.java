package com.onlinejudge.worker.preempt;

import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.worker.service.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tech-spec §11.2 — verify {@link PreemptionDrainService} satisfies the
 * four guarantees the GCP SPOT preempt window demands:
 *
 * <ol>
 *   <li>Stops every running Kafka listener container (no new submissions
 *       claimed).</li>
 *   <li>Publishes one {@code WORKER_PREEMPTED} verdict per in-flight row,
 *       carrying the original submission's userId / problemId / phase /
 *       region.</li>
 *   <li>Marks the idempotency row poisoned so the next worker start
 *       doesn't re-execute and double-verdict.</li>
 *   <li>Is idempotent — a second {@code drain()} short-circuits.</li>
 * </ol>
 */
class PreemptionDrainServiceTest {

    private InFlightTracker tracker;
    private IdempotencyService idempotency;
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, byte[]> kafkaTemplate = mock(KafkaTemplate.class);
    private KafkaListenerEndpointRegistry listenerRegistry;
    private PreemptionDrainService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        tracker = new InFlightTracker();
        idempotency = mock(IdempotencyService.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        listenerRegistry = mock(KafkaListenerEndpointRegistry.class);

        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));

        service = new PreemptionDrainService(tracker, idempotency, kafkaTemplate, listenerRegistry);
        ReflectionTestUtils.setField(service, "evaluatedResultsTopic", "evaluated_results.us-central1");
        ReflectionTestUtils.setField(service, "drainTimeoutMs", 25_000L);
        ReflectionTestUtils.setField(service, "listenerStopMs", 1_000L);
    }

    @Test
    void drainStopsListenersAndVerdictsEveryInFlightRow() throws Exception {
        MessageListenerContainer pretest = mock(MessageListenerContainer.class);
        MessageListenerContainer system  = mock(MessageListenerContainer.class);
        when(pretest.isRunning()).thenReturn(true);
        when(system.isRunning()).thenReturn(true);
        // stop(callback) — invoke the latch immediately so the drain's
        // await() doesn't time out.
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(pretest).stop(any(Runnable.class));
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(system).stop(any(Runnable.class));
        when(listenerRegistry.getListenerContainers())
                .thenReturn(List.of(pretest, system));

        Instant t0 = Instant.parse("2026-05-18T12:00:00Z");
        tracker.register(new InFlightTracker.InFlightSubmission(
                "sub-A", "user-A", "prob-A", "contest-1", "us-central1", "pretest", 1_000L, t0));
        tracker.register(new InFlightTracker.InFlightSubmission(
                "sub-B", "user-B", "prob-B", "",          "us-central1", "system",  2_000L, t0));

        PreemptionDrainService.DrainResult result = service.drain();

        // 1) Both listeners paused + stopped.
        verify(pretest).pause();
        verify(system).pause();
        verify(pretest).stop(any(Runnable.class));
        verify(system).stop(any(Runnable.class));
        assertThat(result.listenersStopped()).isEqualTo(2);

        // 2) Two WORKER_PREEMPTED verdicts published to the evaluated_results
        //    topic (keyed by userId so leaderboard sees the right partition).
        ArgumentCaptor<String> keyCaptor   = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> bodyCaptor  = ArgumentCaptor.forClass(byte[].class);
        verify(kafkaTemplate, times(2)).send(
                eq("evaluated_results.us-central1"),
                keyCaptor.capture(),
                bodyCaptor.capture());
        assertThat(keyCaptor.getAllValues()).containsExactlyInAnyOrder("user-A", "user-B");

        List<VerdictEvent> verdicts = bodyCaptor.getAllValues().stream()
                .map(b -> {
                    try { return VerdictEvent.parseFrom(b); }
                    catch (Exception ex) { throw new AssertionError(ex); }
                })
                .toList();
        assertThat(verdicts).allMatch(v -> v.getResult().equals("WORKER_PREEMPTED"));
        assertThat(verdicts).allMatch(v -> v.getRegion().equals("us-central1"));
        assertThat(verdicts).extracting(VerdictEvent::getSubmissionId)
                .containsExactlyInAnyOrder("sub-A", "sub-B");
        assertThat(verdicts).extracting(VerdictEvent::getPhase)
                .containsExactlyInAnyOrder("pretest", "system");

        // 3) Both idempotency rows marked poisoned.
        verify(idempotency).markPoison("sub-A", "pretest");
        verify(idempotency).markPoison("sub-B", "system");

        // 4) Producer flushed so verdicts are durable before SIGTERM.
        verify(kafkaTemplate).flush();

        // Result envelope reflects the count.
        assertThat(result.alreadyDrained()).isFalse();
        assertThat(result.inFlightAtStart()).isEqualTo(2);
        assertThat(result.verdictsPublished()).isEqualTo(2);
        assertThat(result.verdictPublishFailures()).isEqualTo(0);

        // Tracker is empty post-drain so a re-trigger is a no-op.
        assertThat(tracker.size()).isEqualTo(0);
    }

    @Test
    void drainIsIdempotent_secondCallShortCircuits() {
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of());

        PreemptionDrainService.DrainResult first = service.drain();
        PreemptionDrainService.DrainResult second = service.drain();

        assertThat(first.alreadyDrained()).isFalse();
        assertThat(second.alreadyDrained()).isTrue();
        // No additional Kafka traffic on the second call.
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void emptyTracker_drainIsCleanNoop() {
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of());

        PreemptionDrainService.DrainResult result = service.drain();

        assertThat(result.inFlightAtStart()).isEqualTo(0);
        assertThat(result.verdictsPublished()).isEqualTo(0);
        assertThat(result.verdictPublishFailures()).isEqualTo(0);
        verifyNoInteractions(idempotency);
        // flush() still runs — it's a cheap producer-side belt-and-braces.
        verify(kafkaTemplate).flush();
    }

    @Test
    void publishFailureCountedSeparately() throws Exception {
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of());

        // First send succeeds, second send throws (broker down).
        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<Object> ok = CompletableFuture.completedFuture(null);
        java.util.concurrent.CompletableFuture<Object> bad = new CompletableFuture<>();
        bad.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class)))
                .thenReturn((CompletableFuture) ok)
                .thenReturn((CompletableFuture) bad);

        Instant t0 = Instant.now();
        tracker.register(new InFlightTracker.InFlightSubmission(
                "sub-A", "user-A", "prob-A", "", "us-central1", "pretest", 1L, t0));
        tracker.register(new InFlightTracker.InFlightSubmission(
                "sub-B", "user-B", "prob-B", "", "us-central1", "pretest", 2L, t0));

        PreemptionDrainService.DrainResult result = service.drain();

        // We get exactly one success and one failure — order is iteration-dependent
        // but the count tally is deterministic.
        assertThat(result.verdictsPublished() + result.verdictPublishFailures()).isEqualTo(2);
        assertThat(result.verdictPublishFailures()).isEqualTo(1);
        // The failing row stays in the tracker so the next drain (or
        // operator follow-up) can see it; the succeeding one is gone.
        assertThat(tracker.size()).isEqualTo(1);
    }
}
