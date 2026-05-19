package com.onlinejudge.worker.preempt;

import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.worker.service.IdempotencyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tech-spec §11.2 / roadmap §3.8 — SPOT preemption shutdown drain for
 * {@code oj-compute}.
 *
 * <p>GCP gives a SPOT VM 30 s notice before reclaim. Without a drain, every
 * submission the worker is currently executing dies mid-flight, the verdict
 * is never published, the idempotency row stale-reclaims, and after the
 * attempts cap it ends up in the DLQ — pure operator pain for a foreseeable
 * preemption.
 *
 * <p>This service does the right thing in that 30 s window:
 * <ol>
 *   <li><b>Stop pulling.</b> Pause every Kafka listener container so no new
 *       submissions are claimed.</li>
 *   <li><b>Verdict the in-flight.</b> For each entry in {@link InFlightTracker},
 *       publish a {@code VerdictEvent(result=WORKER_PREEMPTED)} keyed by
 *       userId — same partition as the original submission so leaderboard
 *       sees both events in order.</li>
 *   <li><b>Mark the idempotency row poisoned.</b> So when the worker comes
 *       back the row is not re-claimed and the contestant won't get a stale
 *       second verdict. Operators get a clean grep target.</li>
 *   <li><b>Flush + return.</b> {@code KafkaTemplate.flush()} blocks until
 *       the broker has ack'd the verdicts. Total wall-time is bounded by
 *       {@link #drainTimeoutMs} (default 25 s, well under the 30 s preempt
 *       budget).</li>
 * </ol>
 *
 * <p>{@link #drain()} is idempotent — a second call after a successful drain
 * is a no-op. The endpoint that triggers this is exposed by
 * {@link PreemptDrainEndpoint}.
 */
@Slf4j
@Service
public class PreemptionDrainService {

    /** Distinct from ACCEPTED / WRONG_ANSWER / etc. — operators filter on this
     *  to count preemption losses and replay if needed. {@code VerdictEvent.result}
     *  is a free-form string (see {@code common/src/main/proto/events.proto}). */
    public static final String VERDICT_WORKER_PREEMPTED = "WORKER_PREEMPTED";

    private final InFlightTracker tracker;
    private final IdempotencyService idempotencyService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    @Value("${app.kafka.topic.evaluated-results}")
    private String evaluatedResultsTopic;

    /** Hard wall-clock cap for the whole drain. The GCP SPOT preempt window
     *  is 30 s; we target 25 s to leave headroom for the systemd unit's
     *  ExecStop overhead. */
    @Value("${app.preempt.drain-timeout-ms:25000}")
    private long drainTimeoutMs = 25_000L;

    /** Per-listener stop wait — bounded so a hung listener can't eat the
     *  whole drain budget. Spring Kafka's stop(callback) returns once the
     *  current poll finishes; in practice that's the current submission's
     *  ack, which we already short-circuit by pausing first. */
    @Value("${app.preempt.listener-stop-ms:5000}")
    private long listenerStopMs = 5_000L;

    private final AtomicBoolean drained = new AtomicBoolean(false);

    public PreemptionDrainService(InFlightTracker tracker,
                                  IdempotencyService idempotencyService,
                                  KafkaTemplate<String, byte[]> kafkaTemplate,
                                  KafkaListenerEndpointRegistry listenerRegistry) {
        this.tracker = tracker;
        this.idempotencyService = idempotencyService;
        this.kafkaTemplate = kafkaTemplate;
        this.listenerRegistry = listenerRegistry;
    }

    /**
     * Result of one drain pass. Caller (the actuator endpoint or a test)
     * returns these fields as JSON so operator log-scrape can confirm the
     * shape of the loss.
     */
    public record DrainResult(
            boolean alreadyDrained,
            int listenersStopped,
            int inFlightAtStart,
            int verdictsPublished,
            int verdictPublishFailures,
            long elapsedMs
    ) {}

    /**
     * Execute the drain. Safe to call multiple times — the second call
     * short-circuits with {@code alreadyDrained=true}.
     */
    public DrainResult drain() {
        long start = System.nanoTime();
        if (!drained.compareAndSet(false, true)) {
            log.warn("[preempt-drain] Drain already executed — short-circuiting");
            return new DrainResult(true, 0, 0, 0, 0,
                    elapsedMs(start));
        }

        int listenersStopped = stopListeners();

        Collection<InFlightTracker.InFlightSubmission> snapshot = tracker.snapshot();
        int inFlightAtStart = snapshot.size();
        log.warn("[preempt-drain] Draining — listenersStopped={} inFlight={}",
                listenersStopped, inFlightAtStart);

        int published = 0;
        int failures  = 0;
        long deadlineMs = System.currentTimeMillis() + drainTimeoutMs;

        for (InFlightTracker.InFlightSubmission s : snapshot) {
            if (System.currentTimeMillis() > deadlineMs) {
                log.error("[preempt-drain] Timeout exceeded — {} in-flight not verdicted",
                        inFlightAtStart - published - failures);
                failures += (inFlightAtStart - published - failures);
                break;
            }
            try {
                publishPreemptedVerdict(s);
                idempotencyService.markPoison(s.submissionId(), s.phase());
                tracker.deregister(s.submissionId(), s.phase());
                published++;
            } catch (Exception ex) {
                failures++;
                log.error("[preempt-drain] Failed to publish WORKER_PREEMPTED submission={} phase={}: {}",
                        s.submissionId(), s.phase(), ex.getMessage());
            }
        }

        // Belt-and-braces: flush any pending producer batches before we
        // return — the systemd unit will SIGTERM the JVM moments after.
        try {
            kafkaTemplate.flush();
        } catch (Exception ex) {
            log.error("[preempt-drain] KafkaTemplate.flush() failed: {}", ex.getMessage());
        }

        long elapsed = elapsedMs(start);
        log.warn("[preempt-drain] Drain complete — published={} failures={} elapsedMs={}",
                published, failures, elapsed);
        return new DrainResult(false, listenersStopped, inFlightAtStart,
                published, failures, elapsed);
    }

    private int stopListeners() {
        int count = 0;
        for (MessageListenerContainer c : listenerRegistry.getListenerContainers()) {
            if (!c.isRunning()) {
                continue;
            }
            try {
                // Pause first so the listener doesn't pull a fresh record
                // mid-stop. We then ask for a clean stop; if it hangs past
                // listenerStopMs we move on — the JVM is going down anyway.
                c.pause();
                java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
                c.stop(latch::countDown);
                if (!latch.await(listenerStopMs, TimeUnit.MILLISECONDS)) {
                    log.warn("[preempt-drain] Listener {} did not stop within {} ms — continuing",
                            c.getListenerId(), listenerStopMs);
                }
                count++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("[preempt-drain] Interrupted stopping listener {}", c.getListenerId());
            } catch (Exception ex) {
                log.error("[preempt-drain] Failed to stop listener {}: {}",
                        c.getListenerId(), ex.getMessage());
            }
        }
        return count;
    }

    private void publishPreemptedVerdict(InFlightTracker.InFlightSubmission s) throws Exception {
        VerdictEvent v = VerdictEvent.newBuilder()
                .setSubmissionId(s.submissionId())
                .setUserId(s.userId() == null ? "" : s.userId())
                .setProblemId(s.problemId() == null ? "" : s.problemId())
                .setContestId(s.contestId() == null ? "" : s.contestId())
                .setResult(VERDICT_WORKER_PREEMPTED)
                .setExecutionTimeMs(0)
                .setMemoryUsedMb(0)
                .setGatewayTsMs(s.gatewayTsMs())
                .setPoints(0)
                .setPhase(s.phase() == null ? "" : s.phase())
                .setRegion(s.region() == null ? "" : s.region())
                .setEventTsMs(System.currentTimeMillis())
                .build();
        kafkaTemplate.send(evaluatedResultsTopic, s.userId(), v.toByteArray())
                .get(2, TimeUnit.SECONDS);
    }

    private static long elapsedMs(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    // Test seam — let unit tests reset the one-shot guard so they can run
    // multiple drains against a fresh service. Package-private on purpose.
    void resetForTesting() {
        drained.set(false);
    }
}
