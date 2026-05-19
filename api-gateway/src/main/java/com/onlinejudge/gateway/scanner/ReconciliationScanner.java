package com.onlinejudge.gateway.scanner;

import com.onlinejudge.common.events.Events.SubmissionEvent;
import com.onlinejudge.gateway.model.Submission;
import com.onlinejudge.gateway.observability.GatewayMetrics;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Roadmap §3.9 — periodic sweep that recovers submissions stuck in
 * {@code PENDING} after the outbox publisher missed them.
 *
 * <p><b>Failure mode this closes.</b> Today the outbox publisher is the
 * only thing that drives a submission out of PENDING. If api-gateway
 * crashes <em>between</em> the {@code submissions} row INSERT and the
 * {@code outbox_events} INSERT, there's no outbox row for the publisher
 * to ever pick up — the submission sits PENDING indefinitely. This
 * scanner is the safety net: it walks
 * {@code submissions WHERE status='PENDING' AND created_at < now()-stale}
 * and re-publishes a {@code SubmissionEvent} to the pretest topic for
 * each row. Re-publishes are bounded by
 * {@code app.reconciliation.max-attempts} per row; rows that exceed the
 * cap are routed to the DLQ.
 *
 * <p>The scanner is a strict <em>safety net</em> — the happy path is
 * unchanged. The existing outbox publisher continues to drive every
 * non-stuck submission. We never mutate submissions to a non-PENDING
 * status except for the terminal DLQ path ({@link #routeToDlq}).
 *
 * <p><b>Scheduling.</b> {@code @Scheduled} runs on Spring's default
 * {@code TaskScheduler}, which on this app is the single-thread
 * {@code @EnableScheduling}-provided executor (configured in
 * {@link com.onlinejudge.gateway.ApiGatewayApplication}). Because
 * api-gateway has {@code spring.threads.virtual.enabled=true}, the
 * scheduler thread that fires this method is a virtual thread, so the
 * blocking Kafka {@code .get(5s)} ack does not pin a platform thread.
 *
 * <p><b>Thundering-herd guard.</b> {@code batch-size} caps a single sweep
 * so a long outage can't dump tens of thousands of republishes into
 * Kafka in one go; subsequent sweeps drain the rest.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.reconciliation.enabled", havingValue = "true", matchIfMissing = true)
public class ReconciliationScanner {

    private final StuckSubmissionRepository repo;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    /**
     * Tech-spec §9.3: gateway.reconciliation.{swept,republished,dlq}_total
     * counters. Optional so existing Mockito tests without an explicit
     * mock keep working — the wire-in below null-guards every call.
     */
    @Autowired(required = false)
    private GatewayMetrics gatewayMetrics;

    @Value("${app.kafka.topic.pretest}")
    private String pretestTopic;

    @Value("${app.kafka.topic.dlq:submissions.dlq}")
    private String dlqTopic;

    @Value("${app.reconciliation.stale-after-seconds:900}")
    private long staleAfterSeconds;

    @Value("${app.reconciliation.batch-size:500}")
    private int batchSize;

    @Value("${app.reconciliation.max-attempts:10}")
    private int maxAttempts;

    /**
     * Result of one sweep. Returned so tests can assert on the counters
     * without scraping logs; nothing on the production code path consumes
     * the value today.
     */
    public record ScanResult(int swept, int republished, int dlq) { }

    /**
     * Drive interval: {@code fixedDelayString} treats each sweep as
     * "wait this long after the previous one finishes" — so a slow sweep
     * (Kafka under load) does not stack a second sweep on top of itself.
     *
     * <p>{@code @Transactional} wraps the whole sweep so the per-row
     * {@code bumpReconcileAttempts} / {@code markFailed} updates run in
     * the same transaction context as the read. Each Kafka send is
     * acknowledged synchronously ({@code .get(5s)}) so we only ever bump
     * the attempt counter after the broker has accepted the record.
     */
    @Scheduled(fixedDelayString = "${app.reconciliation.interval-seconds:60}000")
    @Transactional
    public ScanResult sweep() {
        Instant staleBefore = Instant.now().minusSeconds(staleAfterSeconds);
        List<Submission> stuck = repo.findStuckSubmissions(staleBefore, maxAttempts, batchSize);
        if (stuck.isEmpty()) {
            return new ScanResult(0, 0, 0);
        }

        int republished = 0;
        int dlq = 0;
        for (Submission s : stuck) {
            try {
                if (s.getReconcileAttempts() >= maxAttempts) {
                    // Defence-in-depth: the SQL WHERE already filtered these
                    // out, but a row whose counter raced past the cap mid-sweep
                    // (or a future change to the query) should still terminate.
                    routeToDlq(s);
                    dlq++;
                    continue;
                }

                SubmissionEvent ev = buildSubmissionEvent(s);
                // Key by userId — same partitioning rule as the outbox
                // publisher, so all events for one user land on the same
                // partition and are consumed in order. The .get() blocks
                // for the broker ack: we explicitly want to know whether
                // the send succeeded before mutating the row.
                kafkaTemplate.send(pretestTopic, s.getUserId().toString(), ev.toByteArray())
                        .get(5, TimeUnit.SECONDS);
                repo.bumpReconcileAttempts(s.getId());
                republished++;
                log.info("[reconcile] republished submission={} attempt={} userId={} region={}",
                        s.getId(), s.getReconcileAttempts() + 1, s.getUserId(), s.getRegion());
            } catch (Exception ex) {
                // Don't bump the counter — the next sweep will retry this row.
                // We log and continue so a single broker hiccup doesn't poison
                // the whole batch.
                log.warn("[reconcile] republish failed submission={} attempt={}: {}",
                        s.getId(), s.getReconcileAttempts(), ex.getMessage());
            }
        }

        log.info("[reconcile] sweep complete swept={} republished={} dlq={} staleBefore={}",
                stuck.size(), republished, dlq, staleBefore);
        // Tech-spec §9.3 wire-in: three independent counters (separate
        // alerts in infra/observability/). Bulk-increment per sweep
        // matches the ScanResult shape that callers/tests already use.
        if (gatewayMetrics != null) {
            gatewayMetrics.addReconciliationSwept(stuck.size());
            gatewayMetrics.addReconciliationRepublished(republished);
            gatewayMetrics.addReconciliationDlq(dlq);
        }
        return new ScanResult(stuck.size(), republished, dlq);
    }

    /**
     * Terminal path for submissions that exhausted
     * {@code max-attempts} republishes. Flip status to FAILED so the row
     * stops matching the scanner's WHERE clause, then drop a small
     * JSON envelope on the DLQ topic. We deliberately use the same
     * {@code submissions.dlq} topic the execution-worker uses, so a
     * single tail of that topic surfaces every dead submission whatever
     * the cause.
     */
    private void routeToDlq(Submission s) throws Exception {
        repo.markFailed(s.getId());
        String envelope = """
                {"submissionId":"%s","userId":"%s","reason":"reconcile-exhausted","attempts":%d,"region":"%s"}"""
                .formatted(s.getId(), s.getUserId(), s.getReconcileAttempts(), s.getRegion());
        kafkaTemplate.send(dlqTopic, s.getId().toString(), envelope.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .get(5, TimeUnit.SECONDS);
        log.warn("[reconcile] DLQ submission={} attempts={} userId={}",
                s.getId(), s.getReconcileAttempts(), s.getUserId());
    }

    /**
     * Rebuild the proto wire event from the persisted row. Mirrors the
     * fields the outbox publisher fills — the consumer (execution-worker)
     * cannot tell the two senders apart, which is exactly the point: the
     * scanner is a transparent retry of the same event.
     */
    private SubmissionEvent buildSubmissionEvent(Submission s) {
        return SubmissionEvent.newBuilder()
                .setSubmissionId(s.getId().toString())
                .setUserId(s.getUserId().toString())
                .setProblemId(s.getProblemId() != null ? s.getProblemId().toString() : "")
                .setContestId(s.getContestId() != null ? s.getContestId().toString() : "")
                .setS3CodeUrl(s.getS3CodeUrl() != null ? s.getS3CodeUrl() : "")
                .setLanguage(s.getLanguage() != null ? s.getLanguage() : "")
                .setGatewayTsMs(s.getGatewayTsMs())
                .setRegion(s.getRegion() != null ? s.getRegion() : "")
                .setPhase("pretest")
                .build();
    }
}
