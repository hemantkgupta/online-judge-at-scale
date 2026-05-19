package com.onlinejudge.worker.preempt;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tech-spec §11.2 — track every submission the worker is actively executing
 * so the SPOT-preemption drain hook can publish a {@code WORKER_PREEMPTED}
 * verdict per in-flight row before the VM is reclaimed.
 *
 * <p>Registration happens once the idempotency claim is held and just before
 * (or right after) the sandbox lease is acquired. Deregistration happens on
 * every terminal outcome — verdict published, DLQ, claim released — so the
 * tracker only ever holds submissions that would otherwise be lost on a
 * 30 s preempt.
 *
 * <p>Concurrency: the consumer runs with {@code concurrency=4} (pretest) +
 * {@code concurrency=2} (system), so registrations and reads happen on
 * different listener-container threads. {@link ConcurrentHashMap} gives us
 * a coherent snapshot for the drain pass without locking the producers.
 */
@Component
public class InFlightTracker {

    /**
     * What we need to remember about a submission so the drain hook can
     * synthesise a faithful {@code WORKER_PREEMPTED} verdict — keyed by
     * idempotency composite key ({@code submissionId:phase}).
     */
    public record InFlightSubmission(
            String submissionId,
            String userId,
            String problemId,
            String contestId,
            String region,
            String phase,
            long gatewayTsMs,
            java.time.Instant leaseStartedAt
    ) {}

    private final ConcurrentMap<String, InFlightSubmission> inFlight = new ConcurrentHashMap<>();

    public void register(InFlightSubmission s) {
        inFlight.put(key(s.submissionId(), s.phase()), s);
    }

    public void deregister(String submissionId, String phase) {
        inFlight.remove(key(submissionId, phase));
    }

    /**
     * Point-in-time snapshot of currently in-flight submissions. Defensive
     * copy: returns a list that's safe to iterate while concurrent
     * deregister() calls mutate the underlying map (the drain hook removes
     * entries as it publishes verdicts; we don't want
     * ConcurrentModificationException semantics to leak through).
     */
    public Collection<InFlightSubmission> snapshot() {
        return Collections.unmodifiableCollection(new java.util.ArrayList<>(inFlight.values()));
    }

    public int size() {
        return inFlight.size();
    }

    /** Drop everything. Called after a successful drain so a re-trigger is a no-op. */
    public void clear() {
        inFlight.clear();
    }

    private static String key(String submissionId, String phase) {
        return submissionId + ":" + phase;
    }
}
