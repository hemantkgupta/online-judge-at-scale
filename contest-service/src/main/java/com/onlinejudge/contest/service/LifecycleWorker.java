package com.onlinejudge.contest.service;

import com.onlinejudge.contest.model.Contest;
import com.onlinejudge.contest.model.ContestState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Background lifecycle worker: watches the clock and triggers automated
 * contest state transitions.
 *
 * Two automated transitions:
 * 1. REGISTRATION → ACTIVE at T0 (startTime)
 * 2. ACTIVE → CLOSED at T1 (endTime)
 *
 * The worker polls every second. This 1-second granularity means the
 * transition fires within 1 second of the scheduled time. Combined with
 * NTP synchronization across gateway pods (±10ms), the effective precision
 * is 1 second — sufficient for contests where the window is 90 minutes.
 *
 * CLOSED → RESULTS is NOT automated here — it requires confirmation that
 * Flink has drained all in-flight events. This is triggered by an admin
 * or by a separate drain-checking job.
 *
 * Concurrency safety: if multiple lifecycle worker instances run (HA setup),
 * only one will succeed in transitioning a given contest — CockroachDB's
 * optimistic locking (@Version) serializes concurrent writes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LifecycleWorker {

    private final ContestService contestService;

    /**
     * Polls every second for contests that need automated transitions.
     */
    @Scheduled(fixedDelay = 1000)
    public void checkLifecycleTransitions() {
        Instant now = Instant.now();

        List<Contest> pending = contestService.findContestsNeedingTransition(now);
        if (pending.isEmpty()) return;

        for (Contest contest : pending) {
            try {
                processContestTransition(contest, now);
            } catch (Exception ex) {
                // Log and continue — don't let one contest's failure block others.
                // The next poll cycle will retry.
                log.error("[lifecycle] Failed to transition contest={}: {}",
                        contest.getId(), ex.getMessage());
            }
        }
    }

    private void processContestTransition(Contest contest, Instant now) {
        switch (contest.getState()) {
            case REGISTRATION -> {
                if (now.isAfter(contest.getStartTime()) || now.equals(contest.getStartTime())) {
                    log.info("[lifecycle] T0 reached for contest={}. Activating.", contest.getId());
                    contestService.activate(contest.getId());
                }
            }
            case ACTIVE -> {
                if (now.isAfter(contest.getEndTime()) || now.equals(contest.getEndTime())) {
                    log.info("[lifecycle] T1 reached for contest={}. Closing.", contest.getId());
                    contestService.close(contest.getId());
                }
            }
            default -> {
                // CREATED, CLOSED, RESULTS — no automated transitions
            }
        }
    }
}
