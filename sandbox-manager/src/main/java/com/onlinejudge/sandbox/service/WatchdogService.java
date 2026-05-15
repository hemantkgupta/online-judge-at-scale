package com.onlinejudge.sandbox.service;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Host-side wall-clock watchdog for leased sandboxes. Lives inside the
 * Sandbox Manager because the SM already owns the firecracker pids, the
 * cgroup writes, and the lease state machine — three vantage points the
 * guest doesn't have and the worker shouldn't need.
 *
 * <p>One {@link ScheduledExecutorService} services all sandboxes. Per
 * lease we schedule exactly one task; per release we cancel it. The
 * fire callback is supplied by {@code LeaseService} (it's the one that
 * needs to drive the state machine and SIGKILL the VMM); this class is
 * a thin scheduler so it's trivial to unit-test.
 *
 * <p><b>Concurrency contract:</b> {@link #schedule} replaces any
 * existing future for the same sandbox (defensive — a duplicate lease
 * is a bug, but we'd rather not leak a timer). {@link #cancel} is
 * idempotent — calling it on an unknown sandbox is a no-op.
 */
@Slf4j
@Component
public class WatchdogService {

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(8, r -> {
                Thread t = new Thread(r, "sm-watchdog");
                t.setDaemon(true);
                return t;
            });

    private final ConcurrentHashMap<String, ScheduledFuture<?>> active =
            new ConcurrentHashMap<>();

    /** Schedule {@code onFire} to run when wall-clock reaches {@code fireAtMillis}. */
    public void schedule(String sandboxId, long fireAtMillis, Runnable onFire) {
        long delay = Math.max(0, fireAtMillis - System.currentTimeMillis());
        Runnable wrapped = () -> {
            try {
                onFire.run();
            } catch (Throwable t) {
                log.error("[sm] watchdog fire failed sandbox={}: {}", sandboxId, t.getMessage(), t);
            } finally {
                active.remove(sandboxId);
            }
        };
        ScheduledFuture<?> f = scheduler.schedule(wrapped, delay, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> prev = active.put(sandboxId, f);
        if (prev != null) {
            prev.cancel(false);
            log.warn("[sm] watchdog re-armed sandbox={} (previous timer cancelled)", sandboxId);
        }
    }

    /** Cancel a pending fire. Idempotent. Returns true if a timer was cancelled. */
    public boolean cancel(String sandboxId) {
        ScheduledFuture<?> f = active.remove(sandboxId);
        if (f == null) return false;
        return f.cancel(false);
    }

    /** Test/inspection: how many timers are currently armed. */
    public int activeCount() {
        return active.size();
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
