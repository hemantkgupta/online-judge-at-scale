package com.onlinejudge.sandbox.service;

import com.onlinejudge.sandbox.model.Sandbox;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-language warm pool of READY sandboxes. The replenishment loop
 * runs every 500 ms and tops each language pool up to its target depth
 * by asking {@link LeaseService#provisionForPool} to boot fresh
 * Firecracker VMs in the background.
 *
 * <p><b>Why a deque?</b> So we can pop from one end (LIFO — favour
 * recently-booted sandboxes which are likely warmer in the page cache)
 * and offer to the same end on provisioning. {@link BlockingDeque} also
 * gives us atomic poll/offer; the lease path is wait-free.
 *
 * <p><b>Concurrency model:</b>
 * <ul>
 *   <li>The replenish tick is single-threaded (Spring's
 *       {@code @Scheduled}).</li>
 *   <li>Provisioning is offloaded to a small bounded executor so a slow
 *       boot doesn't stall the tick.</li>
 *   <li>{@code provisioning} counts in-flight boots so the next tick
 *       doesn't double-spawn.</li>
 * </ul>
 *
 * <p>{@code LeaseService} depends on this bean (composition); we depend
 * back on {@code LeaseService} via {@link ObjectProvider} to break the
 * cycle (it's only used inside the scheduler thread, never at startup).
 */
@Slf4j
@Component
@ConfigurationProperties(prefix = "app.pool")
public class PoolManager {

    /** Defaults for {@code app.pool.targets} when not configured. */
    private Map<String, Integer> targets = new LinkedHashMap<>(Map.of(
            "python", 3, "cpp", 2, "java", 2));

    /** Defaults for {@code app.pool.max-parallel-boot}. */
    private int maxParallelBoot = 4;

    /** Default 503 retry hint, in milliseconds. */
    private long retryAfterMs = 250;

    private final ConcurrentHashMap<String, BlockingDeque<Sandbox>> readyByLanguage =
            new ConcurrentHashMap<>();

    /** In-flight provisioning counters keyed by language. Subtracted on
     *  completion (success or failure). Used by {@link #replenish} so a
     *  500ms tick that follows another 500ms tick doesn't double-spawn. */
    private final ConcurrentHashMap<String, AtomicInteger> provisioning =
            new ConcurrentHashMap<>();

    private final ExecutorService bootExecutor = Executors.newFixedThreadPool(
            8, r -> {
                Thread t = new Thread(r, "sm-pool-boot");
                t.setDaemon(true);
                return t;
            });

    private final ObjectProvider<LeaseService> leaseServiceProvider;

    private volatile boolean enabled = true;

    public PoolManager(ObjectProvider<LeaseService> leaseServiceProvider) {
        this.leaseServiceProvider = leaseServiceProvider;
    }

    // -- ConfigurationProperties setters/getters ----------------------------

    public Map<String, Integer> getTargets() { return targets; }
    public void setTargets(Map<String, Integer> targets) {
        if (targets != null) this.targets = targets;
    }

    public int getMaxParallelBoot() { return maxParallelBoot; }
    public void setMaxParallelBoot(int v) { this.maxParallelBoot = v; }

    public long getRetryAfterMs() { return retryAfterMs; }
    public void setRetryAfterMs(long v) { this.retryAfterMs = v; }

    // -- Operations ---------------------------------------------------------

    /** Pop one READY sandbox for {@code language}; returns null if empty.
     *  Constant-time, lock-free. */
    public Sandbox acquire(String language) {
        BlockingDeque<Sandbox> q = readyByLanguage.get(language);
        if (q == null) return null;
        return q.pollFirst();
    }

    /** Park a fresh READY sandbox in the pool. Called by
     *  {@link LeaseService#provisionForPool} on successful boot, and by
     *  tests that want to seed a pool without booting. */
    public void offer(String language, Sandbox sb) {
        readyByLanguage
                .computeIfAbsent(language, k -> new LinkedBlockingDeque<>())
                .offerFirst(sb);
    }

    public int readyCount(String language) {
        BlockingDeque<Sandbox> q = readyByLanguage.get(language);
        return q == null ? 0 : q.size();
    }

    public long retryAfterMsForLanguage(String language) {
        return retryAfterMs;
    }

    /** Whether to attempt a provisioning round on the next tick. Off in
     *  unit tests that don't want the scheduler to fight them. */
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    /**
     * Top each language pool up to target. Runs every 500 ms. Idempotent
     * — if the previous tick is still booting, this one observes the
     * elevated {@code provisioning} count and bails early.
     */
    @Scheduled(fixedDelay = 500)
    public void replenish() {
        if (!enabled) return;
        LeaseService leaseService = leaseServiceProvider.getIfAvailable();
        if (leaseService == null) return;

        for (Map.Entry<String, Integer> e : targets.entrySet()) {
            String language = e.getKey();
            int target = e.getValue();
            int current = readyCount(language);
            int inflight = provisioning
                    .computeIfAbsent(language, k -> new AtomicInteger(0))
                    .get();
            int deficit = target - (current + inflight);
            if (deficit <= 0) continue;
            int toBoot = Math.min(deficit, maxParallelBoot);
            for (int i = 0; i < toBoot; i++) {
                provisioning.get(language).incrementAndGet();
                bootExecutor.submit(() -> {
                    try {
                        Sandbox sb = leaseService.provisionForPool(language);
                        if (sb != null) {
                            offer(language, sb);
                        }
                    } catch (Throwable t) {
                        log.warn("[sm] pool boot failed language={}: {}",
                                language, t.getMessage());
                    } finally {
                        provisioning.get(language).decrementAndGet();
                    }
                });
            }
        }
    }

    /** Read-only snapshot of pool depth. For metrics + tests. */
    public Map<String, Integer> snapshot() {
        Map<String, Integer> out = new LinkedHashMap<>();
        readyByLanguage.forEach((k, v) -> out.put(k, v.size()));
        return Collections.unmodifiableMap(out);
    }

    @PreDestroy
    void shutdown() {
        bootExecutor.shutdownNow();
    }
}
