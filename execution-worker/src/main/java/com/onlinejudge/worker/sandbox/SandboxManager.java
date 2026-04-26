package com.onlinejudge.worker.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages per-language warm pools of code execution sandboxes.
 *
 * This is the execution tier's resource manager — separated from the
 * Execution Service (job orchestrator) for failure isolation and
 * independent scaling.
 *
 * Responsibilities:
 * 1. Maintain per-language pools at target depth
 * 2. Provision new sandboxes (boot Docker containers)
 * 3. Lease sandboxes to the Execution Service
 * 4. Run host-side watchdog timer to kill stuck containers
 * 5. Destroy sandboxes after execution (never reuse)
 * 6. Track metrics per pool
 *
 * The replenishment loop runs every 500ms and provisions fresh sandboxes
 * to fill any deficit. The watchdog loop runs every 1s and kills sandboxes
 * that have exceeded the maximum lease time (timeout + grace period).
 *
 * Pool targets (configurable per language):
 *   python: 8 (higher — Python executions take 1.5-3s average)
 *   java:   4 (lower — Java executions take 0.5-1.5s average)
 *   cpp:    4 (lowest — C++ executions take 50-200ms average)
 */
@Slf4j
@Component
public class SandboxManager {

    /** Per-language pool configuration: language → target depth */
    private static final Map<String, Integer> DEFAULT_POOL_TARGETS = Map.of(
            "python", 8,
            "java",   4,
            "cpp",    4
    );

    /** Docker images per language */
    private static final Map<String, String> LANGUAGE_IMAGES = Map.of(
            "python", "python:3.12-slim",
            "java",   "openjdk:21-slim",
            "cpp",    "gcc:13"
    );

    @Value("${app.execution.timeout-seconds:5}")
    private int executionTimeoutSeconds;

    @Value("${app.sandbox.watchdog-grace-seconds:10}")
    private int watchdogGraceSeconds;

    @Value("${app.execution.memory-limit-mb:256}")
    private int memoryLimitMb;

    @Value("${app.sandbox.max-parallel-provision:4}")
    private int maxParallelProvision;

    /** Per-language sandbox pools */
    private final Map<String, SandboxPool> pools = new ConcurrentHashMap<>();

    /** All currently alive (non-terminated) sandboxes, for watchdog scanning */
    private final Set<Sandbox> aliveSandboxes = ConcurrentHashMap.newKeySet();

    /** Thread pool for parallel sandbox provisioning */
    private final ExecutorService provisioningExecutor =
            Executors.newFixedThreadPool(4, r -> {
                Thread t = new Thread(r, "sandbox-provisioner");
                t.setDaemon(true);
                return t;
            });

    @PostConstruct
    public void initialize() {
        // Create pools for each language
        DEFAULT_POOL_TARGETS.forEach((language, target) -> {
            pools.put(language, new SandboxPool(language, target));
            log.info("[sandbox] Created pool for language={} target={}", language, target);
        });

        // Initial provisioning
        replenishPools();
    }

    @PreDestroy
    public void shutdown() {
        log.info("[sandbox] Shutting down. Terminating all sandboxes.");
        provisioningExecutor.shutdownNow();
        aliveSandboxes.forEach(this::terminateSandbox);
    }

    /**
     * Lease a sandbox for the given language and submission.
     *
     * Returns a LeaseResult with the sandbox if one is available.
     * Returns LeaseResult.poolExhausted() if no READY sandbox is available.
     *
     * The caller (Execution Service) should:
     * - On success: use the sandbox, then call release()
     * - On pool exhausted: pause the Kafka consumer, back off, retry
     */
    public LeaseResult lease(String language, String submissionId) {
        SandboxPool pool = pools.get(language);
        if (pool == null) {
            // Unknown language — fall back to python pool
            pool = pools.get("python");
        }

        Sandbox sandbox = pool.tryLease(submissionId);
        if (sandbox == null) {
            log.warn("[sandbox] Pool exhausted for language={}. Submission {} must wait.",
                    language, submissionId);
            return LeaseResult.poolExhausted(language);
        }

        log.info("[sandbox] Leased sandbox={} for submission={} language={}",
                sandbox.getId(), submissionId, language);
        return LeaseResult.success(sandbox);
    }

    /**
     * Release a sandbox after execution completes.
     *
     * Transitions: LEASED → DIRTY → TERMINATED.
     * Destroys the Docker container and triggers pool replenishment.
     */
    public void release(Sandbox sandbox) {
        if (!sandbox.markDirty()) {
            log.warn("[sandbox] Failed to mark sandbox={} as DIRTY (state={}). " +
                    "May have been killed by watchdog.", sandbox.getId(), sandbox.getState());
        }

        terminateSandbox(sandbox);

        // Record termination in the pool for metrics
        SandboxPool pool = pools.get(sandbox.getLanguage());
        if (pool != null) {
            pool.recordTerminated();
        }
    }

    /**
     * Replenishment loop: runs every 500ms.
     *
     * For each language pool, computes the deficit (target - ready - provisioning)
     * and provisions fresh sandboxes to fill it. Provisioning happens asynchronously
     * on the provisioning thread pool.
     */
    @Scheduled(fixedDelay = 500)
    public void replenishPools() {
        for (SandboxPool pool : pools.values()) {
            int deficit = pool.computeDeficit();

            if (deficit <= 0) continue;

            // Alert on critical states
            if (pool.isExhausted()) {
                log.error("[sandbox] CRITICAL: Pool {} exhausted! READY=0", pool.language());
            } else if (pool.isCriticallyLow()) {
                log.warn("[sandbox] Pool {} below 20%: {}", pool.language(), pool);
            }

            // Provision up to maxParallelProvision new sandboxes
            int toProvision = Math.min(deficit, maxParallelProvision);
            for (int i = 0; i < toProvision; i++) {
                pool.recordProvisioning();
                provisioningExecutor.submit(() -> provisionSandbox(pool));
            }
        }
    }

    /**
     * Host-side watchdog: runs every 1 second.
     *
     * Scans all LEASED sandboxes and kills any that have exceeded the
     * maximum lease time (execution timeout + grace period).
     *
     * This is the most critical safety mechanism in the execution tier.
     * It runs OUTSIDE the sandbox (on the host), so a malicious submission
     * cannot disable it by:
     * - Sending SIGSTOP to the watchdog
     * - Manipulating the guest clock
     * - Exhausting the guest PID space
     * - Panicking the guest kernel
     *
     * When the watchdog fires, it kills the Docker container (equivalent
     * to killing the Firecracker VMM process in production). The entire
     * sandbox vanishes; there is no guest-side defense.
     */
    @Scheduled(fixedDelay = 1000)
    public void watchdogScan() {
        long maxLeaseMs = (executionTimeoutSeconds + watchdogGraceSeconds) * 1000L;

        for (Sandbox sandbox : aliveSandboxes) {
            if (sandbox.getState() != SandboxState.LEASED) continue;

            long leaseAge = sandbox.leaseAgeMs();
            if (leaseAge > maxLeaseMs) {
                log.warn("[watchdog] Killing sandbox={} for submission={}: " +
                        "lease age {}ms exceeds max {}ms",
                        sandbox.getId(), sandbox.getSubmissionId(),
                        leaseAge, maxLeaseMs);

                // Force-kill: LEASED → TERMINATED (skip DIRTY)
                sandbox.markTerminated();
                terminateSandbox(sandbox);

                SandboxPool pool = pools.get(sandbox.getLanguage());
                if (pool != null) {
                    pool.recordTerminated();
                }
            }
        }
    }

    /**
     * Provisions a single sandbox: creates a Docker container for the language.
     *
     * In production, this would call the Firecracker API over a Unix Domain Socket
     * to boot a MicroVM with the language's root filesystem image. Locally, we
     * create a Docker container in a paused state, ready to execute code.
     *
     * Boot time: ~125ms for Firecracker, ~500ms for Docker (cold pull excluded).
     */
    private void provisionSandbox(SandboxPool pool) {
        Sandbox sandbox = new Sandbox(pool.language());
        aliveSandboxes.add(sandbox);

        try {
            String image = LANGUAGE_IMAGES.getOrDefault(pool.language(), "python:3.12-slim");

            // Create a Docker container (not started yet — just created)
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "create",
                    "--name", "oj-sandbox-" + sandbox.getId(),
                    "--network", "none",
                    "--memory", memoryLimitMb + "m",
                    "--cpus", "0.5",
                    "--pids-limit", "64",
                    "--read-only",
                    "--tmpfs", "/tmp:size=64m",
                    image,
                    "sleep", "infinity"  // Keep container alive until killed
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();
            String containerId = new String(process.getInputStream().readAllBytes()).trim();
            process.waitFor(10, TimeUnit.SECONDS);

            if (process.exitValue() == 0 && !containerId.isBlank()) {
                sandbox.markReady(containerId);
                pool.addReady(sandbox);
                log.debug("[sandbox] Provisioned sandbox={} language={} container={}",
                        sandbox.getId(), pool.language(), containerId.substring(0, 12));
            } else {
                aliveSandboxes.remove(sandbox);
                log.error("[sandbox] Failed to provision sandbox={}: exit={}",
                        sandbox.getId(), process.exitValue());
            }

        } catch (Exception ex) {
            aliveSandboxes.remove(sandbox);
            log.error("[sandbox] Provisioning error for sandbox={}: {}",
                    sandbox.getId(), ex.getMessage());
        }
    }

    /**
     * Terminates a sandbox by destroying its Docker container.
     * This is the equivalent of killing the Firecracker VMM process.
     */
    private void terminateSandbox(Sandbox sandbox) {
        aliveSandboxes.remove(sandbox);
        sandbox.markTerminated();

        String containerId = sandbox.getContainerId();
        if (containerId == null || containerId.isBlank()) return;

        try {
            // docker rm -f: force-remove, kills the container if running
            new ProcessBuilder("docker", "rm", "-f", "oj-sandbox-" + sandbox.getId())
                    .redirectErrorStream(true)
                    .start()
                    .waitFor(5, TimeUnit.SECONDS);

            log.debug("[sandbox] Terminated sandbox={} container={}",
                    sandbox.getId(), containerId.substring(0, Math.min(12, containerId.length())));

        } catch (Exception ex) {
            log.error("[sandbox] Failed to terminate sandbox={}: {}",
                    sandbox.getId(), ex.getMessage());
        }
    }

    // --- Metrics ---

    public Map<String, SandboxPool> getPools() {
        return Collections.unmodifiableMap(pools);
    }

    /**
     * Returns a metrics snapshot for all pools.
     */
    public Map<String, Map<String, Object>> getMetrics() {
        Map<String, Map<String, Object>> metrics = new LinkedHashMap<>();
        for (SandboxPool pool : pools.values()) {
            Map<String, Object> poolMetrics = new LinkedHashMap<>();
            poolMetrics.put("ready", pool.readyCount());
            poolMetrics.put("provisioning", pool.provisioningCount());
            poolMetrics.put("leased", pool.leasedCount());
            poolMetrics.put("target", pool.targetDepth());
            poolMetrics.put("utilization", String.format("%.1f%%", pool.utilization() * 100));
            poolMetrics.put("totalCreated", pool.totalCreated());
            poolMetrics.put("totalTerminated", pool.totalTerminated());
            poolMetrics.put("exhausted", pool.isExhausted());
            poolMetrics.put("criticallyLow", pool.isCriticallyLow());
            metrics.put(pool.language(), poolMetrics);
        }
        return metrics;
    }

    /**
     * Lease result: either a sandbox or a pool-exhausted signal.
     */
    public record LeaseResult(Sandbox sandbox, boolean exhausted, String language) {
        public static LeaseResult success(Sandbox sandbox) {
            return new LeaseResult(sandbox, false, sandbox.getLanguage());
        }
        public static LeaseResult poolExhausted(String language) {
            return new LeaseResult(null, true, language);
        }
        public boolean isSuccess() { return sandbox != null; }
    }
}
