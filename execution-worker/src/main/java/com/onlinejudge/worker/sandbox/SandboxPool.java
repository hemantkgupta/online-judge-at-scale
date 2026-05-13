package com.onlinejudge.worker.sandbox;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe warm pool for a single language runtime.
 *
 * Maintains a queue of READY sandboxes that can be leased on demand.
 * When a sandbox is leased, it leaves the queue and never returns —
 * used sandboxes are destroyed, not recycled. The replenishment loop
 * creates fresh sandboxes to maintain the target pool depth.
 *
 * Data structure choice: ConcurrentLinkedQueue (lock-free FIFO).
 * Why not ArrayBlockingQueue? We don't want to block on lease — if
 * the pool is empty, we return null and let the caller handle backpressure
 * (pause the Kafka consumer, retry after pool replenishes).
 *
 * Counters are atomic to support concurrent reads from metrics exporters
 * without locking the pool.
 */
public class SandboxPool {

    private final String language;
    private final int targetDepth;

    /** READY sandboxes waiting to be leased. */
    private final ConcurrentLinkedQueue<Sandbox> readyQueue = new ConcurrentLinkedQueue<>();

    /** Sandboxes currently in PROVISIONING state (booting, not yet READY). */
    private final AtomicInteger provisioningCount = new AtomicInteger(0);

    /** Sandboxes currently LEASED (executing submissions). */
    private final AtomicInteger leasedCount = new AtomicInteger(0);

    /** Total sandboxes ever created by this pool (monotonic counter). */
    private final AtomicInteger totalCreated = new AtomicInteger(0);

    /** Total sandboxes ever terminated by this pool (monotonic counter). */
    private final AtomicInteger totalTerminated = new AtomicInteger(0);

    public SandboxPool(String language, int targetDepth) {
        this.language = language;
        this.targetDepth = targetDepth;
    }

    /**
     * Attempts to lease a READY sandbox for the given submission.
     *
     * Returns the leased sandbox, or null if the pool is empty.
     * This is O(1) — a single CAS on the queue head.
     *
     * If null: the caller should pause the Kafka consumer (backpressure)
     * and retry after the replenishment loop creates new sandboxes.
     */
    public Sandbox tryLease(String submissionId) {
        while (true) {
            Sandbox sandbox = readyQueue.poll();
            if (sandbox == null) {
                return null; // Pool exhausted
            }

            // CAS: try to lease. If another thread got here first, the CAS fails
            // and we try the next sandbox in the queue.
            if (sandbox.tryLease(submissionId)) {
                leasedCount.incrementAndGet();
                return sandbox;
            }
            // If tryLease failed, the sandbox was already leased by another thread.
            // This is rare (queue poll + CAS is nearly atomic) but possible.
            // Continue to the next sandbox.
        }
    }

    /**
     * Adds a newly provisioned sandbox to the READY pool.
     * Called by the replenishment loop after the sandbox finishes booting.
     */
    public void addReady(Sandbox sandbox) {
        readyQueue.offer(sandbox);
        provisioningCount.decrementAndGet();
        totalCreated.incrementAndGet();
    }

    /**
     * Records that a sandbox has been terminated (post-execution cleanup).
     */
    public void recordTerminated() {
        leasedCount.decrementAndGet();
        totalTerminated.incrementAndGet();
    }

    /**
     * Records that a new sandbox is being provisioned.
     */
    public void recordProvisioning() {
        provisioningCount.incrementAndGet();
    }

    /**
     * Records that a provisioning attempt failed (Docker create errored, boot timed out, etc.).
     * Decrements the in-flight counter so the replenishment loop's deficit calculation
     * stays accurate. Symmetric to {@link #recordProvisioning()} for the failure path —
     * the success path goes through {@link #addReady(Sandbox)}, which decrements the
     * same counter and adds the sandbox to the ready queue.
     */
    public void recordProvisionFailed() {
        provisioningCount.decrementAndGet();
    }

    /**
     * Computes the current deficit: how many more sandboxes need to be provisioned
     * to reach the target pool depth.
     *
     * deficit = target - (ready + provisioning)
     *
     * If deficit > 0, the replenishment loop should boot that many new sandboxes.
     */
    public int computeDeficit() {
        int currentReady = readyQueue.size();
        int inFlight = provisioningCount.get();
        return Math.max(0, targetDepth - (currentReady + inFlight));
    }

    // --- Metrics ---

    public int readyCount() { return readyQueue.size(); }
    public int provisioningCount() { return provisioningCount.get(); }
    public int leasedCount() { return leasedCount.get(); }
    public int targetDepth() { return targetDepth; }
    public String language() { return language; }
    public int totalCreated() { return totalCreated.get(); }
    public int totalTerminated() { return totalTerminated.get(); }

    /**
     * Returns the pool utilization ratio: leased / (leased + ready).
     * Values approaching 1.0 indicate the pool is nearly exhausted.
     */
    public double utilization() {
        int leased = leasedCount.get();
        int ready = readyQueue.size();
        int total = leased + ready;
        return total == 0 ? 0.0 : (double) leased / total;
    }

    /**
     * Returns true if the pool is critically low (below 20% of target depth).
     */
    public boolean isCriticallyLow() {
        return readyQueue.size() < targetDepth * 0.20;
    }

    /**
     * Returns true if the pool is exhausted (zero READY sandboxes).
     */
    public boolean isExhausted() {
        return readyQueue.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("SandboxPool[%s ready=%d provisioning=%d leased=%d target=%d util=%.1f%%]",
                language, readyCount(), provisioningCount(), leasedCount(), targetDepth,
                utilization() * 100);
    }
}
