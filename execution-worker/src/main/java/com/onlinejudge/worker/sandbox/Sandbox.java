package com.onlinejudge.worker.sandbox;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a single code execution sandbox (Docker container or Firecracker VM).
 *
 * Thread-safe: state transitions use AtomicReference CAS to prevent
 * two threads from simultaneously leasing the same sandbox.
 *
 * Each sandbox has:
 * - A unique ID (for tracking and logging)
 * - A language runtime (determines which pool it belongs to)
 * - A lifecycle state (PROVISIONING → READY → LEASED → DIRTY → TERMINATED)
 * - A session token (set at lease time, verified by the Execution Service)
 * - Timestamps for state transitions (for metrics and debugging)
 */
public class Sandbox {

    private final String id;
    private final String language;
    private final Instant createdAt;
    private final AtomicReference<SandboxState> state;

    // Set at lease time
    private volatile String sessionToken;
    private volatile String submissionId;
    private volatile Instant leasedAt;

    // Docker container ID (set after provisioning)
    private volatile String containerId;

    public Sandbox(String language) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.language = language;
        this.createdAt = Instant.now();
        this.state = new AtomicReference<>(SandboxState.PROVISIONING);
    }

    /**
     * Atomically transitions from expected state to target state.
     * Returns true if the CAS succeeded; false if another thread won the race.
     */
    public boolean compareAndSetState(SandboxState expected, SandboxState target) {
        return state.compareAndSet(expected, target);
    }

    /**
     * Marks the sandbox as READY (provisioning complete).
     * Only valid from PROVISIONING state.
     */
    public boolean markReady(String containerId) {
        if (compareAndSetState(SandboxState.PROVISIONING, SandboxState.READY)) {
            this.containerId = containerId;
            return true;
        }
        return false;
    }

    /**
     * Attempts to lease this sandbox for a specific submission.
     * Only valid from READY state. Returns false if another thread leased it first.
     *
     * The session token is a random UUID that the Execution Service must present
     * when communicating with this sandbox. This prevents a different worker from
     * talking to someone else's sandbox after a network partition or rebalance.
     */
    public boolean tryLease(String submissionId) {
        if (compareAndSetState(SandboxState.READY, SandboxState.LEASED)) {
            this.submissionId = submissionId;
            this.sessionToken = UUID.randomUUID().toString();
            this.leasedAt = Instant.now();
            return true;
        }
        return false;
    }

    /**
     * Marks the sandbox as DIRTY (execution complete, cleanup pending).
     * Only valid from LEASED state.
     */
    public boolean markDirty() {
        return compareAndSetState(SandboxState.LEASED, SandboxState.DIRTY);
    }

    /**
     * Marks the sandbox as TERMINATED (fully destroyed).
     * Valid from DIRTY or LEASED (if watchdog killed it mid-execution).
     */
    public boolean markTerminated() {
        SandboxState current = state.get();
        if (current == SandboxState.DIRTY || current == SandboxState.LEASED) {
            state.set(SandboxState.TERMINATED);
            return true;
        }
        return false;
    }

    public String getId() { return id; }
    public String getLanguage() { return language; }
    public SandboxState getState() { return state.get(); }
    public String getSessionToken() { return sessionToken; }
    public String getSubmissionId() { return submissionId; }
    public String getContainerId() { return containerId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getLeasedAt() { return leasedAt; }

    /**
     * Returns the wall-clock time this sandbox has been leased, in milliseconds.
     * Used by the host-side watchdog to detect stuck executions.
     */
    public long leaseAgeMs() {
        if (leasedAt == null) return 0;
        return Instant.now().toEpochMilli() - leasedAt.toEpochMilli();
    }

    @Override
    public String toString() {
        return String.format("Sandbox[%s/%s state=%s sub=%s]",
                id, language, state.get(), submissionId);
    }
}
