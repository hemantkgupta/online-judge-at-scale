package com.onlinejudge.worker.sandbox;

/**
 * The five lifecycle states of a code execution sandbox.
 *
 * Every sandbox transitions through exactly this sequence:
 *   PROVISIONING → READY → LEASED → DIRTY → TERMINATED
 *
 * The critical invariant: a used sandbox is NEVER returned to the pool.
 * After execution, the sandbox transitions to DIRTY (volumes unmounted,
 * writable overlay zeroed) and then TERMINATED (process killed, memory
 * reclaimed). The pool replenishes by booting fresh sandboxes.
 *
 * This destroy-never-reuse policy is a security invariant, not an
 * optimization choice: a malicious submission can leave data in memory,
 * shared libraries, or the writable overlay that persists if the sandbox
 * is recycled. The 125ms boot cost is absorbed by the warm pool.
 */
public enum SandboxState {

    /**
     * Container/VM is booting. Language runtime initializing.
     * Not yet available for leasing.
     */
    PROVISIONING,

    /**
     * Idle in the language-specific pool. No user data present.
     * No per-submission limits applied. Waiting for a lease request.
     */
    READY,

    /**
     * Assigned to a specific submission. Per-submission resource limits applied.
     * Host-side watchdog timer active. Session token issued.
     */
    LEASED,

    /**
     * Execution complete (or timeout/crash). Volumes unmounted.
     * Writable overlay securely zeroed. Transitioning to termination.
     */
    DIRTY,

    /**
     * Process killed. Memory reclaimed. Block device destroyed.
     * Pool replenishment triggered for this language.
     */
    TERMINATED
}
