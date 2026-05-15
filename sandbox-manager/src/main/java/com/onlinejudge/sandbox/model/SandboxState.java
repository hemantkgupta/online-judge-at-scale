package com.onlinejudge.sandbox.model;

/**
 * Five-state lifecycle from Part 7 of the blog. A sandbox transitions
 * through exactly these states; <b>a used sandbox is never returned to the
 * pool</b> — destroy-after-use is a security invariant, not an optimization.
 *
 * <pre>
 *   PROVISIONING → READY → LEASED → DIRTY → TERMINATED
 *                                              │
 *           replenish: boot fresh VM ◄────────┘
 * </pre>
 *
 * <p>What each state means today (no pool yet — Phase C will add the
 * PROVISIONING-on-startup replenishment loop):
 * <ul>
 *   <li><b>PROVISIONING</b> — the {@code firecracker} process has been
 *       started but the guest agent hasn't responded on vsock yet.</li>
 *   <li><b>READY</b> — guest agent ack'd, VM is idle and not yet leased.
 *       Today this state is transient (we boot on-demand, so a lease
 *       transitions PROVISIONING → READY → LEASED in one go).</li>
 *   <li><b>LEASED</b> — assigned to a specific Execution Service caller.
 *       The session token gates {@code /exec} and {@code /release}.</li>
 *   <li><b>DIRTY</b> — execution complete (or timeout fired). VM and all
 *       per-job state are about to be destroyed.</li>
 *   <li><b>TERMINATED</b> — Firecracker process killed, vsock UDS unlinked,
 *       memory reclaimed. Replenishment kicks in if the pool depth is below
 *       target (Phase C).</li>
 * </ul>
 */
public enum SandboxState {
    PROVISIONING, READY, LEASED, DIRTY, TERMINATED
}
