package com.onlinejudge.worker.service;

/**
 * Abstraction over the sandbox runtime that actually executes contestant code.
 *
 * <p>Two implementations:
 * <ul>
 *   <li>{@link DockerExecutionService} — default. Docker containers with cgroup
 *       resource limits, network isolation, and (on Linux, when
 *       {@code app.sandbox.linux-hardening.enabled=true}) a Seccomp-BPF profile,
 *       {@code no-new-privileges}, capability dropping, and a private cgroup
 *       namespace. Works on macOS / Linux.</li>
 *   <li>{@link FirecrackerExecutionService} — production-grade. Per-submission
 *       hardware-isolated microVM via the Firecracker REST API
 *       (Apache 2.0, open-source). Linux-only — requires {@code /dev/kvm}.
 *       Enabled by setting {@code app.sandbox.backend=firecracker}.</li>
 * </ul>
 *
 * <p>The blog's Part 7 calls Firecracker out as the production sandbox; Docker
 * is the safe-everywhere local substitute with the same {@link #execute(String, String, String, String)}
 * contract.
 */
public interface ExecutionBackend {

    DockerExecutionService.ExecutionResult execute(
            String submissionId, String language, String code, String input);
}
