package com.onlinejudge.worker.service;

import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;

import java.util.List;

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
 * is the safe-everywhere local substitute.
 *
 * <p><b>Workstream B (test-case-aware execution)</b>: backends now take the
 * full ordered test-case list and run code against each ordinal. The Firecracker
 * backend ships them over vsock to the in-guest agent; the Docker backend
 * iterates locally and concatenates results (used only for dev / smoke).
 */
public interface ExecutionBackend {

    DockerExecutionService.ExecutionResult execute(
            String submissionId, String language, String code, List<TestCaseSpec> testCases);
}
