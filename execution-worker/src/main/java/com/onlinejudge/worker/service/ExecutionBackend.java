package com.onlinejudge.worker.service;

import com.onlinejudge.worker.service.AgentClient.PerTestResult;
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
 *
 * <p><b>Roadmap §2.3 (per-problem limits)</b>: {@code timeLimitMs} and
 * {@code memoryLimitMb} carry the per-problem judging budgets that originate
 * in the {@code problems} row. A value of 0 means "use the backend's
 * application.yml default" — keeps the smoke / bypass path
 * ({@code app.problem-service.required=false}) working even when problem-service
 * has not yet been deployed.
 */
public interface ExecutionBackend {

    /**
     * Execute the contestant solution against the given test-case list.
     *
     * @param submissionId  UUID string for logging / lease key
     * @param language      {@code python | java | cpp}
     * @param code          contestant source as a UTF-8 string
     * @param testCases     ordered per-test inputs + expected SHA-256 hashes
     * @param timeLimitMs   per-test wall clock the agent should enforce, or 0
     *                      to fall back to the backend's configured default
     * @param memoryLimitMb per-VM / per-container memory cap, or 0 to fall
     *                      back to the backend's configured default
     */
    ExecutionResult execute(
            String submissionId,
            String language,
            String code,
            List<TestCaseSpec> testCases,
            int timeLimitMs,
            int memoryLimitMb);

    /**
     * Worker-facing execution result. Mirrors
     * {@link DockerExecutionService.ExecutionResult} for legacy fields and
     * additionally carries the per-test breakdown the agent produced — the
     * worker copies that into {@code VerdictEvent.per_test} (roadmap §2.4).
     *
     * <p>{@code perTest} may be empty when the backend short-circuited
     * (unsupported language, sandbox-manager unreachable, etc.) or when the
     * Docker fallback ran a single concatenated test — Workstream B never
     * gave it a per-ordinal breakdown.
     */
    record ExecutionResult(String status, String output,
                           int executionTimeMs, int memoryUsedMb,
                           List<PerTestResult> perTest) {

        /** Back-compat ctor for legacy call sites that don't have per-test data. */
        public ExecutionResult(String status, String output,
                               int executionTimeMs, int memoryUsedMb) {
            this(status, output, executionTimeMs, memoryUsedMb, List.of());
        }
    }
}
