package com.onlinejudge.worker.service;

import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.AgentClient.AgentResult;
import com.onlinejudge.worker.service.AgentClient.PerTestResult;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The <b>Execution Service</b> side of Part 7's three-zone split.
 *
 * <p>Job orchestrator for one submission against the Firecracker backend.
 * Does not speak the Firecracker REST API or hold KVM access — those live
 * in the Sandbox Manager (separate privileged daemon). The flow:
 *
 * <ol>
 *   <li><b>Lease</b> — ask the Sandbox Manager (HTTP) for a fresh sandbox of
 *       the requested language. Response includes the vsock UDS path, port,
 *       and the session token to stamp on the agent request.</li>
 *   <li><b>Exec</b> — dial the in-guest Execution Agent directly over vsock
 *       via {@link AgentClient}, shipping <i>all</i> test cases in one round
 *       trip (Workstream B Option-α — no mounted code drive). The agent
 *       runs each ordinal and returns a per-test verdict array.</li>
 *   <li><b>Release</b> — destroy the sandbox via the Sandbox Manager. Runs
 *       in a finally block (destroy-never-reuse is a security invariant).</li>
 * </ol>
 *
 * <p><b>Test-case secrecy:</b> the worker hashes each expected stdout
 * (SHA-256 over {@code stripTrailing()} bytes) before sending. The agent
 * receives only hashes — plaintext expected outputs never leave the trusted
 * host, even if a contestant exploit broke out into the agent.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "app.sandbox.backend", havingValue = "firecracker")
public class FirecrackerExecutionService implements ExecutionBackend {

    @Value("${app.execution.timeout-seconds:5}")
    private int timeoutSeconds;

    @Value("${app.execution.memory-limit-mb:256}")
    private int memoryLimitMb;

    private final SandboxManagerClient sandboxManager;
    private final AgentClient agentClient;
    private final WorkerMetrics metrics;

    public FirecrackerExecutionService(SandboxManagerClient sandboxManager,
                                       AgentClient agentClient,
                                       WorkerMetrics metrics) {
        this.sandboxManager = sandboxManager;
        this.agentClient = agentClient;
        this.metrics = metrics;
        log.info("[firecracker] backend selected — lease via sandbox-manager, "
                + "exec direct over vsock to in-guest agent");
    }

    @Override
    public ExecutionBackend.ExecutionResult execute(
            String submissionId, String language, String code,
            List<TestCaseSpec> testCases,
            int timeLimitMs, int memoryLimitMb) {

        if (!isSupportedLanguage(language)) {
            log.warn("[firecracker] unsupported language={} submission={}", language, submissionId);
            return new ExecutionBackend.ExecutionResult(
                    "INTERNAL_ERROR",
                    "unsupported language for firecracker backend: " + language,
                    0, 0);
        }

        // Roadmap §2.3: per-problem limits override the application.yml
        // defaults. 0 == "use the backend default" (smoke / bypass path).
        int effectiveTimeMs = timeLimitMs > 0 ? timeLimitMs : timeoutSeconds * 1000;
        int effectiveMemoryMb = memoryLimitMb > 0 ? memoryLimitMb : this.memoryLimitMb;

        SandboxManagerClient.Lease lease = null;
        try {
            lease = sandboxManager.lease(submissionId, language, effectiveMemoryMb);
            log.info("[firecracker] LEASED submission={} sandbox={} vsock={}:{} cases={} t={}ms m={}MiB",
                    submissionId, lease.sandboxId(), lease.vsockUdsPath(), lease.vsockPort(),
                    testCases == null ? 0 : testCases.size(),
                    effectiveTimeMs, effectiveMemoryMb);

            long started = System.nanoTime();
            AgentResult r = agentClient.exec(
                    lease.vsockUdsPath(),
                    lease.vsockPort(),
                    lease.sessionToken(),
                    language,
                    code,
                    effectiveTimeMs,
                    testCases == null ? List.of() : testCases);
            if (metrics != null) {
                metrics.recordAgentExecLatency(System.nanoTime() - started, language);
            }

            log.info("[firecracker] submission={} overall={} perTest={}",
                    submissionId, r.overallVerdict(), r.perTest().size());

            return new ExecutionBackend.ExecutionResult(
                    mapVerdict(r.overallVerdict()),
                    summarize(r.perTest(), r.detail()),
                    aggregateTimeMs(r.perTest()),
                    aggregateMemoryMb(r.perTest()),
                    r.perTest() == null ? List.of() : r.perTest());

        } catch (PoolExhaustedException pex) {
            // Propagate — the consumer wants to nack+retry without
            // publishing a verdict. We never leased, so no release.
            throw pex;
        } catch (IOException ex) {
            log.error("[firecracker] sandbox-manager call failed submission={}: {}",
                    submissionId, ex.getMessage());
            return new ExecutionBackend.ExecutionResult(
                    "INTERNAL_ERROR", ex.getMessage(), 0, 0);
        } finally {
            if (lease != null) {
                sandboxManager.release(lease.sandboxId());
            }
        }
    }

    private static boolean isSupportedLanguage(String language) {
        return "python".equals(language) || "java".equals(language) || "cpp".equals(language);
    }

    /**
     * Map the agent's verdict keyword to the {@code ExecutionResult.status}
     * string the rest of the worker speaks. The agent uses {@code TIMEOUT};
     * downstream consumers (verdict consumer, leaderboard) expect the
     * legacy {@code TIME_LIMIT_EXCEEDED} spelling.
     */
    private static String mapVerdict(String agentVerdict) {
        if (agentVerdict == null) return "INTERNAL_ERROR";
        return switch (agentVerdict) {
            case "OK"             -> "OK";
            case "WRONG_ANSWER"   -> "WRONG_ANSWER";
            case "RUNTIME_ERROR"  -> "RUNTIME_ERROR";
            case "COMPILE_ERROR"  -> "COMPILE_ERROR";
            case "TIMEOUT"        -> "TIME_LIMIT_EXCEEDED";
            default               -> "INTERNAL_ERROR";
        };
    }

    /**
     * Until {@code VerdictEvent} proto carries a per-test array (future work),
     * we collapse the per-test detail into the legacy single-string
     * {@code output} field so downstream consumers see <i>something</i>.
     * Format: {@code "<ordinal>:<verdict>"} comma-joined, plus the agent's
     * detail string when present.
     */
    private static String summarize(List<PerTestResult> perTest, String detail) {
        String body = perTest == null ? "" : perTest.stream()
                .map(t -> t.ordinal() + ":" + t.verdict())
                .collect(Collectors.joining(","));
        if (detail != null && !detail.isBlank()) {
            return body.isEmpty() ? detail : body + " | " + detail;
        }
        return body;
    }

    private static int aggregateTimeMs(List<PerTestResult> perTest) {
        if (perTest == null || perTest.isEmpty()) return 0;
        return (int) perTest.stream().mapToLong(PerTestResult::timeMs).max().orElse(0L);
    }

    private static int aggregateMemoryMb(List<PerTestResult> perTest) {
        if (perTest == null || perTest.isEmpty()) return 0;
        long maxKb = perTest.stream().mapToLong(PerTestResult::memoryKb).max().orElse(0L);
        return (int) (maxKb / 1024);
    }
}
