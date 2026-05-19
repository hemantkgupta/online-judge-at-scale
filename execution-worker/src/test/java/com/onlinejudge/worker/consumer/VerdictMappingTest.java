package com.onlinejudge.worker.consumer;

import com.onlinejudge.worker.service.AgentClient.PerTestResult;
import com.onlinejudge.worker.service.ExecutionBackend;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tech-spec §4.2 + §14 L1 — verdict-category disambiguation.
 *
 * <p>The worker historically conflated user-attributable
 * {@code RUNTIME_ERROR} (contestant code crashed inside the sandbox —
 * exit != 0, segfault, uncaught exception) with operator-attributable
 * {@code INTERNAL_ERROR} (worker bug, sandbox-manager fault, jailer
 * failure, disk full). Both became {@code RUNTIME_ERROR} on the wire,
 * which made operator faults look like contestant bugs on the leaderboard.
 *
 * <p>The fix is in {@link SubmissionConsumer#determineVerdict}: the two
 * categories pass through to the verdict as-is and stay distinct on
 * Kafka. These tests pin the mapping for both categories (two cases each)
 * and for the legitimate user-facing verdicts so a future refactor can't
 * silently re-introduce the conflation.
 */
class VerdictMappingTest {

    /** Helper — the second arg (timeLimitMs) only matters for the OK branch. */
    private static ExecutionBackend.ExecutionResult result(String status, int execMs) {
        return new ExecutionBackend.ExecutionResult(
                status, "detail", execMs, 0, List.<PerTestResult>of());
    }

    // ───── RUNTIME_ERROR (user-attributable) — two cases ─────

    @Test
    void runtimeError_fromContestantNonZeroExit_isUserAttributable() {
        // DockerExecutionService maps a non-zero exit to "RUNTIME_ERROR".
        // The consumer must preserve that category — the contestant's code
        // crashed, the leaderboard should attribute it to them.
        String verdict = SubmissionConsumer.determineVerdict(result("RUNTIME_ERROR", 50), 1000);
        assertThat(verdict).isEqualTo("RUNTIME_ERROR");
    }

    @Test
    void runtimeError_fromAgentReportedFault_isUserAttributable() {
        // FirecrackerExecutionService.mapVerdict maps the in-guest agent's
        // RUNTIME_ERROR verdict (e.g. Python uncaught exception, JVM
        // ArithmeticException, C++ segfault) to "RUNTIME_ERROR". Same
        // category as the Docker path — contestant code is at fault.
        String verdict = SubmissionConsumer.determineVerdict(result("RUNTIME_ERROR", 120), 2000);
        assertThat(verdict).isEqualTo("RUNTIME_ERROR");
    }

    // ───── INTERNAL_ERROR (operator-attributable) — two cases ─────

    @Test
    void internalError_fromSandboxManagerIoFailure_isOperatorAttributable() {
        // FirecrackerExecutionService raises INTERNAL_ERROR when the SM call
        // throws IOException (jailer crash, /lease 500, network blip).
        // Pre-§14-L1 this was rewritten to RUNTIME_ERROR — wrong: the
        // contestant didn't cause it, ops needs the signal in its own bucket.
        String verdict = SubmissionConsumer.determineVerdict(result("INTERNAL_ERROR", 0), 1000);
        assertThat(verdict).isEqualTo("INTERNAL_ERROR");
    }

    @Test
    void internalError_fromAgentEmptyResponse_isOperatorAttributable() {
        // AgentClient returns INTERNAL_ERROR on empty/garbled responses
        // from the in-guest agent — operator's vsock or agent binary has a
        // problem. Distinct category from a contestant fault.
        String verdict = SubmissionConsumer.determineVerdict(result("INTERNAL_ERROR", 0), 1000);
        assertThat(verdict).isEqualTo("INTERNAL_ERROR");
    }

    // ───── adjacent verdicts — pin them so the switch stays exhaustive ─────

    @Test
    void timeLimitExceeded_passesThrough() {
        assertThat(SubmissionConsumer.determineVerdict(result("TIME_LIMIT_EXCEEDED", 5000), 1000))
                .isEqualTo("TIME_LIMIT_EXCEEDED");
    }

    @Test
    void okWithinBudget_becomesAccepted() {
        assertThat(SubmissionConsumer.determineVerdict(result("OK", 100), 1000))
                .isEqualTo("ACCEPTED");
    }

    @Test
    void okOverBudget_becomesTimeLimitExceeded() {
        // The agent reports OK (program finished cleanly) but the measured
        // time exceeds the per-problem budget — remap to TLE on the way out.
        assertThat(SubmissionConsumer.determineVerdict(result("OK", 1500), 1000))
                .isEqualTo("TIME_LIMIT_EXCEEDED");
    }

    @Test
    void compileError_passesThroughDistinctly() {
        // Previously fell through to WRONG_ANSWER (default branch). Compile
        // failures are user-attributable but distinct from a runtime crash.
        assertThat(SubmissionConsumer.determineVerdict(result("COMPILE_ERROR", 0), 1000))
                .isEqualTo("COMPILE_ERROR");
    }
}
