package com.onlinejudge.worker.service;

import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.AgentClient.AgentResult;
import com.onlinejudge.worker.service.AgentClient.PerTestResult;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FirecrackerExecutionService} — the Execution Service
 * side of Part 7's three-zone split, post Workstream B's Option-α pivot.
 *
 * <p>Worker now ships code + per-test inputs + expected hashes over vsock
 * in one round trip (no mounted code drive). These tests verify:
 *
 * <ul>
 *   <li>unsupported language → INTERNAL_ERROR, no lease attempted</li>
 *   <li>agent is invoked with the lease's vsock UDS path / port / token
 *       and the per-test list the consumer prepared</li>
 *   <li>agent overall_verdict maps to the right ExecutionResult.status</li>
 *   <li>release is always called, even when the agent throws</li>
 * </ul>
 */
class FirecrackerExecutionServiceTest {

    private static final List<TestCaseSpec> ONE_CASE =
            List.of(new TestCaseSpec(1, "stdin", "deadbeef"));

    private SandboxManagerClient newMockSm() {
        return mock(SandboxManagerClient.class);
    }

    private AgentClient newMockAgent() {
        return mock(AgentClient.class);
    }

    private FirecrackerExecutionService newSvc(SandboxManagerClient sm, AgentClient ac) {
        return new FirecrackerExecutionService(sm, ac, mock(WorkerMetrics.class));
    }

    private static AgentResult agentResult(String overall, int ordinal,
                                           String verdict, long timeMs, long memoryKb) {
        return new AgentResult(overall, "", List.of(
                new PerTestResult(ordinal, verdict, timeMs, memoryKb, "")));
    }

    @Test
    void unsupportedLanguage_shortCircuitsWithoutLeasing() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        FirecrackerExecutionService svc = newSvc(sm, ac);

        var result = svc.execute("s1", "rust", "fn main() {}", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("INTERNAL_ERROR");
        verify(sm, never()).lease(anyString(), anyString(), anyInt());
        verify(sm, never()).release(anyString());
        verify(ac, never()).exec(anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList());
    }

    @Test
    void leaseExecRelease_isAlwaysCalledAndVerdictMapsCorrectly() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        SandboxManagerClient.Lease lease = new SandboxManagerClient.Lease(
                "sb-1", "tok-1", "/tmp/fc-sb-1-vsock.sock", 1234);

        when(sm.lease(anyString(), eq("python"), anyInt())).thenReturn(lease);
        when(ac.exec(eq("/tmp/fc-sb-1-vsock.sock"), eq(1234), eq("tok-1"),
                eq("python"), anyString(), anyInt(), anyList()))
                .thenReturn(agentResult("OK", 1, "OK", 137, 2048));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s1", "python", "print(42)", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("OK");
        assertThat(result.executionTimeMs()).isEqualTo(137);
        assertThat(result.output()).contains("1:OK");

        // Worker must drive the agent directly via the lease's vsock metadata
        // and pass the full test-case list.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<TestCaseSpec>> cases =
                ArgumentCaptor.forClass(List.class);
        verify(ac).exec(eq("/tmp/fc-sb-1-vsock.sock"), eq(1234), eq("tok-1"),
                eq("python"), eq("print(42)"), anyInt(), cases.capture());
        assertThat(cases.getValue()).containsExactlyElementsOf(ONE_CASE);
        verify(sm).release("sb-1");
    }

    @Test
    void timeoutVerdictMapsToTimeLimitExceeded() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        when(sm.lease(anyString(), anyString(), anyInt()))
                .thenReturn(new SandboxManagerClient.Lease("sb", "t", "/tmp/x", 1234));
        when(ac.exec(anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList()))
                .thenReturn(agentResult("TIMEOUT", 1, "TIMEOUT", 5000, 1024));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s2", "python", "while True: pass", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("TIME_LIMIT_EXCEEDED");
    }

    @Test
    void compileErrorPropagates() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        when(sm.lease(anyString(), anyString(), anyInt()))
                .thenReturn(new SandboxManagerClient.Lease("sb", "t", "/tmp/x", 1234));
        when(ac.exec(anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList()))
                .thenReturn(agentResult("COMPILE_ERROR", 1, "COMPILE_ERROR", 80, 0));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s3", "java", "class Solution {", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("COMPILE_ERROR");
    }

    @Test
    void wrongAnswerOverallMaps() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        when(sm.lease(anyString(), anyString(), anyInt()))
                .thenReturn(new SandboxManagerClient.Lease("sb-wa", "tok", "/tmp/x", 1234));
        when(ac.exec(anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList()))
                .thenReturn(agentResult("WRONG_ANSWER", 2, "WRONG_ANSWER", 22, 2300));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s-wa", "python", "print('x')", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("WRONG_ANSWER");
        verify(sm).release("sb-wa");
    }

    @Test
    void leaseFailure_returnsInternalErrorAndNoRelease() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        when(sm.lease(anyString(), anyString(), anyInt()))
                .thenThrow(new IOException("manager unreachable"));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s5", "python", "print()", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("INTERNAL_ERROR");
        assertThat(result.output()).contains("manager unreachable");
        verify(sm, never()).release(anyString());
        verify(ac, never()).exec(anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList());
    }

    @Test
    void agentInternalError_stillReleasesLease() throws IOException {
        SandboxManagerClient sm = newMockSm();
        AgentClient ac = newMockAgent();
        SandboxManagerClient.Lease lease = new SandboxManagerClient.Lease(
                "sb-bad", "tok", "/tmp/x", 1234);
        when(sm.lease(anyString(), anyString(), anyInt())).thenReturn(lease);
        when(ac.exec(any(), anyInt(), anyString(),
                anyString(), anyString(), anyInt(), anyList()))
                .thenReturn(AgentResult.internalError("agent ate it"));

        FirecrackerExecutionService svc = newSvc(sm, ac);
        var result = svc.execute("s6", "python", "print()", ONE_CASE, 0, 0);

        assertThat(result.status()).isEqualTo("INTERNAL_ERROR");
        verify(sm).release("sb-bad");
    }
}
