package com.onlinejudge.sandbox.web;

import com.onlinejudge.sandbox.api.LeaseRequest;
import com.onlinejudge.sandbox.api.LeaseResponse;
import com.onlinejudge.sandbox.model.Sandbox;
import com.onlinejudge.sandbox.model.SandboxState;
import com.onlinejudge.sandbox.service.LeaseService;
import com.onlinejudge.sandbox.service.PoolExhaustedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SandboxController}.
 *
 * <p>Post-Workstream-E the controller surface is just {@code /lease} and
 * {@code /release}. The {@code /exec} endpoint is gone — the Execution
 * Service now dials the in-guest agent directly over vsock, so the SM is
 * no longer in the data plane for contestant code. {@code AgentClient}
 * has been deleted from this module as well.
 */
class SandboxControllerTest {

    private LeaseService leaseService;
    private SandboxController controller;

    @BeforeEach
    void setUp() {
        leaseService = mock(LeaseService.class);
        controller = new SandboxController(leaseService);
        ReflectionTestUtils.setField(controller, "vsockUdsDir", "/tmp");
        ReflectionTestUtils.setField(controller, "agentPort", 1234);
    }

    @Test
    void leaseHappyPath_returnsSandboxAndToken() {
        Sandbox sb = sandbox("sb-1", "python", "tok-1", SandboxState.LEASED);
        when(leaseService.lease(eq("sub-1"), eq("python"), eq(256),
                isNull(), isNull(), isNull(), isNull())).thenReturn(sb);

        ResponseEntity<?> resp = controller.lease(
                new LeaseRequest("sub-1", "python", 256, null, null));

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        LeaseResponse body = (LeaseResponse) resp.getBody();
        assertThat(body.sandboxId()).isEqualTo("sb-1");
        assertThat(body.sessionToken()).isEqualTo("tok-1");
        assertThat(body.vsockPort()).isEqualTo(1234);
    }

    @Test
    void leaseFailure_returns500() {
        when(leaseService.lease(anyString(), anyString(), any(),
                any(), any(), any(), any()))
                .thenThrow(new RuntimeException("KVM gone"));

        ResponseEntity<?> resp = controller.lease(
                new LeaseRequest("sub-2", "python", 256, null, null));

        assertThat(resp.getStatusCode().value()).isEqualTo(500);
    }

    @Test
    void leasePoolExhausted_returns503WithRetryAfter() {
        when(leaseService.lease(anyString(), anyString(), any(),
                any(), any(), any(), any()))
                .thenThrow(new PoolExhaustedException("python", 250));

        ResponseEntity<?> resp = controller.lease(
                new LeaseRequest("sub-3", "python", 256, null, null));

        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body.get("error")).isEqualTo("pool_exhausted");
        assertThat(body.get("language")).isEqualTo("python");
        assertThat(body.get("retry_after_ms")).isEqualTo(250L);
    }

    @Test
    void release_alwaysReturns200_evenForUnknownSandbox() {
        // LeaseService.release is best-effort idempotent — controller
        // surfaces 200 regardless. Verify it calls through.
        ResponseEntity<Void> resp = controller.release("sb-whatever");

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        verify(leaseService).release("sb-whatever");
    }

    private static Sandbox sandbox(String id, String lang, String token, SandboxState state) {
        Sandbox sb = new Sandbox(id, lang, 999L,
                "/tmp/fc-" + id + ".sock",
                "/tmp/fc-" + id + "-vsock.sock",
                token);
        sb.transition(state);
        return sb;
    }
}
