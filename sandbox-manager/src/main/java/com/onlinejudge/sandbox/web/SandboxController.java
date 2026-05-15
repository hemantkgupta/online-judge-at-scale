package com.onlinejudge.sandbox.web;

import com.onlinejudge.sandbox.api.LeaseRequest;
import com.onlinejudge.sandbox.api.LeaseResponse;
import com.onlinejudge.sandbox.model.Sandbox;
import com.onlinejudge.sandbox.service.LeaseService;
import com.onlinejudge.sandbox.service.PoolExhaustedException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP API the Execution Service calls. Two endpoints:
 *
 * <pre>
 *   POST /lease           lease a fresh sandbox for one submission
 *   POST /release/{id}    destroy the sandbox and reclaim its slot
 * </pre>
 *
 * <p>Both are naturally idempotent — the SM either has the sandbox or doesn't.
 *
 * <p><b>What used to live here:</b> a third endpoint, {@code POST /exec/{id}},
 * that accepted contestant code and proxied it over vsock to the in-guest
 * agent. That put the SM in the data plane carrying user code — wrong
 * per Part 7 of the blog: "Execution Service does not run user code
 * directly. It communicates with the Execution Agent over Firecracker's
 * vsock." Workstream E removed {@code /exec} from the SM. The Execution
 * Service now dials the agent directly using the {@code vsockUdsPath} +
 * {@code vsockPort} + {@code sessionToken} the lease response already
 * carries.
 */
@Slf4j
@RestController
@RequestMapping
@RequiredArgsConstructor
public class SandboxController {

    private final LeaseService leaseService;

    @Value("${app.firecracker.vsock-uds-dir}")
    private String vsockUdsDir;

    @Value("${app.agent.port}")
    private int agentPort;

    @PostMapping("/lease")
    public ResponseEntity<?> lease(@RequestBody LeaseRequest req) {
        try {
            Sandbox sb = leaseService.lease(
                    req.submissionId(), req.language(), req.memoryMib(),
                    req.cpuQuotaUs(), req.cpuPeriodUs(),
                    req.codeDrivePath(), req.wallSeconds());
            return ResponseEntity.ok(new LeaseResponse(
                    sb.sandboxId(),
                    sb.sessionToken(),
                    sb.vsockUdsPath(),
                    agentPort,
                    sb.state().name()));
        } catch (PoolExhaustedException ex) {
            // Pool is empty for the requested language. Replenishment is
            // running; clients should back off and retry. The
            // retry_after_ms hint mirrors HTTP's Retry-After semantics
            // but in milliseconds so the JSON consumer can use it
            // without parsing two clock formats.
            log.warn("[sm] /lease 503 — pool exhausted language={}", ex.language());
            return ResponseEntity.status(503).body(Map.of(
                    "error", "pool_exhausted",
                    "language", ex.language(),
                    "retry_after_ms", ex.retryAfterMs()));
        } catch (Exception ex) {
            log.error("[sm] /lease failed: {}", ex.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/release/{sandboxId}")
    public ResponseEntity<Void> release(@PathVariable String sandboxId) {
        leaseService.release(sandboxId);
        return ResponseEntity.ok().build();
    }
}
