package com.onlinejudge.worker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP client for the Sandbox Manager (Part 7's per-host daemon). Lives
 * inside the Execution Service (= this execution-worker) and is the
 * trust-zone-clean way for the orchestration tier to ask the privileged
 * resource tier for a sandbox.
 *
 * <p>Two calls map 1:1 to the Sandbox Manager's REST API:
 * <ul>
 *   <li>{@link #lease(String, String, Integer)} — request a fresh
 *       sandbox for one submission. Returns lease metadata
 *       (sandbox id, session token, vsock UDS path, vsock port).</li>
 *   <li>{@link #release(String)} — destroy the sandbox.
 *       Destroy-never-reuse is a security invariant; the Manager
 *       SIGKILLs the firecracker process so the entire VM disappears.</li>
 * </ul>
 *
 * <p><b>What used to live here:</b> a third call, {@code exec()}, that
 * POSTed contestant code to the SM's {@code /exec/{id}} endpoint. That
 * put the SM in the data plane carrying user code — wrong per Part 7 of
 * the blog ("Execution Service does not run user code directly. It
 * communicates with the Execution Agent over Firecracker's vsock").
 * Workstream E moved that call out of HTTP and into a direct vsock dial
 * from the worker, implemented in {@link AgentClient}. The SM stays on
 * the lease + release control plane only.
 *
 * <p>This client does NOT retry on its own. The caller
 * (FirecrackerExecutionService) decides whether a 503 from the Manager
 * means "back off Kafka and let redelivery happen" or "fail this
 * submission" — that decision belongs in the orchestrator, not the
 * transport.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SandboxManagerClient {

    /** Sandbox Manager base URL. Defaults to the docker-compose service name
     *  on the compute VM; tests / local dev override via env var. */
    @Value("${app.sandbox.manager-url:http://oj-sandbox-manager:9100}")
    private String managerUrl;

    private final ObjectMapper objectMapper;

    // One client, virtual-thread-friendly. Spring Boot 3.2 + virtual threads
    // means every HTTP call inside a Kafka consumer thread is OK to block.
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public Lease lease(String submissionId, String language, Integer memoryMib) throws IOException {
        Map<String, Object> body = Map.of(
                "submissionId", submissionId,
                "language",     language,
                "memoryMib",    memoryMib == null ? 256 : memoryMib);

        HttpResponse<String> resp = post("/lease", body, Duration.ofSeconds(15));
        if (resp.statusCode() != 200) {
            throw new IOException("sandbox-manager /lease HTTP " + resp.statusCode()
                    + ": " + resp.body());
        }
        JsonNode n = objectMapper.readTree(resp.body());
        return new Lease(
                n.path("sandboxId").asText(),
                n.path("sessionToken").asText(),
                n.path("vsockUdsPath").asText(),
                n.path("vsockPort").asInt());
    }

    public void release(String sandboxId) {
        // Best-effort cleanup — if the SM has crashed, the next time it
        // starts the VM will be gone anyway (process tree dies with it).
        try {
            HttpResponse<String> resp = post("/release/" + sandboxId, Map.of(),
                    Duration.ofSeconds(5));
            if (resp.statusCode() != 200) {
                log.warn("[em] /release/{} HTTP {}: {}",
                        sandboxId, resp.statusCode(), resp.body());
            }
        } catch (IOException e) {
            log.warn("[em] /release/{} failed: {}", sandboxId, e.getMessage());
        }
    }

    private HttpResponse<String> post(String path, Map<String, Object> body, Duration timeout)
            throws IOException {
        try {
            String json = objectMapper.writeValueAsString(body);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(managerUrl + path))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
            return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("sandbox-manager call interrupted: " + path, ie);
        }
    }

    // ---------- DTOs ----------

    /** Lease handle returned by {@link #lease}. Carry around until exec+release done. */
    public record Lease(String sandboxId, String sessionToken,
                         String vsockUdsPath, int vsockPort) {}
}
