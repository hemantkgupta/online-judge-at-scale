package com.onlinejudge.worker.preempt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Localhost-only trigger surface for {@link PreemptionDrainService}.
 *
 * <p>A systemd unit on {@code oj-compute} fires {@code curl -X POST
 * http://127.0.0.1:8082/actuator/preempt-drain -H 'X-Preempt-Token: …'}
 * from its {@code ExecStop=} hook when GCP delivers the SPOT preempt
 * signal. The execution-worker then drains in-flight submissions before
 * the JVM is SIGTERMed.
 *
 * <p>Why not Spring MVC?  The worker pulls
 * {@code spring-boot-starter-actuator} but deliberately NOT
 * {@code spring-boot-starter-web} — adding Tomcat just to expose this one
 * endpoint would inflate the image and broaden the attack surface. The
 * JDK's {@code com.sun.net.httpserver.HttpServer} ships in every JDK and
 * is enough for one POST.
 *
 * <p><b>Security model.</b> The HTTP server binds to {@code 127.0.0.1}
 * only (so the endpoint is unreachable from outside the VM at the TCP
 * layer) AND requires a shared-secret {@code X-Preempt-Token} header
 * (so a co-tenant process on the VM can't trip the drain). The token
 * comes from {@code APP_PREEMPT_TOKEN} in the worker's env, written by
 * the startup script from Secret Manager.
 */
@Slf4j
@Component
public class PreemptDrainEndpoint {

    private final PreemptionDrainService drainService;
    private final ObjectMapper objectMapper;

    @Value("${app.preempt.endpoint.host:127.0.0.1}")
    private String host;

    @Value("${app.preempt.endpoint.port:8082}")
    private int port;

    @Value("${app.preempt.endpoint.path:/actuator/preempt-drain}")
    private String path;

    /** Shared secret expected in the {@code X-Preempt-Token} header. Blank
     *  disables the endpoint entirely (default in unit-test profiles). */
    @Value("${app.preempt.endpoint.token:}")
    private String expectedToken;

    private HttpServer server;

    public PreemptDrainEndpoint(PreemptionDrainService drainService,
                                ObjectMapper objectMapper) {
        this.drainService = drainService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void start() throws IOException {
        if (expectedToken == null || expectedToken.isBlank()) {
            log.info("[preempt-endpoint] APP_PREEMPT_TOKEN unset — drain endpoint disabled");
            return;
        }
        if (!"127.0.0.1".equals(host) && !"localhost".equals(host) && !"0.0.0.0".equals(host)) {
            // Defensive — refuse weird hosts. 0.0.0.0 is allowed (the
            // docker compose entry binds the host port to 127.0.0.1 so
            // the external interface is still protected at the OS layer)
            // but any other interface bind is rejected.
            log.error("[preempt-endpoint] Refusing to bind preempt-drain to host={}", host);
            return;
        }
        if ("0.0.0.0".equals(host)) {
            log.warn("[preempt-endpoint] Binding to 0.0.0.0 — caller must enforce "
                    + "loopback-only ingress at the network layer (docker -p 127.0.0.1:…).");
        }

        server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext(path, this::handle);
        server.setExecutor(null); // default sync executor — one drain call is cheap
        server.start();
        log.info("[preempt-endpoint] Listening on http://{}:{}{}", host, port, path);
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            // 0s grace — we're shutting down; nothing in flight on this server
            // matters because the drain already ran (or never will).
            server.stop(0);
            log.info("[preempt-endpoint] Stopped");
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
                return;
            }
            String supplied = exchange.getRequestHeaders().getFirst("X-Preempt-Token");
            if (!constantTimeEquals(expectedToken, supplied)) {
                log.warn("[preempt-endpoint] Token mismatch from {}",
                        exchange.getRemoteAddress());
                respond(exchange, 401, "{\"error\":\"unauthorized\"}");
                return;
            }
            log.warn("[preempt-endpoint] Drain triggered from {}", exchange.getRemoteAddress());
            PreemptionDrainService.DrainResult result = drainService.drain();
            respond(exchange, 200, objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.error("[preempt-endpoint] Handler failed: {}", ex.getMessage(), ex);
            respond(exchange, 500, "{\"error\":\"internal\"}");
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
    }

    /** {@link java.security.MessageDigest#isEqual(byte[], byte[])}-style
     *  comparison so a token-prefix attacker can't time the response. */
    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        byte[] ab = a.getBytes(StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(StandardCharsets.UTF_8);
        return java.security.MessageDigest.isEqual(ab, bb);
    }
}
