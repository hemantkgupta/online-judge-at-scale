package com.onlinejudge.worker.service.source;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link HttpSourceCodeFetcher}.
 *
 * <p>Uses {@link com.sun.net.httpserver.HttpServer} (JDK built-in) rather
 * than WireMock so the worker module doesn't pull in another test dep.
 * The server is bound to an ephemeral port and torn down per test.
 */
class HttpSourceCodeFetcherTest {

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private HttpSourceCodeFetcher newFetcher(String bearer) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        return new HttpSourceCodeFetcher(client, Duration.ofSeconds(10), bearer);
    }

    // -----------------------------------------------------------------

    @Test
    void happyPath_returnsBody() throws IOException {
        server.createContext("/code", exch -> {
            byte[] body = "print('hi')\n".getBytes(StandardCharsets.UTF_8);
            exch.sendResponseHeaders(200, body.length);
            try (OutputStream out = exch.getResponseBody()) { out.write(body); }
        });

        assertThat(newFetcher(null).fetch(baseUrl + "/code")).isEqualTo("print('hi')\n");
    }

    @Test
    void bearerToken_isAttached() throws IOException {
        AtomicReference<String> seenAuth = new AtomicReference<>();
        server.createContext("/code", exch -> {
            seenAuth.set(exch.getRequestHeaders().getFirst("Authorization"));
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exch.sendResponseHeaders(200, body.length);
            try (OutputStream out = exch.getResponseBody()) { out.write(body); }
        });

        newFetcher("super-secret").fetch(baseUrl + "/code");
        assertThat(seenAuth.get()).isEqualTo("Bearer super-secret");
    }

    @Test
    void authFail_throwsIoException() {
        server.createContext("/code", exch -> {
            byte[] body = "unauthorised".getBytes(StandardCharsets.UTF_8);
            exch.sendResponseHeaders(401, body.length);
            try (OutputStream out = exch.getResponseBody()) { out.write(body); }
        });

        assertThatThrownBy(() -> newFetcher(null).fetch(baseUrl + "/code"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("status=401");
    }

    @Test
    void contentLengthExceedsCap_isRejected() {
        server.createContext("/code", exch -> {
            // Lie a bit: claim oversize content-length up front.
            exch.sendResponseHeaders(200, SourceSizeGuard.SOURCE_MAX_BYTES + 1);
            try (OutputStream out = exch.getResponseBody()) { out.write(new byte[1]); }
        });

        assertThatThrownBy(() -> newFetcher(null).fetch(baseUrl + "/code"))
                .isInstanceOf(SourceTooLargeException.class)
                .hasMessageContaining("scheme=http(s)");
    }

    @Test
    void streamingExceedsCap_isRejected() {
        // Chunked response (sendResponseHeaders(200, 0) → no content-length;
        // server writes until close). Worker streams + counts and must abort.
        server.createContext("/code", exch -> {
            exch.sendResponseHeaders(200, 0);
            try (OutputStream out = exch.getResponseBody()) {
                byte[] chunk = new byte[4096];
                java.util.Arrays.fill(chunk, (byte) 'x');
                for (int i = 0; i < 20; i++) { out.write(chunk); } // 80 KB > 64 KB
            }
        });

        assertThatThrownBy(() -> newFetcher(null).fetch(baseUrl + "/code"))
                .isInstanceOf(SourceTooLargeException.class);
    }

    @Test
    void readTimeout_isMappedToIoException() {
        // Server accepts the connection then sleeps past the read timeout.
        server.createContext("/code", exch -> {
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            exch.sendResponseHeaders(200, 0);
            exch.getResponseBody().close();
        });

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpSourceCodeFetcher fetcher = new HttpSourceCodeFetcher(
                client, Duration.ofMillis(200), null);

        assertThatThrownBy(() -> fetcher.fetch(baseUrl + "/code"))
                .isInstanceOf(IOException.class);
    }
}
