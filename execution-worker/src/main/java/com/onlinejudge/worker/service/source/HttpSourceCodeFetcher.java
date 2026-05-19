package com.onlinejudge.worker.service.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Plain HTTP(S) GET source-code fetcher.
 *
 * <p>Bridges the gap for deployments where contestant source already lives
 * behind an authenticated HTTPS endpoint (a partner LMS, an internal proxy
 * in front of S3, etc.). The producer stamps a {@code https://…} URL on the
 * {@code SubmissionEvent}, and the worker GETs it inline.
 *
 * <h2>Auth</h2>
 * Optional bearer token via {@code app.source.http.bearer-token}. When
 * unset the request goes anonymous. We deliberately do not support
 * Basic-auth-in-URL ({@code https://user:pass@host}) — those creds end up
 * in worker logs and Kafka traces.
 *
 * <h2>Timeouts</h2>
 * 5s connect, 10s read. Both are configurable via
 * {@code app.source.http.connect-timeout-ms} and {@code app.source.http.read-timeout-ms}.
 *
 * <h2>Size cap</h2>
 * Two-layer guard against tech-spec §7.2:
 * <ol>
 *   <li>Pre-stream: reject if the server advertises {@code Content-Length}
 *       greater than the cap.</li>
 *   <li>Streaming: read at most {@code SOURCE_MAX_BYTES + 1} bytes — a chunked
 *       response that lies about its length still cannot land an oversize
 *       blob in the sandbox.</li>
 * </ol>
 */
public class HttpSourceCodeFetcher {

    private static final String SCHEME = "http(s)";

    private final HttpClient client;
    private final Duration readTimeout;
    private final String bearerToken;

    public HttpSourceCodeFetcher(HttpClient client, Duration readTimeout, String bearerToken) {
        this.client       = client;
        this.readTimeout  = readTimeout;
        this.bearerToken  = bearerToken;
    }

    public String fetch(String url) throws IOException {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(readTimeout)
                .GET();
        if (bearerToken != null && !bearerToken.isBlank()) {
            req.header("Authorization", "Bearer " + bearerToken);
        }

        HttpResponse<InputStream> resp;
        try {
            resp = client.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP source fetch interrupted: " + url, ex);
        }

        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            // Drain so the connection is reclaimable.
            try (InputStream body = resp.body()) { body.readAllBytes(); } catch (IOException ignored) {}
            throw new IOException("HTTP source fetch failed: " + url + " status=" + status);
        }

        long advertised = resp.headers().firstValueAsLong("Content-Length").orElse(-1L);
        SourceSizeGuard.checkAdvertisedLength(advertised, SCHEME);

        try (InputStream body = resp.body()) {
            return SourceSizeGuard.readUpToCap(body, SCHEME);
        }
    }
}
