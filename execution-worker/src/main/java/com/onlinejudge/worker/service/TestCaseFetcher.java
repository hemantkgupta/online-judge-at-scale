package com.onlinejudge.worker.service;

import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.ProblemServiceClient.TestCaseUrls;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates fetching the per-test case specs for a submission
 * (Workstream B).
 *
 * <p>Two stages:
 * <ol>
 *   <li>Ask the Problem Service for the ordered list of V4-signed GCS download
 *       URLs (one input + one expected_output per ordinal).</li>
 *   <li>For each ordinal: HTTP GET the signed input + expected_output URLs,
 *       SHA-256 the expected output (after stripTrailing — the rule must match
 *       Workstream F's Go agent exactly), and emit a {@link TestCaseSpec}.</li>
 * </ol>
 *
 * <p>The signed URLs returned by Problem Service are
 * {@code https://storage.googleapis.com/<bucket>/<key>?X-Goog-...}, which is
 * deliberately the case so the holder of the URL can GET without GCS-SDK auth.
 * That's what we do here — plain {@link HttpClient}. The {@link GcsClient}
 * bean is reserved for the contestant-source-code path
 * ({@code SubmissionConsumer.resolveSourceCode}), which uses bare {@code gs://}
 * URIs with the SDK.
 *
 * <p><b>Hash canonicalization:</b> SHA-256 over
 * {@code new String(bytes, UTF_8).stripTrailing().getBytes(UTF_8)}. This is
 * the exact rule the Go agent applies before comparing — DO NOT change it
 * without updating {@code infra/firecracker/agent/cmd/agent/main.go}.
 *
 * <p><b>Input is plumbed as a STRING</b> (UTF-8 decoded from GCS bytes). This
 * restricts binary stdin, which competitive programming problems do not use.
 * If a future problem class needs binary stdin we move {@code input} to
 * base64 in the wire format.
 *
 * <p>Sequential fetching for now — pretest ordinals ≤ 10, so the latency cost
 * is bounded. Parallelism is a TODO once system-test ordinals (≤ 100) move
 * through this path.
 */
@Slf4j
@Component
public class TestCaseFetcher {

    private final ProblemServiceClient problemServiceClient;
    private final WorkerMetrics metrics;
    private final HttpClient httpClient;

    // @Autowired marks this as THE constructor Spring should use. Without it,
    // Spring sees two constructors, can't pick, and falls back to looking for
    // a no-arg default — which doesn't exist → "No default constructor found"
    // and the entire ApplicationContext bails out at boot. (Spring's implicit
    // single-constructor autowiring only fires when there is exactly one.)
    @Autowired
    public TestCaseFetcher(ProblemServiceClient problemServiceClient,
                           WorkerMetrics metrics) {
        this(problemServiceClient, metrics, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build());
    }

    /** Test-friendly seam — lets unit tests inject a stubbed HttpClient. */
    TestCaseFetcher(ProblemServiceClient problemServiceClient,
                    WorkerMetrics metrics,
                    HttpClient httpClient) {
        this.problemServiceClient = problemServiceClient;
        this.metrics = metrics;
        this.httpClient = httpClient;
    }

    /**
     * Fetch + canonicalize all test cases for the given problem in the
     * requested phase.
     *
     * @param problemId problem UUID string
     * @param phase     {@code "pretest"} → pretestOnly=true (ordinals 1–10);
     *                  any other value → pretestOnly=false (full suite)
     */
    public List<TestCaseSpec> fetch(String problemId, String phase) throws IOException {
        boolean pretestOnly = "pretest".equals(phase);
        List<TestCaseUrls> urls = problemServiceClient.fetchTestCases(problemId, pretestOnly);

        List<TestCaseSpec> out = new ArrayList<>(urls.size());
        for (TestCaseUrls u : urls) {
            byte[] inputBytes = httpGet(u.inputUrl(), "tests");
            byte[] expectedBytes = httpGet(u.expectedOutputUrl(), "tests");

            String input = new String(inputBytes, StandardCharsets.UTF_8);
            String expectedHash = canonicalHash(expectedBytes);

            out.add(new TestCaseSpec(u.ordinal(), input, expectedHash));
        }
        log.debug("[test-fetcher] problem={} phase={} testCases={}", problemId, phase, out.size());
        return out;
    }

    /**
     * SHA-256 of {@code new String(bytes, UTF_8).stripTrailing().getBytes(UTF_8)}.
     * Lowercase hex. <b>Wire-compatible with Workstream F</b>.
     */
    public static String canonicalHash(byte[] bytes) {
        String stripped = new String(bytes == null ? new byte[0] : bytes,
                StandardCharsets.UTF_8).stripTrailing();
        byte[] digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            digest = md.digest(stripped.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory on every JVM.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private byte[] httpGet(String url, String bucketLabel) throws IOException {
        if (url == null || url.isBlank()) {
            throw new IOException("test-case URL is blank");
        }
        long started = System.nanoTime();
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<byte[]> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() / 100 != 2) {
                throw new IOException("test-case GET HTTP " + resp.statusCode() + ": " + url);
            }
            return resp.body();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("test-case GET interrupted: " + url, ie);
        } finally {
            if (metrics != null) {
                metrics.recordGcsFetchLatency(System.nanoTime() - started, bucketLabel);
            }
        }
    }

    /**
     * One test case in the worker→agent wire format. {@code input} is the
     * UTF-8 stdin string (no binary-stdin support — see class javadoc).
     * {@code expectedHash} is lowercase-hex SHA-256 of the canonicalized
     * expected stdout — the agent compares using the same rule.
     */
    public record TestCaseSpec(int ordinal, String input, String expectedHash) {}
}
