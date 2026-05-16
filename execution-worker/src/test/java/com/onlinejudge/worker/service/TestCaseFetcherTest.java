package com.onlinejudge.worker.service;

import com.onlinejudge.worker.observability.WorkerMetrics;
import com.onlinejudge.worker.service.ProblemServiceClient.TestCaseBundle;
import com.onlinejudge.worker.service.ProblemServiceClient.TestCaseUrls;
import com.onlinejudge.worker.service.TestCaseFetcher.ProblemSpec;
import com.onlinejudge.worker.service.TestCaseFetcher.TestCaseSpec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TestCaseFetcher} (Workstream B).
 *
 * <p>Mocks {@link ProblemServiceClient} for the URL list and stubs
 * {@link HttpClient} with a per-URL byte-table so we can verify the
 * SHA-256 canonicalization rule wire-by-wire.
 *
 * <p>Hash invariant: SHA-256 of {@code "hello\n".stripTrailing()} = SHA-256
 * of {@code "hello"} = {@code 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824}.
 */
class TestCaseFetcherTest {

    /** Canonical hash of "hello" — verified against an independent CLI. */
    private static final String HELLO_HASH =
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    @Test
    void canonicalHash_stripsTrailingWhitespace() {
        // "hello\n".stripTrailing() == "hello"
        String h1 = TestCaseFetcher.canonicalHash("hello\n".getBytes(StandardCharsets.UTF_8));
        String h2 = TestCaseFetcher.canonicalHash("hello".getBytes(StandardCharsets.UTF_8));
        String h3 = TestCaseFetcher.canonicalHash("hello   \r\n\t".getBytes(StandardCharsets.UTF_8));

        assertThat(h1).isEqualTo(HELLO_HASH);
        assertThat(h2).isEqualTo(HELLO_HASH);
        assertThat(h3).isEqualTo(HELLO_HASH);
    }

    @Test
    void canonicalHash_handlesEmpty() {
        // SHA-256("") = "e3b0c4..."
        String h = TestCaseFetcher.canonicalHash(new byte[0]);
        assertThat(h).isEqualTo(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(TestCaseFetcher.canonicalHash(null)).isEqualTo(h);
    }

    @Test
    void fetch_pretestPhase_callsProblemServiceWithPretestOnlyTrue_andHashesExpected() throws Exception {
        ProblemServiceClient psc = mock(ProblemServiceClient.class);
        WorkerMetrics metrics = mock(WorkerMetrics.class);

        when(psc.fetchTestCases(eq("p-1"), eq(true)))
                .thenReturn(new TestCaseBundle(1000, 256, List.of(
                        new TestCaseUrls(1,
                                "https://storage.googleapis.com/oj-tests/p-1/1/input?sig=A",
                                "https://storage.googleapis.com/oj-tests/p-1/1/expected?sig=B"))));

        Map<String, byte[]> table = new HashMap<>();
        table.put("https://storage.googleapis.com/oj-tests/p-1/1/input?sig=A",
                "3 1 4 1 5\n".getBytes(StandardCharsets.UTF_8));
        table.put("https://storage.googleapis.com/oj-tests/p-1/1/expected?sig=B",
                "hello\n".getBytes(StandardCharsets.UTF_8));

        TestCaseFetcher fetcher = new TestCaseFetcher(psc, metrics, fakeClient(table));

        ProblemSpec result = fetcher.fetch("p-1", "pretest");
        List<TestCaseSpec> specs = result.testCases();

        assertThat(specs).hasSize(1);
        TestCaseSpec spec = specs.get(0);
        assertThat(spec.ordinal()).isEqualTo(1);
        assertThat(spec.input()).isEqualTo("3 1 4 1 5\n");
        assertThat(spec.expectedHash()).isEqualTo(HELLO_HASH);
        // Roadmap §2.3 — per-problem limits travel on the ProblemSpec.
        assertThat(result.timeLimitMs()).isEqualTo(1000);
        assertThat(result.memoryLimitMb()).isEqualTo(256);

        verify(metrics, org.mockito.Mockito.atLeastOnce())
                .recordGcsFetchLatency(org.mockito.ArgumentMatchers.anyLong(), eq("tests"));
    }

    @Test
    void fetch_systemPhase_passesPretestOnlyFalse() throws Exception {
        ProblemServiceClient psc = mock(ProblemServiceClient.class);
        when(psc.fetchTestCases(any(), anyBoolean()))
                .thenReturn(new TestCaseBundle(0, 0, List.of()));

        TestCaseFetcher fetcher = new TestCaseFetcher(psc, mock(WorkerMetrics.class), fakeClient(Map.of()));
        fetcher.fetch("p-1", "system");

        verify(psc).fetchTestCases(eq("p-1"), eq(false));
    }

    // ---------- helpers ----------

    /**
     * A tiny stubbed {@link HttpClient} that resolves each GET against a fixed
     * URL → bytes table. We do this rather than spin up a real test server
     * because the call shape we exercise is "GET this signed URL, give me
     * bytes" — there is no interesting HTTP-protocol behaviour here.
     */
    private static HttpClient fakeClient(Map<String, byte[]> table) {
        HttpClient client = mock(HttpClient.class);
        try {
            when(client.send(any(HttpRequest.class), any())).thenAnswer(inv -> {
                HttpRequest req = inv.getArgument(0);
                String url = req.uri().toString();
                byte[] body = table.get(url);
                if (body == null) {
                    throw new IOException("no fake response wired for " + url);
                }
                return new FakeResponse(req.uri(), 200, body);
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return client;
    }

    /** Tiny HttpResponse stand-in for the fake client. */
    private record FakeResponse(URI uri, int status, byte[] body)
            implements HttpResponse<byte[]> {
        @Override public int statusCode() { return status; }
        @Override public HttpRequest request() { return null; }
        @Override public Optional<HttpResponse<byte[]>> previousResponse() { return Optional.empty(); }
        @Override public HttpHeaders headers() { return HttpHeaders.of(Map.of(), (a, b) -> true); }
        @Override public byte[] body() { return body; }
        @Override public Optional<SSLSession> sslSession() { return Optional.empty(); }
        @Override public URI uri() { return uri; }
        @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
    }

    /** Unused stand-in to silence "unused import" warnings if needed later. */
    @SuppressWarnings("unused")
    private static <T> Function<T, CompletableFuture<T>> async() {
        return CompletableFuture::completedFuture;
    }
}
