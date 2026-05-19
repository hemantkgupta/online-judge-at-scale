package com.onlinejudge.worker.service.source;

import com.onlinejudge.worker.service.GcsClient;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

/**
 * Single entry-point that turns {@code SubmissionEvent.s3_code_url} into the
 * contestant's UTF-8 source. Dispatches by scheme to the per-scheme fetchers.
 *
 * <p>Tech-spec §5.1 lists five schemes:
 * <table border="1">
 *   <tr><th>Scheme</th><th>Use</th><th>Auth</th></tr>
 *   <tr><td>{@code data:}</td><td>Inline, dev / smoke tests</td><td>none</td></tr>
 *   <tr><td>{@code gs://}</td><td>GCP production</td><td>workload identity</td></tr>
 *   <tr><td>{@code s3://}</td><td>AWS production</td><td>default AWS chain</td></tr>
 *   <tr><td>{@code r2://}</td><td>Cloudflare R2</td><td>R2 access keys via AWS chain</td></tr>
 *   <tr><td>{@code http(s)://}</td><td>Custom HTTPS proxy / partner LMS</td><td>optional bearer</td></tr>
 * </table>
 *
 * <p>Every path passes through {@link SourceSizeGuard} so a 5 MB blob from any
 * scheme is rejected before reaching the sandbox.
 *
 * <p>Unsupported / unrecognised schemes raise
 * {@link UnsupportedSourceSchemeException}, which the consumer's catch-all
 * surfaces to the idempotency attempts cap → DLQ path (Roadmap §2.5).
 */
@Slf4j
public class SourceCodeResolver {

    /** May be null in tests that exercise the data: path only. */
    private final GcsClient gcsClient;
    /** May be null in deployments without an AWS classpath. */
    private final S3SourceCodeFetcher s3Fetcher;
    /** May be null in deployments without R2 configured. */
    private final S3SourceCodeFetcher r2Fetcher;
    /** May be null in deployments without http(s) fetcher (locked-down workers). */
    private final HttpSourceCodeFetcher httpFetcher;

    public SourceCodeResolver(GcsClient gcsClient,
                              S3SourceCodeFetcher s3Fetcher,
                              S3SourceCodeFetcher r2Fetcher,
                              HttpSourceCodeFetcher httpFetcher) {
        this.gcsClient   = gcsClient;
        this.s3Fetcher   = s3Fetcher;
        this.r2Fetcher   = r2Fetcher;
        this.httpFetcher = httpFetcher;
    }

    public String resolve(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("SubmissionEvent.s3CodeUrl is required");
        }

        if (url.startsWith("data:")) {
            String decoded = decodeDataUrl(url);
            byte[] bytes = decoded.getBytes(StandardCharsets.UTF_8);
            SourceSizeGuard.checkSize(bytes, "data");
            return decoded;
        }

        String lower = url.toLowerCase(Locale.ROOT);

        if (lower.startsWith("gs://")) {
            if (gcsClient == null) {
                throw new UnsupportedSourceSchemeException(url,
                        "gs:// scheme but GcsClient not wired (running without GcsConfig?)");
            }
            try {
                byte[] bytes = SourceSizeGuard.checkSize(gcsClient.fetch(url), "gs");
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException ex) {
                throw new RuntimeException("GCS source fetch failed: " + url, ex);
            }
        }

        if (lower.startsWith("s3://")) {
            return fetchS3Like(url, s3Fetcher, "s3");
        }

        if (lower.startsWith("r2://")) {
            return fetchS3Like(url, r2Fetcher, "r2");
        }

        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            if (httpFetcher == null) {
                throw new UnsupportedSourceSchemeException(url,
                        "http(s):// scheme but HttpSourceCodeFetcher not wired");
            }
            try {
                return httpFetcher.fetch(url);
            } catch (IOException ex) {
                throw new RuntimeException("HTTP source fetch failed: " + url, ex);
            }
        }

        if (lower.startsWith("local://")) {
            throw new UnsupportedSourceSchemeException(url,
                    "local:// URLs do not embed source; local mode must emit data: URLs");
        }

        throw new UnsupportedSourceSchemeException(url, "unknown scheme");
    }

    private String fetchS3Like(String url, S3SourceCodeFetcher fetcher, String scheme) {
        if (fetcher == null) {
            throw new UnsupportedSourceSchemeException(url,
                    scheme + ":// scheme but fetcher not configured");
        }
        try {
            return new String(fetcher.fetch(url), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException(scheme + " source fetch failed: " + url, ex);
        }
    }

    // -----------------------------------------------------------------
    // data: URL decoding — lifted verbatim from the original
    // SubmissionConsumer so existing smoke tests stay green.
    // -----------------------------------------------------------------

    private static String decodeDataUrl(String dataUrl) {
        int comma = dataUrl.indexOf(',');
        if (comma < 0) {
            throw new IllegalArgumentException("Malformed data URL: missing comma separator");
        }

        String metadata = dataUrl.substring(5, comma).toLowerCase(Locale.ROOT);
        String payload = dataUrl.substring(comma + 1);
        if (metadata.contains(";base64")) {
            try {
                return new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Malformed data URL: invalid base64 payload", ex);
            }
        }

        return percentDecode(payload);
    }

    private static String percentDecode(String payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length());
        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            if (c == '%') {
                if (i + 2 >= payload.length()) {
                    throw new IllegalArgumentException("Malformed data URL: incomplete percent escape");
                }
                int hi = Character.digit(payload.charAt(i + 1), 16);
                int lo = Character.digit(payload.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw new IllegalArgumentException("Malformed data URL: invalid percent escape");
                }
                out.write((hi << 4) + lo);
                i += 2;
            } else {
                out.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }
}
