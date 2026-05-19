package com.onlinejudge.worker.config;

import com.onlinejudge.worker.service.GcsClient;
import com.onlinejudge.worker.service.source.HttpSourceCodeFetcher;
import com.onlinejudge.worker.service.source.S3SourceCodeFetcher;
import com.onlinejudge.worker.service.source.SourceCodeResolver;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Wires the per-scheme source-code fetchers and the central
 * {@link SourceCodeResolver} that {@code SubmissionConsumer} depends on.
 *
 * <h2>Defaults — least surprise</h2>
 * <ul>
 *   <li>{@code data:} and {@code gs://} need no config — they were already
 *       supported and stay enabled unconditionally (the GCS bean comes from
 *       {@link GcsConfig}).</li>
 *   <li>{@code http(s)://} is enabled by default with a 5s connect / 10s read
 *       timeout. Bearer token is empty (anonymous). Operators turn it off
 *       by setting {@code app.source.http.enabled=false}.</li>
 *   <li>{@code s3://} is enabled only when {@code app.source.s3.enabled=true}.
 *       Region is whatever the default AWS chain resolves; override with
 *       {@code app.source.s3.region}.</li>
 *   <li>{@code r2://} is enabled only when {@code app.source.r2.endpoint} is
 *       set (no sane default — every R2 account has a unique endpoint).</li>
 * </ul>
 */
@Configuration
public class SourceCodeResolverConfig {

    // ---------- http(s) ----------------------------------------------

    @Value("${app.source.http.connect-timeout-ms:5000}")
    private long httpConnectTimeoutMs;

    @Value("${app.source.http.read-timeout-ms:10000}")
    private long httpReadTimeoutMs;

    @Value("${app.source.http.bearer-token:}")
    private String httpBearerToken;

    @Bean
    @ConditionalOnProperty(name = "app.source.http.enabled", havingValue = "true", matchIfMissing = true)
    public HttpSourceCodeFetcher httpSourceCodeFetcher() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(httpConnectTimeoutMs))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        return new HttpSourceCodeFetcher(client,
                Duration.ofMillis(httpReadTimeoutMs),
                httpBearerToken);
    }

    // ---------- s3 ----------------------------------------------------

    @Value("${app.source.s3.region:}")
    private String s3Region;

    @Bean(name = "s3SourceCodeFetcher")
    @ConditionalOnProperty(name = "app.source.s3.enabled", havingValue = "true")
    public S3SourceCodeFetcher s3SourceCodeFetcher() {
        S3ClientBuilder b = S3Client.builder();
        if (s3Region != null && !s3Region.isBlank()) {
            b.region(Region.of(s3Region));
        }
        return new S3SourceCodeFetcher(b.build(), "s3");
    }

    // ---------- r2 ----------------------------------------------------

    @Value("${app.source.r2.endpoint:}")
    private String r2Endpoint;

    @Value("${app.source.r2.region:auto}")
    private String r2Region;

    @Bean(name = "r2SourceCodeFetcher")
    @ConditionalOnProperty(name = "app.source.r2.endpoint")
    public S3SourceCodeFetcher r2SourceCodeFetcher() {
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create(r2Endpoint))
                .region(Region.of(r2Region))
                // R2 requires path-style addressing — virtual-hosted-style
                // requires the bucket as a DNS prefix on the R2 endpoint
                // which the SDK won't generate automatically.
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
        return new S3SourceCodeFetcher(client, "r2");
    }

    // ---------- resolver ---------------------------------------------

    /**
     * Both S3-flavoured beans are optional and may be absent at runtime;
     * {@link ObjectProvider#getIfAvailable()} returns null in that case
     * (the resolver tolerates null fetchers by rejecting their scheme).
     */
    @Bean
    public SourceCodeResolver sourceCodeResolver(
            ObjectProvider<GcsClient> gcsClientProvider,
            @org.springframework.beans.factory.annotation.Qualifier("s3SourceCodeFetcher")
            ObjectProvider<S3SourceCodeFetcher> s3Provider,
            @org.springframework.beans.factory.annotation.Qualifier("r2SourceCodeFetcher")
            ObjectProvider<S3SourceCodeFetcher> r2Provider,
            ObjectProvider<HttpSourceCodeFetcher> httpProvider) {
        return new SourceCodeResolver(
                gcsClientProvider.getIfAvailable(),
                s3Provider.getIfAvailable(),
                r2Provider.getIfAvailable(),
                httpProvider.getIfAvailable());
    }
}
