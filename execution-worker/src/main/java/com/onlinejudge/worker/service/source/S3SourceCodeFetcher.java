package com.onlinejudge.worker.service.source;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.Locale;

/**
 * S3-API source-code fetcher. One instance per scheme — there are two:
 *
 * <ul>
 *   <li>{@code s3://} — AWS S3. Auth via the default AWS credential
 *       chain (instance profile in EC2/EKS, env vars in dev, SSO locally).
 *       Region from {@code AWS_REGION} or the SDK's regional defaults.</li>
 *   <li>{@code r2://} — Cloudflare R2. Same wire protocol as S3, but the
 *       SDK must be pointed at the R2 endpoint
 *       ({@code https://&lt;account&gt;.r2.cloudflarestorage.com}) and region
 *       {@code auto}. Credentials are R2 access keys, also resolved through
 *       the default AWS chain so a single secret-manager entry covers both.</li>
 * </ul>
 *
 * <p>URL shape (both schemes): {@code <scheme>://<bucket>/<key…>}.
 *
 * <h2>Size cap</h2>
 * Tech-spec §7.2 cap is enforced two ways:
 * <ol>
 *   <li>The S3 response carries {@code Content-Length}; reject before
 *       buffering the body.</li>
 *   <li>The {@code ResponseTransformer.toBytes()} result is also re-checked
 *       — belt-and-braces against an S3-API implementation that mis-reports
 *       the header.</li>
 * </ol>
 *
 * <p>The {@link S3Client} is injected so tests and Cloudflare R2 can share
 * the implementation without duplicating fetch logic. Production wiring
 * lives in {@code SourceCodeResolverConfig}.
 */
public class S3SourceCodeFetcher {

    private final S3Client client;
    private final String scheme;

    public S3SourceCodeFetcher(S3Client client, String scheme) {
        this.client = client;
        this.scheme = scheme;
    }

    public byte[] fetch(String url) throws IOException {
        BucketKey bk = parse(url, scheme);
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bk.bucket())
                .key(bk.key())
                .build();
        try {
            ResponseBytes<GetObjectResponse> bytes =
                    client.getObject(req, ResponseTransformer.toBytes());
            long advertised = bytes.response().contentLength() == null
                    ? -1L : bytes.response().contentLength();
            SourceSizeGuard.checkAdvertisedLength(advertised, scheme);
            return SourceSizeGuard.checkSize(bytes.asByteArray(), scheme);
        } catch (NoSuchKeyException ex) {
            throw new IOException(scheme + " object not found: " + url, ex);
        } catch (S3Exception ex) {
            throw new IOException(scheme + " fetch failed: " + url
                    + " status=" + ex.statusCode(), ex);
        }
    }

    static BucketKey parse(String url, String scheme) throws IOException {
        String prefix = scheme + "://";
        String lower = url.toLowerCase(Locale.ROOT);
        if (!lower.startsWith(prefix)) {
            throw new IOException("Expected " + prefix + " URL, got: " + url);
        }
        String rest = url.substring(prefix.length());
        int slash = rest.indexOf('/');
        if (slash <= 0 || slash == rest.length() - 1) {
            throw new IOException("Malformed " + scheme + " URL: " + url);
        }
        return new BucketKey(rest.substring(0, slash), rest.substring(slash + 1));
    }

    record BucketKey(String bucket, String key) {}
}
