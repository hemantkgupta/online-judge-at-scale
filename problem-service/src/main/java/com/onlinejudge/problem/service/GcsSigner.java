package com.onlinejudge.problem.service;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Generates V4-signed GCS download URLs with a 5-minute TTL.
 *
 * <p>Why V4 and not V2: GCS V4 signatures match the AWS SigV4 wire format
 * (canonical request + scope + HMAC-SHA256), which is what Google's docs
 * now recommend and what every modern HTTP client / debugger understands.
 * V2 is legacy.
 *
 * <p>The signing key is the private key of a Google Cloud service account.
 * In production the JSON key file is mounted from Secret Manager and rotated
 * out of band. The {@code Storage} client itself can use application-default
 * credentials (or no credentials at all on local dev) — only the signing
 * step needs the service-account JSON.
 *
 * <p>Construction strategy:
 * <ul>
 *   <li>If {@code gcs.signer-credentials-path} is set, load
 *       {@link ServiceAccountCredentials} from that file and pass them via
 *       {@code SignUrlOption.signWith(...)} so the signer is explicit.</li>
 *   <li>If unset (local dev / tests), fall back to whatever credentials the
 *       {@link Storage} client itself was built with. The bean is still
 *       constructable so the Spring context starts; unit tests inject a mock
 *       {@code Storage} directly and never touch this branch.</li>
 * </ul>
 */
@Slf4j
@Component
public class GcsSigner {

    /** 5-minute TTL — required by the Execution Service contract. */
    static final long TTL_MINUTES = 5;

    private final Storage storage;
    private final String bucket;
    private final ServiceAccountCredentials signerCredentials;

    public GcsSigner(Storage storage,
                     String bucket,
                     ServiceAccountCredentials signerCredentials) {
        this.storage = storage;
        this.bucket = bucket;
        this.signerCredentials = signerCredentials;
    }

    /**
     * Returns a V4-signed download URL for the given object key, valid for 5
     * minutes from now.
     */
    public String sign(String objectKey) {
        BlobInfo blob = BlobInfo.newBuilder(bucket, objectKey).build();

        Storage.SignUrlOption[] options;
        if (signerCredentials != null) {
            options = new Storage.SignUrlOption[]{
                    Storage.SignUrlOption.withV4Signature(),
                    Storage.SignUrlOption.signWith(signerCredentials)
            };
        } else {
            // Tests and local dev: rely on whatever credentials the Storage
            // client was built with. Will throw at runtime if the client has
            // no signer (which is exactly what we want — fail loudly, don't
            // mint a bogus URL).
            options = new Storage.SignUrlOption[]{
                    Storage.SignUrlOption.withV4Signature()
            };
        }

        URL url = storage.signUrl(blob, TTL_MINUTES, TimeUnit.MINUTES, options);
        return url.toString();
    }

    /**
     * Loads service-account credentials from a JSON key file. Returns
     * {@code null} if the path is blank — the caller decides what that means.
     */
    public static ServiceAccountCredentials loadCredentials(String credentialsPath) {
        if (credentialsPath == null || credentialsPath.isBlank()) {
            log.warn("[gcs-signer] gcs.signer-credentials-path is blank — signed URLs will use ambient creds. " +
                    "OK for local dev; NOT OK in production.");
            return null;
        }
        Path p = Path.of(credentialsPath);
        if (!Files.exists(p)) {
            throw new IllegalStateException("GCS signer credentials file not found: " + credentialsPath);
        }
        try (InputStream in = Files.newInputStream(p)) {
            return ServiceAccountCredentials.fromStream(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load GCS signer credentials from " + credentialsPath, e);
        }
    }
}
