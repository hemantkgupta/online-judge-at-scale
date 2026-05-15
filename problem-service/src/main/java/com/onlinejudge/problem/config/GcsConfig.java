package com.onlinejudge.problem.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.onlinejudge.problem.service.GcsSigner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Wires the {@link Storage} client and the {@link GcsSigner} bean from
 * configuration.
 *
 * <p>Two layers of credentials:
 * <ul>
 *   <li>{@code Storage} client credentials — used by the Java GCS SDK to talk
 *       to the GCS API at all. In production this is the VM's attached service
 *       account (application-default credentials). On local dev with no
 *       credentials we hand it {@link NoCredentials} so the client constructs
 *       but never calls GCS.</li>
 *   <li>Signer credentials — used only by {@code signUrl(...)}. Must be a
 *       service account with a private key on disk; cannot be ambient. Path
 *       given by {@code gcs.signer-credentials-path}. Loaded inside
 *       {@link #gcsSigner} so a missing/blank path is tolerated for dev.</li>
 * </ul>
 */
@Slf4j
@Configuration
public class GcsConfig {

    @Bean
    public Storage storage(@Value("${gcs.project-id:}") String projectId) {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (!projectId.isBlank()) {
            builder.setProjectId(projectId);
        }
        try {
            builder.setCredentials(GoogleCredentials.getApplicationDefault());
        } catch (IOException e) {
            log.warn("[gcs-config] No application-default credentials. Storage client built with NoCredentials. " +
                    "OK for local dev; signed-URL generation still works as long as gcs.signer-credentials-path is set.");
            builder.setCredentials(NoCredentials.getInstance());
        }
        return builder.build().getService();
    }

    @Bean
    public GcsSigner gcsSigner(Storage storage,
                               @Value("${gcs.bucket}") String bucket,
                               @Value("${gcs.signer-credentials-path:}") String credentialsPath) {
        ServiceAccountCredentials signerCredentials = GcsSigner.loadCredentials(credentialsPath);
        return new GcsSigner(storage, bucket, signerCredentials);
    }
}
