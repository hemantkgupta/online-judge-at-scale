package com.onlinejudge.gateway.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.onlinejudge.gateway.service.GcsWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the Google Cloud Storage client used by {@link GcsWriter}.
 *
 * <p>Credentials resolution is handled by the Google client itself: it picks
 * up workload-identity in GKE, the {@code GOOGLE_APPLICATION_CREDENTIALS}
 * env var locally, or gcloud user creds — same precedence as every other
 * Google SDK. We never bake a service account key into the image.
 *
 * <p>Project + bucket are env-var-driven so the same image runs across
 * dev / staging / prod by swapping {@code GCS_PROJECT_ID} and
 * {@code GCS_SOURCE_BUCKET}.
 */
@Configuration
public class GcsConfig {

    @Value("${app.gcs.project-id}")
    private String projectId;

    @Value("${app.gcs.source-bucket}")
    private String sourceBucket;

    @Bean
    public Storage gcsStorage() {
        return StorageOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
    }

    @Bean
    public GcsWriter gcsWriter(Storage gcsStorage) {
        return new GcsWriter(gcsStorage, sourceBucket);
    }
}
