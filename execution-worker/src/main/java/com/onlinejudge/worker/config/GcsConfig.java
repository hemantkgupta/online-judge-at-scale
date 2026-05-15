package com.onlinejudge.worker.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.onlinejudge.worker.service.GcsClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the GCS {@link Storage} client + the worker-side {@link GcsClient}
 * (Workstream B).
 *
 * <p>Credentials resolution is delegated to the Google SDK — workload
 * identity in GKE, {@code GOOGLE_APPLICATION_CREDENTIALS} locally, or
 * {@code gcloud auth} for dev. No service-account key is ever baked into
 * the image.
 *
 * <p>Project + default bucket are env-driven so a single image moves across
 * dev / staging / prod by changing {@code GCS_PROJECT_ID} and
 * {@code GCS_SOURCE_BUCKET}.
 */
@Configuration
public class GcsConfig {

    @Value("${app.gcs.project-id:}")
    private String projectId;

    @Value("${app.gcs.source-bucket:}")
    private String sourceBucket;

    @Bean
    public Storage gcsStorage() {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (projectId != null && !projectId.isBlank()) {
            builder.setProjectId(projectId);
        }
        return builder.build().getService();
    }

    @Bean
    public GcsClient gcsClient(Storage gcsStorage) {
        return new GcsClient(gcsStorage, sourceBucket);
    }
}
