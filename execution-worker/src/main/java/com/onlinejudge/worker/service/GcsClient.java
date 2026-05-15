package com.onlinejudge.worker.service;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Locale;

/**
 * Thin GCS GET client used by the worker (Workstream B).
 *
 * <p>Two object kinds flow through here:
 * <ul>
 *   <li><b>Contestant source code</b> — written by the API gateway
 *       ({@code GcsWriter}) when the submission landed; key shape
 *       {@code submissions/yyyy/MM/dd/<id>.txt}.</li>
 *   <li><b>Test case bytes</b> — admin-uploaded inputs and expected outputs.
 *       The Problem Service returns V4-signed URLs; the {@link TestCaseFetcher}
 *       resolves those down to bucket/key and calls this client with
 *       {@code gs://...} URIs.</li>
 * </ul>
 *
 * <p>Input format accepted by {@link #fetch(String)}:
 * <ul>
 *   <li>{@code gs://<bucket>/<key>} — fully qualified.</li>
 *   <li>{@code <key>} — relative; resolved against {@code app.gcs.source-bucket}.</li>
 * </ul>
 *
 * <p>Network failures and "object not found" surface as {@link IOException}
 * with a descriptive message — the caller decides whether to retry or fail
 * the submission. Workload-identity auth is handled by the {@link Storage}
 * bean wired in {@link com.onlinejudge.worker.config.GcsConfig}.
 */
@Slf4j
public class GcsClient {

    private final Storage storage;
    private final String defaultBucket;

    public GcsClient(Storage storage, String defaultBucket) {
        this.storage = storage;
        this.defaultBucket = defaultBucket;
    }

    /**
     * Fetch the bytes at {@code gcsUri}. Throws {@link IOException} on miss
     * or transport failure so the caller can NACK the Kafka message.
     */
    public byte[] fetch(String gcsUri) throws IOException {
        if (gcsUri == null || gcsUri.isBlank()) {
            throw new IOException("gcsUri is null or blank");
        }
        BlobId id = parse(gcsUri);
        try {
            Blob blob = storage.get(id);
            if (blob == null || !blob.exists()) {
                throw new IOException("GCS object not found: " + id);
            }
            byte[] bytes = blob.getContent();
            log.debug("[gcs] fetched gs://{}/{} bytes={}", id.getBucket(), id.getName(), bytes.length);
            return bytes;
        } catch (RuntimeException ex) {
            log.error("[gcs] fetch failed gs://{}/{}: {}", id.getBucket(), id.getName(), ex.getMessage());
            throw new IOException("GCS fetch failed: " + id, ex);
        }
    }

    /** Bucket this client defaults to when given a bare key. */
    public String defaultBucket() {
        return defaultBucket;
    }

    private BlobId parse(String gcsUri) throws IOException {
        String lower = gcsUri.toLowerCase(Locale.ROOT);
        if (lower.startsWith("gs://")) {
            String rest = gcsUri.substring("gs://".length());
            int slash = rest.indexOf('/');
            if (slash <= 0 || slash == rest.length() - 1) {
                throw new IOException("Malformed gs:// URI: " + gcsUri);
            }
            return BlobId.of(rest.substring(0, slash), rest.substring(slash + 1));
        }
        if (defaultBucket == null || defaultBucket.isBlank()) {
            throw new IOException(
                    "Bare GCS key requires app.gcs.source-bucket to be set: " + gcsUri);
        }
        return BlobId.of(defaultBucket, gcsUri);
    }
}
