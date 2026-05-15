package com.onlinejudge.gateway.service;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Writes contestant source bytes to Google Cloud Storage.
 *
 * <p>Per Part 6 of the blog (Submission Service durability invariant), the
 * source must land in GCS BEFORE the Kafka event referencing it is published.
 * If this writer fails the caller MUST surface the failure to the contestant
 * (503) rather than emit an event with a key that points at nothing — that
 * failure mode lets a submission silently dead-letter and the contestant
 * never gets a verdict.
 *
 * <p>The object key is date-partitioned (<code>submissions/yyyy/MM/dd/&lt;id&gt;.txt</code>)
 * so a future GCS lifecycle rule can tier-down or expire old days cheaply.
 */
@Slf4j
public class GcsWriter {

    private static final DateTimeFormatter DATE_PARTITION =
            DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private final Storage storage;
    private final String bucket;

    public GcsWriter(Storage storage, String bucket) {
        this.storage = storage;
        this.bucket = bucket;
    }

    /**
     * Writes {@code sourceBytes} to {@code gs://<bucket>/submissions/<yyyy/MM/dd>/<submissionId>.txt}
     * and returns the GCS object key (without the {@code gs://bucket/} prefix —
     * the caller is responsible for prefixing with the bucket if it wants a
     * URI-style reference).
     *
     * @throws RuntimeException if the GCS write fails — the caller decides
     *                          whether to fail the request or fall back.
     */
    public String write(String submissionId, String sourceBytes) {
        String datePartition = LocalDate.now(ZoneOffset.UTC).format(DATE_PARTITION);
        String gcsKey = "submissions/" + datePartition + "/" + submissionId + ".txt";

        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, gcsKey)
                .setContentType("text/plain; charset=utf-8")
                .build();

        byte[] bytes = (sourceBytes == null ? "" : sourceBytes).getBytes(StandardCharsets.UTF_8);
        try {
            storage.create(blobInfo, bytes);
            log.debug("[gcs-writer] wrote submission={} bucket={} key={} bytes={}",
                    submissionId, bucket, gcsKey, bytes.length);
            return gcsKey;
        } catch (RuntimeException ex) {
            log.error("[gcs-writer] FAILED to write submission={} bucket={} key={}: {}",
                    submissionId, bucket, gcsKey, ex.getMessage());
            throw new RuntimeException(
                    "GCS write failed for submission=" + submissionId + " key=" + gcsKey, ex);
        }
    }

    /** Bucket this writer is bound to (for caller-side gs:// URI construction). */
    public String bucket() {
        return bucket;
    }
}
