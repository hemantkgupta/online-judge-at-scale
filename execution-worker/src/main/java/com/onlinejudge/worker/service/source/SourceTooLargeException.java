package com.onlinejudge.worker.service.source;

/**
 * Thrown when fetched source code exceeds tech-spec §7.2's
 * {@link SourceSizeGuard#SOURCE_MAX_BYTES} cap. Propagates up through
 * {@code SubmissionConsumer.resolveSourceCode} → the catch-all in
 * {@code processSubmission}, which surfaces it to the DLQ path on
 * repeated failure (attempts cap exceeded).
 */
public class SourceTooLargeException extends RuntimeException {

    public SourceTooLargeException(String scheme, long observedBytes) {
        super("Source exceeds " + SourceSizeGuard.SOURCE_MAX_BYTES
                + "-byte cap (scheme=" + scheme + ", observed=" + observedBytes + ")");
    }
}
