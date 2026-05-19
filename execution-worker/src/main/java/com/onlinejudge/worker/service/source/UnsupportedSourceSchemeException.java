package com.onlinejudge.worker.service.source;

/**
 * Thrown when {@code SubmissionEvent.s3_code_url} carries a scheme the worker
 * has not been compiled or configured to fetch. Surfaces through
 * {@code processSubmission}'s catch-all and ultimately the DLQ path —
 * silently accepting an unknown scheme would let a misrouted producer
 * starve a region of verdicts.
 */
public class UnsupportedSourceSchemeException extends RuntimeException {

    public UnsupportedSourceSchemeException(String url, String reason) {
        super("Unsupported source-code URL scheme (" + reason + "): " + url);
    }
}
