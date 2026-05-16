package com.onlinejudge.worker.service;

/**
 * Thrown by the execution-worker side when the Sandbox Manager replies
 * {@code 503 pool_exhausted}. This is a transient backpressure signal —
 * the contestant did not cause it — and the consumer reacts by nacking
 * the Kafka record for at least {@link #retryAfterMs()} ms <em>without</em>
 * publishing a verdict and <em>without</em> burning an idempotency
 * attempt (roadmap §2.5 + Problem 4 in the prod-readiness brief).
 *
 * <p>Mirrors {@code com.onlinejudge.sandbox.service.PoolExhaustedException}
 * on the SM side, but lives in this module so the worker can react without
 * a dependency on the sandbox-manager artifact.
 */
public class PoolExhaustedException extends RuntimeException {

    private final String language;
    private final long   retryAfterMs;

    public PoolExhaustedException(String language, long retryAfterMs) {
        super("sandbox pool exhausted for language=" + language
                + " (retry_after_ms=" + retryAfterMs + ")");
        this.language     = language;
        this.retryAfterMs = retryAfterMs;
    }

    public String language()      { return language; }
    public long   retryAfterMs()  { return retryAfterMs; }
}
