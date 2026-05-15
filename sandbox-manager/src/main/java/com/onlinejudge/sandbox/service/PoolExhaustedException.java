package com.onlinejudge.sandbox.service;

/**
 * Thrown by {@link LeaseService#lease} when no READY sandbox exists for
 * the requested language and the controller should surface a 503 with a
 * {@code retry_after_ms} body. The replenishment loop will be working
 * to bring depth back to target — clients are expected to retry on the
 * SM, not give up.
 */
public class PoolExhaustedException extends RuntimeException {
    private final String language;
    private final long retryAfterMs;

    public PoolExhaustedException(String language, long retryAfterMs) {
        super("pool exhausted for language=" + language);
        this.language = language;
        this.retryAfterMs = retryAfterMs;
    }

    public String language() { return language; }
    public long retryAfterMs() { return retryAfterMs; }
}
