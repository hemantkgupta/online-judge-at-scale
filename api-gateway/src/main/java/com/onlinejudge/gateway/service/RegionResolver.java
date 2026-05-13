package com.onlinejudge.gateway.service;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Resolves the {@code region} that should be stamped onto a write.
 *
 * <p>In production, every API gateway pod is pinned to a specific region by
 * deployment; that region also addresses the local CockroachDB replicas via
 * {@code LOCALITY REGIONAL BY ROW} so writes land in-region without a global
 * round-trip. Locally there is only one node, so this resolver:
 *
 * <ul>
 *   <li>Returns the value of the {@code X-Region} request header if the caller
 *       provided one — useful for end-to-end tests that simulate multi-region
 *       traffic.</li>
 *   <li>Otherwise falls back to {@code app.region}, which defaults to
 *       {@code us-east-1}.</li>
 * </ul>
 *
 * <p>The resolved region is persisted on every {@code submissions} and
 * {@code outbox_events} row and propagated into the outbox payload that the
 * publisher / changefeed sends to Kafka, so downstream consumers (analytics,
 * scoring, leaderboard) can see where a submission originated.
 */
@Component
public class RegionResolver {

    /** Header name. Matches Spring's case-insensitive lookup. */
    public static final String REGION_HEADER = "X-Region";

    private final String defaultRegion;

    public RegionResolver(@Value("${app.region:us-east-1}") String defaultRegion) {
        if (defaultRegion == null || defaultRegion.isBlank()) {
            throw new IllegalArgumentException("app.region must not be blank");
        }
        this.defaultRegion = defaultRegion;
    }

    /** Region for a given request — header wins, otherwise the gateway's default. */
    public String resolve(HttpServletRequest request) {
        if (request != null) {
            String fromHeader = request.getHeader(REGION_HEADER);
            if (fromHeader != null && !fromHeader.isBlank()) {
                return fromHeader.trim();
            }
        }
        return defaultRegion;
    }

    /** The gateway's configured default region (no request context). */
    public String defaultRegion() {
        return defaultRegion;
    }
}
