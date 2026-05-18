package com.onlinejudge.gateway.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Set;

/**
 * Multi-region routing filter (tech-spec §12).
 *
 * <p>Every authenticated request carries a {@code region} claim in the JWT —
 * stamped at signup as whichever gateway pod accepted the registration, then
 * read back into every subsequent access token on login / refresh. The user's
 * data lives in that home region (CockroachDB REGIONAL BY ROW + the regional
 * Kafka topic family {@code submissions.<region>.pretest}). If a request lands
 * on the <em>wrong</em> regional gateway pod — because the user's DNS resolver
 * picked the closer region, or because a load balancer hashed them onto a
 * different pool — this filter sends a 307 Temporary Redirect to the user's
 * home-region gateway so subsequent requests reach the data.
 *
 * <p>307 (not 301/302) is the right status because it preserves the HTTP
 * method and body — a {@code POST /submissions} that arrived in the wrong
 * region replays as a {@code POST /submissions} in the right one. 302 would
 * downgrade some clients to GET, dropping the submission body silently.
 *
 * <p>The filter runs <em>after</em> {@link JwtAuthenticationFilter} in the
 * Spring Security chain — by the time we look at the token's region claim,
 * the signature and expiry have already been validated, so we can trust the
 * value. Tokens that arrive without a region claim (legacy tokens issued
 * before this rollout) pass through untouched.
 *
 * <p>Public paths (signup / login / refresh / actuator health) bypass the
 * filter unconditionally — those endpoints don't have a user-bound JWT yet,
 * so there's nothing to mismatch against.
 *
 * <p>If the peer-gateway URL is unset, the filter fails closed with 503
 * rather than letting the request continue against the wrong region's data.
 * The X-Region-Mismatch response header is purely diagnostic — clients
 * should follow the Location header transparently.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RegionMismatchFilter extends OncePerRequestFilter {

    /**
     * Paths that are public — never redirect. Auth endpoints can't be
     * redirected because the request has no user-bound JWT yet. Actuator
     * paths are health-checked region-locally by the load balancer, so a
     * redirect there would break LB liveness probes.
     */
    private static final Set<String> PUBLIC_PATHS = Set.of(
            "/api/v1/auth/signup",
            "/api/v1/auth/login",
            "/api/v1/auth/refresh",
            "/actuator/health",
            "/actuator/info"
    );

    private static final String AUTH_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String MISMATCH_HEADER = "X-Region-Mismatch";

    @Value("${app.region:asia-south1}")
    private String currentRegion;

    @Value("${app.peer-gateway.url:}")
    private String peerGatewayUrl;

    private final JwtTokenProvider tokenProvider;

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();

        // Bypass public / non-API paths — nothing to mismatch.
        if (PUBLIC_PATHS.contains(path) || !path.startsWith("/api/")) {
            chain.doFilter(request, response);
            return;
        }

        String header = request.getHeader(AUTH_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            // No bearer token — let the JWT filter / authorization rules
            // produce a 401. This filter is not the gatekeeper for missing
            // auth; it only redirects valid-but-wrong-region requests.
            chain.doFilter(request, response);
            return;
        }
        String token = header.substring(BEARER_PREFIX.length()).trim();

        String tokenRegion;
        try {
            tokenRegion = tokenProvider.extractRegion(token);
        } catch (Exception ex) {
            // Malformed / expired / wrong-signature token — already
            // signalled empty SecurityContext by JwtAuthenticationFilter;
            // Spring will return 401. Don't pre-empt that with a redirect.
            chain.doFilter(request, response);
            return;
        }

        // Legacy tokens (pre-rollout) have no region claim — pass through.
        // Same-region requests are the happy path and the common case.
        if (tokenRegion == null || tokenRegion.equals(currentRegion)) {
            chain.doFilter(request, response);
            return;
        }

        // Region mismatch. We MUST send the user to their home region or
        // they'll hit "user not found" / cross-region read latency. If the
        // peer URL isn't configured, fail closed so the operator notices.
        if (peerGatewayUrl == null || peerGatewayUrl.isBlank()) {
            log.error("[region] mismatch detected but peer-gateway URL not configured: "
                    + "token={} gateway={} path={}", tokenRegion, currentRegion, path);
            response.setStatus(503);
            response.setHeader(MISMATCH_HEADER, "peer URL not configured");
            return;
        }

        String location = peerGatewayUrl + request.getRequestURI()
                + (request.getQueryString() == null ? "" : "?" + request.getQueryString());
        response.setStatus(307);
        response.setHeader("Location", location);
        response.setHeader(MISMATCH_HEADER, "expected=" + tokenRegion + " gateway=" + currentRegion);
        log.info("[region] redirecting userToken={} from gateway={} to peer={} path={}",
                tokenRegion, currentRegion, peerGatewayUrl, path);
    }
}
