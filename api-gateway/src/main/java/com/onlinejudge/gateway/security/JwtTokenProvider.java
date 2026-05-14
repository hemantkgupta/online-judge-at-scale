package com.onlinejudge.gateway.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Signs and validates HS256 JWTs used by the API gateway (Part 3 of the blog).
 *
 * <p>Production deployments would use asymmetric keys (RS256) with a shared
 * IdP issuing tokens and the gateway only verifying signatures. For the local
 * demo we use HS256 with a pre-shared secret configured via {@code app.jwt.secret}:
 * simpler to wire end-to-end, same gateway-side validation surface.
 *
 * <p>Claims:
 * <ul>
 *   <li>{@code sub} — the contestant's {@code userId} (UUID string).</li>
 *   <li>{@code iat} — issued-at.</li>
 *   <li>{@code exp} — expiration (issued-at + {@code app.jwt.ttl-hours}).</li>
 * </ul>
 *
 * <p>The token is validated on every request by {@link JwtAuthenticationFilter};
 * a valid {@code sub} becomes the {@link org.springframework.security.core.Authentication}'s
 * principal, which the gateway uses instead of any user-supplied {@code userId}
 * in the request body — closing the "client claims its own user-id" hole the
 * un-auth'd local version had.
 */
@Component
public class JwtTokenProvider {

    private final SecretKey key;
    private final Duration ttl;
    private final String issuer;

    public JwtTokenProvider(
            @Value("${app.jwt.secret:dev-only-secret-please-rotate-in-prod-32+chars-needed-for-HS256}")
            String secret,
            @Value("${app.jwt.ttl-hours:8}") long ttlHours,
            @Value("${app.jwt.issuer:online-judge}") String issuer) {
        byte[] raw = secret.getBytes(StandardCharsets.UTF_8);
        if (raw.length < 32) {
            throw new IllegalArgumentException(
                    "app.jwt.secret must be at least 32 bytes for HS256 (got " + raw.length + ")");
        }
        this.key = Keys.hmacShaKeyFor(raw);
        this.ttl = Duration.ofHours(ttlHours);
        this.issuer = issuer;
    }

    /** Mints a JWT carrying {@code userId} as the subject. */
    public String issueToken(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId must not be blank");
        }
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(issuer)
                .subject(userId)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(ttl)))
                .signWith(key)
                .compact();
    }

    /**
     * Validates the token's signature, issuer, and expiration. Returns the
     * {@code sub} (userId) on success; throws {@link JwtException} on failure.
     */
    public String validateAndExtractUserId(String token) {
        Claims claims = Jwts.parser()
                .verifyWith(key)
                .requireIssuer(issuer)
                .build()
                .parseSignedClaims(token)
                .getPayload();
        String sub = claims.getSubject();
        if (sub == null || sub.isBlank()) {
            throw new JwtException("Token has no subject claim");
        }
        return sub;
    }
}
