package com.onlinejudge.gateway.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Signs and validates HS256 JWTs used by the API gateway.
 *
 * <p>Supports a {@code kid} (key ID) header so we can rotate signing keys
 * without invalidating every outstanding token on the wire. The provider
 * keeps a {@code current} key (used to sign new tokens) and an optional
 * {@code previous} key (still accepted on inbound validation for one
 * rotation cycle). Both come from {@code app.jwt.keys.<kid>}; today the
 * deployment populates {@code v1} with the {@code JWT_SECRET} env var and
 * pins {@code app.jwt.kid-current=v1}. When rotating: introduce {@code v2}
 * as {@code kid-current}, leave {@code v1} as {@code kid-previous} for one
 * release, then drop {@code v1} on the next.
 *
 * <p>TODO: move the actual key bytes to Google Secret Manager and inject
 * via the Spring Cloud GCP secret-manager property source. For now the
 * keys live in env vars / application.yml — keys never appear in the image.
 *
 * <p>Claims:
 * <ul>
 *   <li>{@code sub} — userId (UUID string).</li>
 *   <li>{@code typ} — {@code access} or {@code refresh}; the gateway only
 *       accepts {@code access} tokens for protected endpoints. {@code refresh}
 *       tokens are not JWTs in this codebase — they are SecureRandom opaque
 *       strings — so this claim is provided for forward-compat / inspectability
 *       and not load-bearing today.</li>
 *   <li>{@code iat} — issued-at.</li>
 *   <li>{@code exp} — expiration (issued-at + {@code app.jwt.access-ttl-seconds}).</li>
 *   <li>{@code iss} — issuer (configurable, defaults to {@code online-judge}).</li>
 *   <li>{@code kid} (header) — key id used to sign.</li>
 * </ul>
 *
 * <p>The token is validated on every request by {@link JwtAuthenticationFilter};
 * the {@code sub} becomes the {@link org.springframework.security.core.Authentication}'s
 * principal, which downstream controllers use instead of any user-supplied
 * {@code userId} in the request body.
 */
@Component
public class JwtTokenProvider {

    public static final String TYPE_ACCESS = "access";
    public static final String TYPE_REFRESH = "refresh";

    private final Map<String, SecretKey> keysByKid = new HashMap<>();
    private final String currentKid;
    private final Duration accessTtl;
    private final String issuer;

    // @Autowired marks this as THE constructor Spring should use. Without it,
    // Spring sees two constructors (this + the legacy 3-arg test-only one
    // below), can't pick, and falls back to looking for a no-arg default —
    // which doesn't exist → "No default constructor found" at boot. Same
    // pattern we hit on TestCaseFetcher and AgentClient.
    @org.springframework.beans.factory.annotation.Autowired
    public JwtTokenProvider(
            @Value("${app.jwt.kid-current:v1}") String currentKid,
            @Value("${app.jwt.kid-previous:}") String previousKid,
            @Value("${app.jwt.keys.v1:${app.jwt.secret:dev-only-secret-please-rotate-in-prod-32+chars-needed-for-HS256}}")
            String keyV1,
            @Value("${app.jwt.keys.v2:}") String keyV2,
            @Value("${app.jwt.access-ttl-seconds:900}") long accessTtlSeconds,
            @Value("${app.jwt.issuer:online-judge}") String issuer) {
        registerKey("v1", keyV1);
        registerKey("v2", keyV2);
        if (!keysByKid.containsKey(currentKid)) {
            throw new IllegalArgumentException(
                    "app.jwt.kid-current=" + currentKid + " has no key configured under app.jwt.keys");
        }
        if (previousKid != null && !previousKid.isBlank() && !keysByKid.containsKey(previousKid)) {
            throw new IllegalArgumentException(
                    "app.jwt.kid-previous=" + previousKid + " has no key configured under app.jwt.keys");
        }
        this.currentKid = currentKid;
        this.accessTtl = Duration.ofSeconds(accessTtlSeconds);
        this.issuer = issuer;
    }

    /**
     * Legacy 3-arg constructor used by {@code JwtTokenProviderTest}. ttlHours
     * is converted to seconds; passing 0 yields a zero-duration access TTL, matching
     * the pre-V5 behaviour the test for "expired token" relies on.
     */
    public JwtTokenProvider(String secret, long ttlHours, String issuer) {
        this("v1", "", secret, "", ttlHours * 3600L, issuer);
    }

    private void registerKey(String kid, String secret) {
        if (secret == null || secret.isBlank()) {
            return;
        }
        byte[] raw = secret.getBytes(StandardCharsets.UTF_8);
        if (raw.length < 32) {
            throw new IllegalArgumentException(
                    "app.jwt.keys." + kid + " must be at least 32 bytes for HS256 (got " + raw.length + ")");
        }
        keysByKid.put(kid, Keys.hmacShaKeyFor(raw));
    }

    public Duration getAccessTtl() {
        return accessTtl;
    }

    /** Mints an access JWT carrying {@code userId} as the subject. */
    public String issueToken(String userId) {
        return issueAccessToken(userId);
    }

    /** Mints an access JWT carrying {@code userId} as the subject. */
    public String issueAccessToken(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId must not be blank");
        }
        Instant now = Instant.now();
        return Jwts.builder()
                .header().keyId(currentKid).and()
                .issuer(issuer)
                .subject(userId)
                .claim("typ", TYPE_ACCESS)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(accessTtl)))
                .signWith(keysByKid.get(currentKid))
                .compact();
    }

    /**
     * Validates the token's signature, issuer, expiration, and (optionally) that
     * {@code typ=access}. Returns the {@code sub} (userId) on success; throws
     * {@link JwtException} on failure.
     */
    public String validateAndExtractUserId(String token) {
        Jws<Claims> jws = parse(token);
        Claims claims = jws.getPayload();
        String sub = claims.getSubject();
        if (sub == null || sub.isBlank()) {
            throw new JwtException("Token has no subject claim");
        }
        Object typ = claims.get("typ");
        // Legacy tokens (pre-V5) won't have a typ claim — accept them; new
        // tokens must explicitly be access tokens.
        if (typ != null && !TYPE_ACCESS.equals(typ.toString())) {
            throw new JwtException("Token is not an access token (typ=" + typ + ")");
        }
        return sub;
    }

    private Jws<Claims> parse(String token) {
        // Resolve the signing key by the token's kid header — falls back to
        // current/previous if no header is present (legacy compat).
        return Jwts.parser()
                .keyLocator(header -> {
                    String kid = (String) header.get("kid");
                    if (kid != null && keysByKid.containsKey(kid)) {
                        return keysByKid.get(kid);
                    }
                    return keysByKid.get(currentKid);
                })
                .requireIssuer(issuer)
                .build()
                .parseSignedClaims(token);
    }
}
