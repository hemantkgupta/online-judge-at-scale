package com.onlinejudge.gateway.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Signs and validates HS256 JWTs used by the API gateway.
 *
 * <p>The set of acceptable signing / verification keys is owned by
 * {@link JwtKeyStore}: this class is the thin "what claims do we put on the
 * token, and what do we extract back" layer. The store can hot-reload from a
 * Secret-Manager-backed sidecar file — see
 * {@code docs/design-docs/key-rotation.md}.
 *
 * <p>Versioned-kid model. Every minted token carries a {@code kid} header that
 * names the key it was signed with. On validation we look up the verification
 * key by the inbound {@code kid}; tokens minted under an older (still-accepted)
 * key continue to verify until the operator removes that kid from the store.
 * That overlap is the whole point of rotation — minted-old tokens keep working
 * for the duration of the configured window (default 7 days; access-token TTL
 * is 15 min so this is fingers-and-toes more headroom than required).
 *
 * <p>Back-compat. The legacy 3-arg constructor {@code (secret, ttlHours,
 * issuer)} is still here — exercised by {@code JwtTokenProviderTest} and a few
 * pre-V5 test seams. It synthesises a single-key store under {@code kid=v1}
 * and produces tokens identical on the wire to a Spring-wired provider with
 * {@code app.jwt.kid-current=v1}.
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
 *   <li>{@code region} — optional, see {@link #issueAccessToken(String, String)}.</li>
 *   <li>{@code kid} (header) — key id used to sign.</li>
 * </ul>
 */
@Component
public class JwtTokenProvider {

    public static final String TYPE_ACCESS = "access";
    public static final String TYPE_REFRESH = "refresh";

    private final JwtKeyStore keyStore;
    private final Duration accessTtl;
    private final String issuer;

    @org.springframework.beans.factory.annotation.Autowired
    public JwtTokenProvider(
            JwtKeyStore keyStore,
            @Value("${app.jwt.access-ttl-seconds:900}") long accessTtlSeconds,
            @Value("${app.jwt.issuer:online-judge}") String issuer) {
        this.keyStore = keyStore;
        this.accessTtl = Duration.ofSeconds(accessTtlSeconds);
        this.issuer = issuer;
    }

    /**
     * Legacy 3-arg constructor used by {@code JwtTokenProviderTest} and other
     * pre-V5 test seams. Synthesises a single-key store under {@code v1}.
     * ttlHours is converted to seconds; passing 0 yields a zero-duration access
     * TTL, matching the pre-V5 behaviour the test for "expired token" relies on.
     */
    public JwtTokenProvider(String secret, long ttlHours, String issuer) {
        this(new JwtKeyStore("v1", singleKeyMap(secret)), ttlHours * 3600L, issuer);
    }

    private static Map<String, String> singleKeyMap(String secret) {
        Map<String, String> m = new HashMap<>();
        m.put("v1", secret);
        return m;
    }

    public Duration getAccessTtl() {
        return accessTtl;
    }

    /** Mints an access JWT carrying {@code userId} as the subject. */
    public String issueToken(String userId) {
        return issueAccessToken(userId, null);
    }

    /** Mints an access JWT carrying {@code userId} as the subject (no region claim). */
    public String issueAccessToken(String userId) {
        return issueAccessToken(userId, null);
    }

    /**
     * Mints an access JWT carrying {@code userId} as the subject and the
     * user's home {@code region} as a non-standard claim. The region claim
     * lets the {@code RegionMismatchFilter} route requests that arrive at
     * the wrong regional gateway pod to the correct one via 307.
     *
     * <p>{@code region} may be {@code null} or blank — in which case no
     * region claim is set (back-compat for tokens issued before the
     * multi-region rollout; the filter treats a missing claim as "no
     * mismatch" and passes the request through).
     */
    public String issueAccessToken(String userId, String region) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId must not be blank");
        }
        JwtKeyStore.Snapshot snap = keyStore.current();
        Instant now = Instant.now();
        io.jsonwebtoken.JwtBuilder builder = Jwts.builder()
                .header().keyId(snap.currentKid()).and()
                .issuer(issuer)
                .subject(userId)
                .claim("typ", TYPE_ACCESS)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(accessTtl)));
        if (region != null && !region.isBlank()) {
            builder.claim("region", region);
        }
        return builder.signWith(snap.currentKey()).compact();
    }

    /**
     * Returns the {@code region} claim from a previously-issued access token,
     * or {@code null} if the token has no region claim (legacy tokens issued
     * before the multi-region rollout). Throws {@link JwtException} if the
     * token is unparseable / has a bad signature / has expired.
     */
    public String extractRegion(String token) {
        Jws<Claims> jws = parse(token);
        Object region = jws.getPayload().get("region");
        return region == null ? null : region.toString();
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
        // Take ONE snapshot so the keyLocator and the parser see a consistent
        // view; concurrent rotation can't flip the map mid-parse.
        JwtKeyStore.Snapshot snap = keyStore.current();
        return Jwts.parser()
                .keyLocator(header -> {
                    String kid = (String) header.get("kid");
                    if (kid != null) {
                        SecretKey k = snap.keyFor(kid);
                        if (k != null) {
                            return k;
                        }
                        // Token names a kid we don't have. Falling back to
                        // currentKey() would produce a misleading bad-signature
                        // error and confuse rotation triage. Throw a clear
                        // JwtException so the log line names the offending kid.
                        throw new JwtException("Token signed with unknown kid='" + kid + "'");
                    }
                    // No kid header — must be a legacy / pre-rotation token.
                    // Validate against the current key (best-effort) — the
                    // store's bootstrap seeds v1 from JWT_SECRET so this
                    // matches the wire of every pre-rotation issuer.
                    return snap.currentKey();
                })
                .requireIssuer(issuer)
                .build()
                .parseSignedClaims(token);
    }
}
