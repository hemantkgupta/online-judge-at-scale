package com.onlinejudge.gateway.security;

import io.jsonwebtoken.JwtException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JwtTokenProvider}.
 *
 * <p>Properties verified:
 * <ul>
 *   <li>Round-trip: a token issued for {@code userId} validates back to the same id.</li>
 *   <li>Tokens signed by a <em>different</em> secret are rejected.</li>
 *   <li>Tokens with a different issuer are rejected.</li>
 *   <li>Expired tokens are rejected (via a 0-hour TTL provider).</li>
 *   <li>Garbage / non-JWT strings are rejected.</li>
 *   <li>Constructor refuses secrets shorter than 32 bytes (HS256 minimum).</li>
 * </ul>
 */
class JwtTokenProviderTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";
    private static final String OTHER_SECRET = "different-secret-32-bytes-min-bbbbbbbbb";

    @Test
    void roundTrip_extractsOriginalUserId() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueToken("user-abc");

        assertThat(provider.validateAndExtractUserId(token)).isEqualTo("user-abc");
    }

    @Test
    void validate_rejectsTokenSignedWithDifferentSecret() {
        JwtTokenProvider issuer = new JwtTokenProvider(SECRET, 1, "online-judge");
        JwtTokenProvider verifier = new JwtTokenProvider(OTHER_SECRET, 1, "online-judge");
        String token = issuer.issueToken("user-abc");

        assertThatThrownBy(() -> verifier.validateAndExtractUserId(token))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void validate_rejectsTokenWithDifferentIssuer() {
        JwtTokenProvider issuer = new JwtTokenProvider(SECRET, 1, "attacker");
        JwtTokenProvider verifier = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = issuer.issueToken("user-abc");

        assertThatThrownBy(() -> verifier.validateAndExtractUserId(token))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void validate_rejectsExpiredToken() throws InterruptedException {
        // Sub-hour TTL isn't supported by long-hours; emulate expiration via a
        // negative-window provider by issuing then waiting beyond a 1ms clock skew.
        JwtTokenProvider issuer = new JwtTokenProvider(SECRET, 0, "online-judge");
        String token = issuer.issueToken("user-abc");
        Thread.sleep(50);

        assertThatThrownBy(() -> issuer.validateAndExtractUserId(token))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void validate_rejectsMalformedToken() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");

        assertThatThrownBy(() -> provider.validateAndExtractUserId("not-a-jwt"))
                .isInstanceOf(JwtException.class);
        assertThatThrownBy(() -> provider.validateAndExtractUserId(""))
                .isInstanceOf(Exception.class);
    }

    @Test
    void issueToken_rejectsBlankUserId() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");

        assertThatThrownBy(() -> provider.issueToken(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.issueToken(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_rejectsShortSecret() {
        assertThatThrownBy(() -> new JwtTokenProvider("short", 1, "online-judge"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
