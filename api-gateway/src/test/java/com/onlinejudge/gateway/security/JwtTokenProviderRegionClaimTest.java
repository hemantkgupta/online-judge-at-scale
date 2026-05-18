package com.onlinejudge.gateway.security;

import io.jsonwebtoken.JwtException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the new {@code region} claim handling in {@link JwtTokenProvider}.
 *
 * <p>Verifies:
 * <ul>
 *   <li>{@code issueAccessToken(userId, region)} embeds the region claim.</li>
 *   <li>{@code extractRegion} round-trips the value.</li>
 *   <li>Tokens issued without a region (legacy 1-arg / null) have a null region claim.</li>
 *   <li>The legacy 3-arg ctor is still usable (back-compat test seam).</li>
 *   <li>{@code extractRegion} on a malformed token throws.</li>
 * </ul>
 */
class JwtTokenProviderRegionClaimTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";

    @Test
    void issueAccessToken_withRegion_embedsClaim() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueAccessToken("user-abc", "asia-south1");

        assertThat(provider.extractRegion(token)).isEqualTo("asia-south1");
        // Subject claim unchanged.
        assertThat(provider.validateAndExtractUserId(token)).isEqualTo("user-abc");
    }

    @Test
    void issueAccessToken_nullRegion_omitsClaim() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueAccessToken("user-abc", null);

        // No region claim → extractRegion returns null.
        assertThat(provider.extractRegion(token)).isNull();
    }

    @Test
    void issueAccessToken_blankRegion_omitsClaim() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueAccessToken("user-abc", "");

        assertThat(provider.extractRegion(token)).isNull();
    }

    @Test
    void legacyIssueToken_hasNoRegionClaim() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueToken("user-abc");

        assertThat(provider.extractRegion(token)).isNull();
    }

    @Test
    void legacyThreeArgConstructor_stillWorks() {
        // Pre-V9 test seams construct the provider with (secret, ttlHours, issuer).
        // This back-compat check ensures we didn't accidentally drop that ctor.
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String token = provider.issueAccessToken("user-abc", "asia-south1");

        assertThat(provider.validateAndExtractUserId(token)).isEqualTo("user-abc");
        assertThat(provider.extractRegion(token)).isEqualTo("asia-south1");
    }

    @Test
    void extractRegion_malformedToken_throws() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");

        assertThatThrownBy(() -> provider.extractRegion("not-a-jwt"))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void extractRegion_differentRegions_roundTripIndependently() {
        JwtTokenProvider provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        String tokenA = provider.issueAccessToken("user-1", "asia-south1");
        String tokenB = provider.issueAccessToken("user-2", "us-central1");

        assertThat(provider.extractRegion(tokenA)).isEqualTo("asia-south1");
        assertThat(provider.extractRegion(tokenB)).isEqualTo("us-central1");
    }
}
