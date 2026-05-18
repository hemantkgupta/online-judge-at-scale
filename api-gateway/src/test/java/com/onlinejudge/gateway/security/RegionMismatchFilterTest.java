package com.onlinejudge.gateway.security;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RegionMismatchFilter}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Token region matches gateway region — pass-through, no redirect.</li>
 *   <li>Token region differs — 307 with the right Location header.</li>
 *   <li>Public paths (signup/login/health) — bypass unconditionally.</li>
 *   <li>Missing Authorization header — pass through (JwtAuthFilter handles).</li>
 *   <li>Malformed token — pass through.</li>
 *   <li>Peer URL not configured — 503 fail-closed.</li>
 *   <li>Query string preserved in the Location URL.</li>
 *   <li>Tokens with no region claim (legacy) — pass through.</li>
 * </ul>
 */
class RegionMismatchFilterTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";
    private static final String GATEWAY_REGION = "asia-south1";
    private static final String PEER_URL = "https://gateway.us-central1.example.com";

    private JwtTokenProvider provider;
    private RegionMismatchFilter filter;

    @BeforeEach
    void setUp() {
        provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        filter = new RegionMismatchFilter(provider);
        ReflectionTestUtils.setField(filter, "currentRegion", GATEWAY_REGION);
        ReflectionTestUtils.setField(filter, "peerGatewayUrl", PEER_URL);
    }

    @Test
    void sameRegion_passesThrough() throws ServletException, IOException {
        String token = provider.issueAccessToken("user-1", GATEWAY_REGION);
        MockHttpServletRequest req = req("/api/v1/submissions", token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        // Chain was invoked, no redirect status was set.
        assertThat(chain.getRequest()).isSameAs(req);
        assertThat(resp.getStatus()).isEqualTo(200); // default MockHttpServletResponse status
        assertThat(resp.getHeader("Location")).isNull();
    }

    @Test
    void differentRegion_returns307WithLocation() throws ServletException, IOException {
        String token = provider.issueAccessToken("user-1", "us-central1");
        MockHttpServletRequest req = req("/api/v1/submissions/abc", token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(307);
        assertThat(resp.getHeader("Location")).isEqualTo(PEER_URL + "/api/v1/submissions/abc");
        assertThat(resp.getHeader("X-Region-Mismatch"))
                .contains("expected=us-central1")
                .contains("gateway=asia-south1");
        // Downstream filter chain was NOT invoked on the redirect path.
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void publicPath_signup_bypassesFilter() throws ServletException, IOException {
        // Even a wrong-region token should not cause a redirect on /signup —
        // signup itself doesn't read user-region; the request needs to reach
        // the controller so a new user can be stamped with THIS gateway's region.
        String token = provider.issueAccessToken("user-1", "us-central1");
        MockHttpServletRequest req = req("/api/v1/auth/signup", token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(chain.getRequest()).isSameAs(req);
    }

    @Test
    void publicPath_login_bypassesFilter() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/v1/auth/login");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(chain.getRequest()).isSameAs(req);
    }

    @Test
    void publicPath_actuatorHealth_bypassesFilter() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/actuator/health");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(chain.getRequest()).isSameAs(req);
    }

    @Test
    void noAuthHeader_passesThrough() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/submissions");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        // No auth header → not our job to 401; let JwtAuthenticationFilter +
        // Spring Security's authorization rules return 401.
        assertThat(chain.getRequest()).isSameAs(req);
        assertThat(resp.getStatus()).isEqualTo(200);
    }

    @Test
    void malformedToken_passesThrough() throws ServletException, IOException {
        MockHttpServletRequest req = req("/api/v1/submissions", "garbage-not-a-jwt");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        // Malformed → defer to the JWT filter / 401 path; don't redirect.
        assertThat(chain.getRequest()).isSameAs(req);
        assertThat(resp.getStatus()).isEqualTo(200);
    }

    @Test
    void legacyTokenNoRegionClaim_passesThrough() throws ServletException, IOException {
        // Token issued without a region claim (e.g. via the legacy issueToken
        // or a token from a pre-rollout deployment) — should not be redirected.
        String token = provider.issueToken("user-1");
        MockHttpServletRequest req = req("/api/v1/submissions", token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(chain.getRequest()).isSameAs(req);
        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(resp.getHeader("Location")).isNull();
    }

    @Test
    void peerUrlUnset_returns503() throws ServletException, IOException {
        ReflectionTestUtils.setField(filter, "peerGatewayUrl", "");
        String token = provider.issueAccessToken("user-1", "us-central1");
        MockHttpServletRequest req = req("/api/v1/submissions", token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(503);
        assertThat(resp.getHeader("X-Region-Mismatch")).contains("peer URL not configured");
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void queryStringPreservedInLocation() throws ServletException, IOException {
        String token = provider.issueAccessToken("user-1", "us-central1");
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/submissions");
        req.setQueryString("limit=10&cursor=abc");
        req.addHeader("Authorization", "Bearer " + token);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, resp, chain);

        assertThat(resp.getStatus()).isEqualTo(307);
        assertThat(resp.getHeader("Location"))
                .isEqualTo(PEER_URL + "/api/v1/submissions?limit=10&cursor=abc");
    }

    private static MockHttpServletRequest req(String path, String token) {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", path);
        req.addHeader("Authorization", "Bearer " + token);
        return req;
    }
}
