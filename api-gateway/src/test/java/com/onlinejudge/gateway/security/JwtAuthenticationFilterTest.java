package com.onlinejudge.gateway.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JwtAuthenticationFilter}.
 *
 * <p>Verifies that the filter:
 * <ul>
 *   <li>Installs the JWT subject as the authentication principal on success.</li>
 *   <li>Leaves the {@code SecurityContext} empty when no header is present
 *       (so unauthenticated routes still reach their controllers).</li>
 *   <li>Leaves the {@code SecurityContext} empty on a malformed / invalid token
 *       — Spring Security's authorization rules then return 401.</li>
 *   <li>Still invokes the downstream filter chain in all cases.</li>
 * </ul>
 */
class JwtAuthenticationFilterTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";

    private JwtTokenProvider provider;
    private JwtAuthenticationFilter filter;

    @BeforeEach
    void setUp() {
        provider = new JwtTokenProvider(SECRET, 1, "online-judge");
        filter = new JwtAuthenticationFilter(provider);
        SecurityContextHolder.clearContext();
    }

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void validToken_installsPrincipal() throws ServletException, IOException {
        String token = provider.issueToken("user-abc");
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", "Bearer " + token);

        FilterChain chain = new MockFilterChain();
        filter.doFilter(req, new MockHttpServletResponse(), chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertThat(auth).isNotNull();
        assertThat(auth.getPrincipal()).isEqualTo("user-abc");
    }

    @Test
    void missingHeader_leavesContextEmpty() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, new MockHttpServletResponse(), chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void invalidToken_clearsContext() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", "Bearer not-a-real-jwt");
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, new MockHttpServletResponse(), chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void nonBearerHeader_ignored() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", "Basic dXNlcjpwYXNz");
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, new MockHttpServletResponse(), chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void chainAlwaysInvoked_evenWhenAuthFails() throws ServletException, IOException {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader("Authorization", "Bearer garbage");
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(req, new MockHttpServletResponse(), chain);

        // The chain was invoked despite the rejected token — Spring Security
        // (not the JWT filter) decides whether to return 401 based on route rules.
        assertThat(chain.getRequest()).isSameAs(req);
    }
}
