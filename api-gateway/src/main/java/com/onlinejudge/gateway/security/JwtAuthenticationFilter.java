package com.onlinejudge.gateway.security;

import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

/**
 * Per-request JWT validation for the API gateway (Part 3 of the blog).
 *
 * <p>Reads the {@code Authorization: Bearer <token>} header, calls
 * {@link JwtTokenProvider#validateAndExtractUserId(String)}, and on success
 * installs an {@link UsernamePasswordAuthenticationToken} whose principal is
 * the {@code userId} from the token. Downstream controllers read that
 * principal via {@link org.springframework.security.core.annotation.AuthenticationPrincipal}
 * — they never trust a {@code userId} field in the request body.
 *
 * <p>On a missing or invalid token, the filter does <em>not</em> short-circuit
 * with a 401 itself — it leaves the {@code SecurityContext} empty and lets
 * Spring Security's authorization rules in {@link SecurityConfig} decide
 * whether the route requires authentication. This keeps the public auth
 * endpoints ({@code /signup}, {@code /login}, {@code /refresh}) and the
 * actuator health endpoints reachable without tokens.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final String AUTH_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final JwtTokenProvider tokenProvider;

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader(AUTH_HEADER);
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            String token = header.substring(BEARER_PREFIX.length()).trim();
            try {
                String userId = tokenProvider.validateAndExtractUserId(token);
                UsernamePasswordAuthenticationToken auth =
                        new UsernamePasswordAuthenticationToken(userId, null, List.of());
                SecurityContextHolder.getContext().setAuthentication(auth);
            } catch (JwtException ex) {
                // Leave SecurityContext empty — SecurityConfig's rules decide
                // whether this endpoint demands authentication.
                log.debug("[auth] Rejected JWT: {}", ex.getMessage());
                SecurityContextHolder.clearContext();
            }
        }
        chain.doFilter(request, response);
    }
}
