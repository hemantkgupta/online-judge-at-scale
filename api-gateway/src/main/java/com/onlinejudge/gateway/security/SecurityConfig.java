package com.onlinejudge.gateway.security;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Spring Security wiring (Part 3 of the blog).
 *
 * <p>Stateless JWT validation: every protected route requires a valid
 * {@code Authorization: Bearer <jwt>}. Sessions and CSRF are disabled — the
 * gateway is a JSON API behind a token, not a server-rendered site.
 *
 * <p>Public routes:
 * <ul>
 *   <li>{@code GET /actuator/health} — load-balancer health checks.</li>
 *   <li>{@code GET /api/v1/submissions/health} — service-level health check.</li>
 *   <li>{@code POST /api/v1/auth/signup} — username + password registration.</li>
 *   <li>{@code POST /api/v1/auth/login} — exchange credentials for access + refresh tokens.</li>
 *   <li>{@code POST /api/v1/auth/refresh} — exchange a refresh token for a new pair (carries
 *       the refresh token in the body, so the endpoint itself is unauthenticated).</li>
 * </ul>
 *
 * <p>Everything else — including {@code POST /api/v1/auth/logout} — demands a valid JWT.
 */
@Configuration
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtAuthenticationFilter jwtFilter;
    private final RegionMismatchFilter regionMismatchFilter;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .sessionManagement(s ->
                        s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(
                                "/actuator/health",
                                "/actuator/info",
                                "/api/v1/submissions/health",
                                "/api/v1/auth/signup",
                                "/api/v1/auth/login",
                                "/api/v1/auth/refresh"
                        ).permitAll()
                        .anyRequest().authenticated())
                .exceptionHandling(eh ->
                        eh.authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                // Order matters: JwtAuthenticationFilter validates the token
                // and populates SecurityContext. RegionMismatchFilter then
                // inspects the (already-trusted) region claim and 307-routes
                // wrong-region requests. Putting region AFTER jwt means we
                // never redirect a request with an invalid token — those
                // get a 401 instead.
                .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class)
                .addFilterAfter(regionMismatchFilter, JwtAuthenticationFilter.class)
                .build();
    }
}
