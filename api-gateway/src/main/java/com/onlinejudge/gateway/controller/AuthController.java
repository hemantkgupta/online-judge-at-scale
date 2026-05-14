package com.onlinejudge.gateway.controller;

import com.onlinejudge.gateway.security.JwtTokenProvider;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Dev-only token mint endpoint (Part 3 of the blog).
 *
 * <p>In production, JWTs come from a dedicated identity provider (the blog's
 * Auth/Identity Service) — the API gateway only <em>verifies</em> tokens, never
 * mints them. For the local demo there's no IdP, so this endpoint hands out a
 * token for any user-id the caller asks for. Permitted in {@link com.onlinejudge.gateway.security.SecurityConfig}.
 *
 * <p>Usage:
 * <pre>{@code
 *   curl -X POST http://localhost:8080/api/v1/auth/token \
 *        -H 'Content-Type: application/json' \
 *        -d '{"userId":"00000000-0000-0000-0000-000000000001"}'
 *   # → {"accessToken":"<JWT>"}
 *
 *   curl -X POST http://localhost:8080/api/v1/submissions \
 *        -H 'Authorization: Bearer <JWT>' \
 *        -H 'Content-Type: application/json' \
 *        -d '{"problemId":"...","language":"python","code":"print(42)"}'
 * }</pre>
 */
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final JwtTokenProvider tokenProvider;

    @PostMapping("/token")
    public ResponseEntity<TokenResponse> token(@RequestBody TokenRequest req) {
        String jwt = tokenProvider.issueToken(req.getUserId());
        return ResponseEntity.ok(new TokenResponse(jwt));
    }

    @Data
    public static class TokenRequest {
        @NotBlank
        private String userId;
    }

    @Data
    public static class TokenResponse {
        private final String accessToken;
    }
}
