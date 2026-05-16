package com.onlinejudge.gateway.controller;

import com.onlinejudge.gateway.dto.LoginRequest;
import com.onlinejudge.gateway.dto.LogoutRequest;
import com.onlinejudge.gateway.dto.RefreshRequest;
import com.onlinejudge.gateway.dto.SignupRequest;
import com.onlinejudge.gateway.dto.SignupResponse;
import com.onlinejudge.gateway.dto.TokenPairResponse;
import com.onlinejudge.gateway.service.AuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * Authentication endpoints.
 *
 * <p>Replaces the pre-V5 dev-token-mint endpoint with real signup, login,
 * refresh, and logout. All four endpoints write an audit row to
 * {@code auth_events}; failed attempts are recorded too (LOGIN_FAIL with the
 * reason in {@code detail}).
 *
 * <p>The old {@code POST /api/v1/auth/token} endpoint has been removed. Any
 * test or script that still calls it must move to {@code POST /api/v1/auth/login}.
 * TODO: audit callers in tests/scripts that still hit /auth/token (see infra/end-to-end-mac-pi.md).
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/signup")
    public ResponseEntity<?> signup(@Valid @RequestBody SignupRequest req, HttpServletRequest httpReq) {
        try {
            UUID userId = authService.signup(req, clientIp(httpReq), userAgent(httpReq));
            return ResponseEntity.status(HttpStatus.CREATED).body(new SignupResponse(userId));
        } catch (AuthService.AuthException ex) {
            return errorResponse(ex);
        }
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest req, HttpServletRequest httpReq) {
        try {
            TokenPairResponse pair = authService.login(req, clientIp(httpReq), userAgent(httpReq));
            return ResponseEntity.ok(pair);
        } catch (AuthService.AuthException ex) {
            return errorResponse(ex);
        }
    }

    @PostMapping("/refresh")
    public ResponseEntity<?> refresh(@Valid @RequestBody RefreshRequest req, HttpServletRequest httpReq) {
        try {
            TokenPairResponse pair = authService.refresh(req.getRefreshToken(), clientIp(httpReq), userAgent(httpReq));
            return ResponseEntity.ok(pair);
        } catch (AuthService.AuthException ex) {
            return errorResponse(ex);
        }
    }

    @PostMapping("/logout")
    public ResponseEntity<?> logout(@Valid @RequestBody LogoutRequest req,
                                    @AuthenticationPrincipal String userId,
                                    HttpServletRequest httpReq) {
        if (userId == null || userId.isBlank()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        try {
            authService.logout(UUID.fromString(userId), req.getRefreshToken(),
                    clientIp(httpReq), userAgent(httpReq));
            return ResponseEntity.noContent().build();
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
    }

    private static ResponseEntity<Map<String, String>> errorResponse(AuthService.AuthException ex) {
        return ResponseEntity.status(ex.status()).body(Map.of("error", ex.getMessage()));
    }

    private static String clientIp(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            int comma = xff.indexOf(',');
            return (comma > 0 ? xff.substring(0, comma) : xff).trim();
        }
        return req.getRemoteAddr();
    }

    private static String userAgent(HttpServletRequest req) {
        return req.getHeader("User-Agent");
    }
}
