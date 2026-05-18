package com.onlinejudge.gateway.service;

import com.onlinejudge.gateway.dto.LoginRequest;
import com.onlinejudge.gateway.dto.SignupRequest;
import com.onlinejudge.gateway.dto.TokenPairResponse;
import com.onlinejudge.gateway.entity.AuthEvent;
import com.onlinejudge.gateway.entity.RefreshToken;
import com.onlinejudge.gateway.entity.User;
import com.onlinejudge.gateway.repository.AuthEventRepository;
import com.onlinejudge.gateway.repository.RefreshTokenRepository;
import com.onlinejudge.gateway.repository.UserRepository;
import com.onlinejudge.gateway.security.JwtTokenProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

/**
 * Auth orchestration: signup, login, refresh, logout.
 *
 * <p>Password hashing: Argon2id (PHC-formatted hash stored in
 * {@code users.password_hash}). Parameters tuned for the target hardware —
 * iterations=3, memory=65536 KiB, parallelism=1. These match
 * {@link Argon2PasswordEncoder#defaultsForSpringSecurity_v5_8()} which is the
 * current recommendation for interactive workloads.
 *
 * <p>Refresh tokens: 32 bytes of {@link SecureRandom} entropy, base64url-encoded.
 * Only the SHA-256 of the raw token is stored server-side — the raw token is
 * returned to the client once at issuance and never persisted. A DB dump
 * cannot impersonate users.
 *
 * <p>Refresh rotation: every successful refresh revokes the old refresh token
 * and issues a new one. Reusing a previously-rotated refresh token is treated
 * as a stolen credential and returns 401.
 */
@Slf4j
@Service
public class AuthService {

    private static final SecureRandom RNG = new SecureRandom();
    private static final int REFRESH_TOKEN_BYTES = 32;

    private final UserRepository userRepository;
    private final RefreshTokenRepository refreshTokenRepository;
    private final AuthEventRepository authEventRepository;
    private final JwtTokenProvider tokenProvider;
    private final Argon2PasswordEncoder passwordEncoder;
    private final Duration refreshTtl;
    /**
     * Region this api-gateway pod is pinned to. Stamped onto new users at
     * signup (so the user's "home" region is whichever gateway accepted the
     * registration), and read back at login to inject the {@code region}
     * claim into the access JWT.
     */
    private final String currentRegion;

    // @Autowired marks this as THE constructor Spring picks at startup —
    // without it Spring sees two constructors (this + the legacy 5-arg one
    // used by AuthServiceTest) and falls back to "no default ctor found".
    // Same pattern as JwtTokenProvider / TestCaseFetcher / AgentClient.
    @org.springframework.beans.factory.annotation.Autowired
    public AuthService(UserRepository userRepository,
                       RefreshTokenRepository refreshTokenRepository,
                       AuthEventRepository authEventRepository,
                       JwtTokenProvider tokenProvider,
                       @Value("${app.jwt.refresh-ttl-seconds:604800}") long refreshTtlSeconds,
                       @Value("${app.region:asia-south1}") String currentRegion) {
        this.userRepository = userRepository;
        this.refreshTokenRepository = refreshTokenRepository;
        this.authEventRepository = authEventRepository;
        this.tokenProvider = tokenProvider;
        // Argon2id with the Spring Security 5.8 defaults — saltLength=16,
        // hashLength=32, parallelism=1, memory=64 MiB (65536 KiB), iterations=3.
        this.passwordEncoder = Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8();
        this.refreshTtl = Duration.ofSeconds(refreshTtlSeconds);
        this.currentRegion = currentRegion;
    }

    /**
     * Legacy constructor used by {@code AuthServiceTest} (no region — defaults
     * to "asia-south1" to match the prod default). Kept so the existing test
     * seam compiles without changes; new tests should pass {@code currentRegion}
     * explicitly via the 6-arg ctor.
     */
    public AuthService(UserRepository userRepository,
                       RefreshTokenRepository refreshTokenRepository,
                       AuthEventRepository authEventRepository,
                       JwtTokenProvider tokenProvider,
                       long refreshTtlSeconds) {
        this(userRepository, refreshTokenRepository, authEventRepository, tokenProvider,
                refreshTtlSeconds, "asia-south1");
    }

    /** Thrown when an auth precondition fails. Maps to 4xx in the controller. */
    public static class AuthException extends RuntimeException {
        private final int status;
        public AuthException(int status, String message) { super(message); this.status = status; }
        public int status() { return status; }
    }

    @Transactional
    public UUID signup(SignupRequest req, String ip, String userAgent) {
        String username = req.getUsername().trim();
        if (userRepository.existsByUsername(username)) {
            recordEvent(null, "SIGNUP_FAIL", ip, userAgent, "username taken");
            throw new AuthException(409, "Username already taken");
        }
        UUID userId = UUID.randomUUID();
        // Stamp the gateway pod's region onto the user so future logins
        // (from anywhere) carry it forward into the JWT region claim.
        User user = new User(userId, username, passwordEncoder.encode(req.getPassword()), currentRegion);
        userRepository.save(user);
        recordEvent(userId, "SIGNUP", ip, userAgent, null);
        log.info("[auth] signup userId={} username={}", userId, username);
        return userId;
    }

    @Transactional
    public TokenPairResponse login(LoginRequest req, String ip, String userAgent) {
        String username = req.getUsername().trim();
        Optional<User> maybeUser = userRepository.findByUsername(username);
        if (maybeUser.isEmpty()) {
            recordEvent(null, "LOGIN_FAIL", ip, userAgent, "unknown username");
            throw new AuthException(401, "Invalid credentials");
        }
        User user = maybeUser.get();
        if (user.getPasswordHash() == null || user.getPasswordHash().isEmpty()
                || !passwordEncoder.matches(req.getPassword(), user.getPasswordHash())) {
            recordEvent(user.getId(), "LOGIN_FAIL", ip, userAgent, "invalid password");
            log.warn("[auth] LOGIN_FAIL userId={} ip={}", user.getId(), ip);
            throw new AuthException(401, "Invalid credentials");
        }
        TokenPairResponse pair = issuePair(user.getId(), user.getRegion());
        recordEvent(user.getId(), "LOGIN_OK", ip, userAgent, null);
        return pair;
    }

    @Transactional
    public TokenPairResponse refresh(String rawRefreshToken, String ip, String userAgent) {
        String hash = sha256Hex(rawRefreshToken);
        Optional<RefreshToken> maybe = refreshTokenRepository.findByTokenHash(hash);
        Instant now = Instant.now();
        if (maybe.isEmpty()) {
            recordEvent(null, "REFRESH_FAIL", ip, userAgent, "unknown token");
            throw new AuthException(401, "Invalid refresh token");
        }
        RefreshToken stored = maybe.get();
        if (!stored.isActive(now)) {
            recordEvent(stored.getUserId(), "REFRESH_FAIL", ip, userAgent,
                    stored.getRevokedAt() != null ? "revoked" : "expired");
            throw new AuthException(401, "Invalid refresh token");
        }
        // Rotate: revoke old, issue new pair. Read the user's region back
        // from the row so the new access token carries the same region claim
        // the previous one did (region is set-once at signup, never updated).
        refreshTokenRepository.revoke(hash, now);
        String region = userRepository.findById(stored.getUserId())
                .map(User::getRegion)
                .orElse(currentRegion);
        TokenPairResponse pair = issuePair(stored.getUserId(), region);
        recordEvent(stored.getUserId(), "REFRESH", ip, userAgent, null);
        return pair;
    }

    @Transactional
    public void logout(UUID userId, String rawRefreshToken, String ip, String userAgent) {
        String hash = sha256Hex(rawRefreshToken);
        Optional<RefreshToken> maybe = refreshTokenRepository.findByTokenHash(hash);
        if (maybe.isPresent() && maybe.get().getUserId().equals(userId)) {
            refreshTokenRepository.revoke(hash, Instant.now());
        }
        // We don't error on "no such token" — logout is idempotent.
        recordEvent(userId, "LOGOUT", ip, userAgent, null);
    }

    // --- helpers --------------------------------------------------------

    private TokenPairResponse issuePair(UUID userId, String region) {
        String access = tokenProvider.issueAccessToken(userId.toString(), region);
        String raw = newRefreshToken();
        RefreshToken rt = new RefreshToken(sha256Hex(raw), userId, Instant.now().plus(refreshTtl));
        refreshTokenRepository.save(rt);
        return new TokenPairResponse(access, raw, tokenProvider.getAccessTtl().toSeconds());
    }

    private static String newRefreshToken() {
        byte[] bytes = new byte[REFRESH_TOKEN_BYTES];
        RNG.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private void recordEvent(UUID userId, String type, String ip, String userAgent, String detail) {
        AuthEvent event = new AuthEvent();
        event.setId(UUID.randomUUID());
        event.setUserId(userId);
        event.setEventType(type);
        event.setIpAddress(truncate(ip, 64));
        event.setUserAgent(truncate(userAgent, 256));
        event.setDetail(truncate(detail, 256));
        event.setCreatedAt(Instant.now());
        authEventRepository.save(event);
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
