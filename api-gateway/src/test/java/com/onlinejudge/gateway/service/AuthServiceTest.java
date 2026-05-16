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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AuthService}.
 *
 * <p>Repositories are Mockito mocks backed by in-memory maps so the
 * rotation/revoke paths can assert the actual stored state. The
 * {@link JwtTokenProvider} is a real instance with a 32-byte test secret.
 * The Argon2 password encoder inside {@code AuthService} is real
 * (~50ms per hash; six tests, acceptable).
 */
class AuthServiceTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";

    private UserRepository userRepository;
    private RefreshTokenRepository refreshTokenRepository;
    private AuthEventRepository authEventRepository;

    private final Map<UUID, User> usersById = new HashMap<>();
    private final Map<String, User> usersByName = new HashMap<>();
    private final Map<String, RefreshToken> refreshByHash = new HashMap<>();
    private final List<AuthEvent> authEvents = new ArrayList<>();

    private AuthService authService;

    @BeforeEach
    void setUp() {
        usersById.clear();
        usersByName.clear();
        refreshByHash.clear();
        authEvents.clear();

        userRepository = mock(UserRepository.class);
        refreshTokenRepository = mock(RefreshTokenRepository.class);
        authEventRepository = mock(AuthEventRepository.class);

        when(userRepository.existsByUsername(anyString()))
                .thenAnswer(inv -> usersByName.containsKey(inv.<String>getArgument(0)));
        when(userRepository.findByUsername(anyString()))
                .thenAnswer(inv -> Optional.ofNullable(usersByName.get(inv.<String>getArgument(0))));
        when(userRepository.save(any(User.class))).thenAnswer(inv -> {
            User u = inv.getArgument(0);
            usersById.put(u.getId(), u);
            usersByName.put(u.getUsername(), u);
            return u;
        });

        when(refreshTokenRepository.findByTokenHash(anyString()))
                .thenAnswer(inv -> Optional.ofNullable(refreshByHash.get(inv.<String>getArgument(0))));
        when(refreshTokenRepository.save(any(RefreshToken.class))).thenAnswer(inv -> {
            RefreshToken rt = inv.getArgument(0);
            refreshByHash.put(rt.getTokenHash(), rt);
            return rt;
        });
        when(refreshTokenRepository.revoke(anyString(), any(Instant.class))).thenAnswer(inv -> {
            String hash = inv.getArgument(0);
            Instant when = inv.getArgument(1);
            RefreshToken t = refreshByHash.get(hash);
            if (t == null || t.getRevokedAt() != null) return 0;
            t.setRevokedAt(when);
            return 1;
        });

        when(authEventRepository.save(any(AuthEvent.class))).thenAnswer(inv -> {
            AuthEvent e = inv.getArgument(0);
            authEvents.add(e);
            return e;
        });

        JwtTokenProvider tokenProvider = new JwtTokenProvider(SECRET, 1, "online-judge");
        authService = new AuthService(userRepository, refreshTokenRepository, authEventRepository,
                tokenProvider, 7 * 24 * 3600L);
    }

    @Test
    void signup_happyPath_returnsUserIdAndPersistsArgon2Hash() {
        UUID userId = authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        assertThat(userId).isNotNull();
        User stored = usersById.get(userId);
        assertThat(stored.getUsername()).isEqualTo("alice");
        assertThat(stored.getPasswordHash()).startsWith("$argon2");
        assertThat(authEvents).hasSize(1);
        assertThat(authEvents.get(0).getEventType()).isEqualTo("SIGNUP");
    }

    @Test
    void signup_duplicateUsername_throwsConflict() {
        authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        assertThatThrownBy(() ->
                authService.signup(req("alice", "anotherpass456"), "1.2.3.4", "ua"))
                .isInstanceOf(AuthService.AuthException.class)
                .satisfies(ex -> assertThat(((AuthService.AuthException) ex).status()).isEqualTo(409));
    }

    @Test
    void login_wrongPassword_returns401AndLogsFailure() {
        authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        assertThatThrownBy(() ->
                authService.login(loginReq("alice", "WRONG-password"), "1.2.3.4", "ua"))
                .isInstanceOf(AuthService.AuthException.class)
                .satisfies(ex -> assertThat(((AuthService.AuthException) ex).status()).isEqualTo(401));
        // SIGNUP + LOGIN_FAIL.
        assertThat(authEvents).hasSize(2);
        AuthEvent fail = authEvents.get(1);
        assertThat(fail.getEventType()).isEqualTo("LOGIN_FAIL");
        assertThat(fail.getDetail()).isEqualTo("invalid password");
    }

    @Test
    void login_happyPath_returnsAccessAndRefreshToken() {
        authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse pair = authService.login(loginReq("alice", "supersecret123"), "1.2.3.4", "ua");
        assertThat(pair.getAccessToken()).isNotBlank();
        assertThat(pair.getRefreshToken()).isNotBlank();
        // 1-hour TTL from the test JwtTokenProvider constructor.
        assertThat(pair.getExpiresIn()).isEqualTo(3600L);
        assertThat(refreshByHash).hasSize(1);
    }

    @Test
    void refresh_afterRevoke_returns401() {
        authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse pair = authService.login(loginReq("alice", "supersecret123"), "1.2.3.4", "ua");

        TokenPairResponse rotated = authService.refresh(pair.getRefreshToken(), "1.2.3.4", "ua");
        assertThat(rotated.getRefreshToken()).isNotEqualTo(pair.getRefreshToken());

        assertThatThrownBy(() -> authService.refresh(pair.getRefreshToken(), "1.2.3.4", "ua"))
                .isInstanceOf(AuthService.AuthException.class)
                .satisfies(ex -> assertThat(((AuthService.AuthException) ex).status()).isEqualTo(401));
    }

    @Test
    void refresh_afterExpiry_returns401() {
        authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse pair = authService.login(loginReq("alice", "supersecret123"), "1.2.3.4", "ua");

        // Force-expire the stored row.
        String hash = AuthService.sha256Hex(pair.getRefreshToken());
        refreshByHash.get(hash).setExpiresAt(Instant.now().minusSeconds(1));

        assertThatThrownBy(() -> authService.refresh(pair.getRefreshToken(), "1.2.3.4", "ua"))
                .isInstanceOf(AuthService.AuthException.class)
                .satisfies(ex -> assertThat(((AuthService.AuthException) ex).status()).isEqualTo(401));
    }

    @Test
    void logout_revokesRefreshToken_andIsIdempotent() {
        UUID userId = authService.signup(req("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse pair = authService.login(loginReq("alice", "supersecret123"), "1.2.3.4", "ua");

        authService.logout(userId, pair.getRefreshToken(), "1.2.3.4", "ua");
        assertThatThrownBy(() -> authService.refresh(pair.getRefreshToken(), "1.2.3.4", "ua"))
                .isInstanceOf(AuthService.AuthException.class);
        // Second logout doesn't throw.
        authService.logout(userId, pair.getRefreshToken(), "1.2.3.4", "ua");
    }

    private static SignupRequest req(String username, String password) {
        SignupRequest r = new SignupRequest();
        r.setUsername(username);
        r.setPassword(password);
        return r;
    }

    private static LoginRequest loginReq(String username, String password) {
        LoginRequest r = new LoginRequest();
        r.setUsername(username);
        r.setPassword(password);
        return r;
    }
}
